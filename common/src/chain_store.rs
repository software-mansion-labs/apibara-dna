use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use error_stack::{Result, ResultExt};
use serde::{Deserialize, Serialize};

use crate::{
    chain::CanonicalChainSegment,
    file_cache::{FileCache, FileCacheError},
    object_store::{
        GetOptions, ObjectStore, ObjectStoreResultExt, ObjectVersion, PutMode, PutOptions,
    },
};

static CANONICAL_PREFIX: &str = "canon";
static RECENT_CHAIN_SEGMENT_PREFIX: &str = "recent";
static LEGACY_RECENT_CHAIN_SEGMENT_NAME: &str = "recent";
static RECENT_SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentSegmentPointer {
    pub key: String,
    pub version: String,
    pub first_block: u64,
    pub last_block: u64,
}

#[derive(Debug)]
pub struct ChainStoreError;

#[derive(Clone)]
pub struct ChainStore {
    cache: FileCache,
    client: ObjectStore,
}

impl ChainStore {
    pub fn new(client: ObjectStore, cache: FileCache) -> Self {
        Self { client, cache }
    }

    pub async fn get(
        &self,
        first_block_number: u64,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        let filename = self.segment_filename(first_block_number);
        self.get_impl(&filename, None, false).await
    }

    pub async fn put(
        &self,
        segment: &CanonicalChainSegment,
    ) -> Result<ObjectVersion, ChainStoreError> {
        let filename = self.segment_filename(segment.info.first_block.number);
        self.put_impl(&filename, segment, PutOptions::default())
            .await
    }

    pub async fn put_recent_snapshot(
        &self,
        segment: &CanonicalChainSegment,
    ) -> Result<RecentSegmentPointer, ChainStoreError> {
        let name = self.recent_snapshot_filename(segment);
        let version = self
            .put_impl(
                &name,
                segment,
                PutOptions {
                    mode: PutMode::Create,
                },
            )
            .await?;

        Ok(RecentSegmentPointer {
            key: self.format_key(&name),
            version: version.0,
            first_block: segment.info.first_block.number,
            last_block: segment.info.last_block.number,
        })
    }

    pub async fn get_recent_snapshot(
        &self,
        pointer: &RecentSegmentPointer,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        validate_recent_pointer(pointer)?;
        let Some(segment) = self
            .get_by_key(&pointer.key, Some(ObjectVersion(pointer.version.clone())))
            .await?
        else {
            return Ok(None);
        };

        if segment.info.first_block.number != pointer.first_block
            || segment.info.last_block.number != pointer.last_block
        {
            return Err(ChainStoreError)
                .attach_printable("recent snapshot pointer does not match segment range")
                .attach_printable_lazy(|| format!("key: {}", pointer.key))
                .attach_printable_lazy(|| format!("pointer first block: {}", pointer.first_block))
                .attach_printable_lazy(|| format!("pointer last block: {}", pointer.last_block))
                .attach_printable_lazy(|| {
                    format!("segment first block: {}", segment.info.first_block.number)
                })
                .attach_printable_lazy(|| {
                    format!("segment last block: {}", segment.info.last_block.number)
                });
        }

        Ok(Some(segment))
    }

    pub(crate) async fn get_legacy_recent(
        &self,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        self.get_impl(LEGACY_RECENT_CHAIN_SEGMENT_NAME, None, true)
            .await
    }

    async fn put_impl(
        &self,
        name: &str,
        segment: &CanonicalChainSegment,
        options: PutOptions,
    ) -> Result<ObjectVersion, ChainStoreError> {
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(segment)
            .change_context(ChainStoreError)
            .attach_printable("failed to serialize chain segment")?;

        let bytes = Bytes::copy_from_slice(serialized.as_slice());

        let response = self
            .client
            .put(&self.format_key(name), bytes, options)
            .await
            .change_context(ChainStoreError)
            .attach_printable("failed to put chain segment")
            .attach_printable_lazy(|| format!("name: {}", name))?;

        Ok(response.version)
    }

    async fn get_impl(
        &self,
        name: &str,
        version: Option<ObjectVersion>,
        skip_cache: bool,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        let key = self.format_key(name);

        if skip_cache {
            let Some(bytes) = self.get_as_bytes(&key, version).await? else {
                return Ok(None);
            };

            let segment = rkyv::from_bytes::<_, rkyv::rancor::Error>(&bytes)
                .change_context(ChainStoreError)
                .attach_printable("failed to deserialize chain segment")
                .attach_printable_lazy(|| format!("name: {}", name))?;

            return Ok(Some(segment));
        }

        if let Some(existing) = self
            .cache
            .general
            .get(&key)
            .await
            .map_err(FileCacheError::Foyer)
            .change_context(ChainStoreError)?
        {
            let segment = rkyv::from_bytes::<_, rkyv::rancor::Error>(existing.value())
                .change_context(ChainStoreError)
                .attach_printable("failed to deserialize chain segment")
                .attach_printable_lazy(|| format!("name: {}", name))?;

            Ok(Some(segment))
        } else {
            let Some(bytes) = self.get_as_bytes(&key, version).await? else {
                return Ok(None);
            };

            let entry = self.cache.general.insert(key, bytes);

            let segment = rkyv::from_bytes::<_, rkyv::rancor::Error>(entry.value())
                .change_context(ChainStoreError)
                .attach_printable("failed to deserialize chain segment")
                .attach_printable_lazy(|| format!("name: {}", name))?;

            Ok(Some(segment))
        }
    }

    async fn get_as_bytes(
        &self,
        key: &str,
        version: Option<ObjectVersion>,
    ) -> Result<Option<Bytes>, ChainStoreError> {
        match self.client.get(key, GetOptions { version }).await {
            Ok(response) => Ok(Some(response.body)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err).change_context(ChainStoreError),
        }
    }

    async fn get_by_key(
        &self,
        key: &str,
        version: Option<ObjectVersion>,
    ) -> Result<Option<CanonicalChainSegment>, ChainStoreError> {
        let Some(bytes) = self.get_as_bytes(key, version).await? else {
            return Ok(None);
        };

        let segment = rkyv::from_bytes::<_, rkyv::rancor::Error>(&bytes)
            .change_context(ChainStoreError)
            .attach_printable("failed to deserialize chain segment")
            .attach_printable_lazy(|| format!("key: {}", key))?;

        Ok(Some(segment))
    }

    fn format_key(&self, key: &str) -> String {
        format!("{}/{}", CANONICAL_PREFIX, key)
    }

    fn segment_filename(&self, first_block: u64) -> String {
        format!("z-{:0>10}", first_block)
    }

    fn recent_snapshot_filename(&self, segment: &CanonicalChainSegment) -> String {
        let first_block = segment.info.first_block.number;
        let last_block = segment.info.last_block.number;
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let counter = RECENT_SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed) % 1_000_000;

        format!(
            "{}/z-{:0>10}-{:0>10}-{:0>20}-{:0>6}",
            RECENT_CHAIN_SEGMENT_PREFIX, first_block, last_block, timestamp_ns, counter
        )
    }
}

fn validate_recent_pointer(pointer: &RecentSegmentPointer) -> Result<(), ChainStoreError> {
    let expected_prefix = format!("{}/{}/", CANONICAL_PREFIX, RECENT_CHAIN_SEGMENT_PREFIX);

    if !pointer.key.starts_with(&expected_prefix) {
        return Err(ChainStoreError)
            .attach_printable("recent snapshot pointer key is outside the recent prefix")
            .attach_printable_lazy(|| format!("key: {}", pointer.key));
    }

    if pointer.version.is_empty() {
        return Err(ChainStoreError)
            .attach_printable("recent snapshot pointer has an empty version")
            .attach_printable_lazy(|| format!("key: {}", pointer.key));
    }

    if pointer.first_block > pointer.last_block {
        return Err(ChainStoreError)
            .attach_printable("recent snapshot pointer has an invalid block range")
            .attach_printable_lazy(|| format!("key: {}", pointer.key))
            .attach_printable_lazy(|| format!("first block: {}", pointer.first_block))
            .attach_printable_lazy(|| format!("last block: {}", pointer.last_block));
    }

    Ok(())
}

impl error_stack::Context for ChainStoreError {}

impl std::fmt::Display for ChainStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain store error")
    }
}
