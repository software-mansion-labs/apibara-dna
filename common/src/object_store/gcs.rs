use bytes::{Bytes, BytesMut};
use error_stack::{Result, ResultExt};
use futures::future::BoxFuture;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_storage::client::{Storage, StorageControl};

use crate::object_store::ObjectStoreResultExt;

use super::{
    error::ToObjectStoreResult, metrics::ObjectStoreMetrics, DeleteOptions, GetOptions,
    ObjectStoreError, ObjectVersion, PutMode, PutOptions,
};

/// Google Cloud Storage backend.
///
/// GCS uses an `i64` *generation* number as its optimistic-concurrency token
/// rather than an HTTP ETag. We store that generation, as a string, inside the
/// generic [`ObjectVersion`] so the rest of the object store stays cloud-agnostic:
///
/// * `GetOptions.version` / `PutMode::Update` -> `set_if_generation_match(<gen>)`
/// * `PutMode::Create` -> `set_if_generation_match(0)` (object must not exist)
///
/// The per-operation methods return boxed (`BoxFuture`) futures on purpose: the
/// SDK builds very deeply-nested generic async types, and erasing them here keeps
/// every downstream crate that awaits the object store under the default
/// `recursion_limit`. The client is cheap to clone (the inner handles are
/// `Arc`-backed), so each call clones it to produce a `'static` future.
#[derive(Clone)]
pub struct GcsClient {
    storage: Storage,
    control: StorageControl,
    /// Required only to create buckets (`projects/{project_id}`).
    project_id: Option<String>,
    metrics: ObjectStoreMetrics,
}

impl GcsClient {
    pub(crate) fn from_clients(
        storage: Storage,
        control: StorageControl,
        project_id: Option<String>,
    ) -> Self {
        Self {
            storage,
            control,
            project_id,
            metrics: ObjectStoreMetrics::default(),
        }
    }

    pub async fn new(
        project_id: Option<String>,
        endpoint: Option<String>,
    ) -> Result<Self, ObjectStoreError> {
        let mut storage_builder = Storage::builder();
        let mut control_builder = StorageControl::builder();

        if let Some(endpoint) = endpoint.as_ref() {
            storage_builder = storage_builder.with_endpoint(endpoint.clone());
            control_builder = control_builder.with_endpoint(endpoint.clone());
        }

        let storage = storage_builder
            .build()
            .await
            .change_context(ObjectStoreError::Configuration)
            .attach_printable("failed to build GCS storage client")?;
        let control = control_builder
            .build()
            .await
            .change_context(ObjectStoreError::Configuration)
            .attach_printable("failed to build GCS control client")?;

        Ok(Self::from_clients(storage, control, project_id))
    }

    pub fn has_bucket(&self, name: &str) -> BoxFuture<'static, Result<bool, ObjectStoreError>> {
        let this = self.clone();
        let name = name.to_string();
        Box::pin(async move {
            match this
                .control
                .get_bucket()
                .set_name(bucket_path(&name))
                .send()
                .await
                .change_to_object_store_context()
            {
                Ok(_) => Ok(true),
                Err(err) if err.is_not_found() => Ok(false),
                Err(err) => Err(err),
            }
        })
    }

    pub fn create_bucket(&self, name: &str) -> BoxFuture<'static, Result<(), ObjectStoreError>> {
        let this = self.clone();
        let name = name.to_string();
        Box::pin(async move {
            let project_id = this
                .project_id
                .as_ref()
                .ok_or(ObjectStoreError::Configuration)
                .attach_printable("a GCP project id is required to create a bucket")
                .attach_printable("set GOOGLE_CLOUD_PROJECT, or pre-create the bucket")?;

            this.control
                .create_bucket()
                .set_parent(format!("projects/{project_id}"))
                .set_bucket_id(name)
                .send()
                .await
                .change_to_object_store_context()?;
            Ok(())
        })
    }

    pub fn get_object(
        &self,
        bucket: &str,
        key: &str,
        options: GetOptions,
    ) -> BoxFuture<'static, Result<(ObjectVersion, Bytes), ObjectStoreError>> {
        let this = self.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            this.metrics.get.add(1, &[]);

            let mut request = this.storage.read_object(bucket_path(&bucket), key);
            if let Some(version) = options.version.as_ref() {
                request = request.set_if_generation_match(parse_generation(version)?);
            }

            let mut response = request.send().await.change_to_object_store_context()?;

            let mut output = BytesMut::new();
            while let Some(chunk) = response
                .next()
                .await
                .transpose()
                .change_to_object_store_context()?
            {
                output.extend_from_slice(&chunk);
            }

            let version = ObjectVersion(response.object().generation.to_string());

            Ok((version, output.freeze()))
        })
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        options: PutOptions,
    ) -> BoxFuture<'static, Result<ObjectVersion, ObjectStoreError>> {
        let this = self.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            this.metrics.put.add(1, &[]);

            let mut request = this.storage.write_object(bucket_path(&bucket), key, body);
            request = match &options.mode {
                PutMode::Overwrite => request,
                // generation 0 means "the object must not already exist".
                PutMode::Create => request.set_if_generation_match(0_i64),
                PutMode::Update(version) => {
                    request.set_if_generation_match(parse_generation(version)?)
                }
            };

            let object = request
                .send_unbuffered()
                .await
                .change_to_object_store_context()?;

            Ok(ObjectVersion(object.generation.to_string()))
        })
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> BoxFuture<'static, Result<Vec<String>, ObjectStoreError>> {
        let this = self.clone();
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        Box::pin(async move {
            this.metrics.list.add(1, &[]);

            let mut items = this
                .control
                .list_objects()
                .set_parent(bucket_path(&bucket))
                .set_prefix(prefix)
                .by_item();

            let mut object_ids = Vec::new();
            while let Some(item) = items.next().await {
                let object = item.change_to_object_store_context()?;
                object_ids.push(object.name);
            }

            Ok(object_ids)
        })
    }

    pub fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        _options: DeleteOptions,
    ) -> BoxFuture<'static, Result<(), ObjectStoreError>> {
        let this = self.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        Box::pin(async move {
            this.metrics.delete.add(1, &[]);

            this.control
                .delete_object()
                .set_bucket(bucket_path(&bucket))
                .set_object(key)
                .send()
                .await
                .change_to_object_store_context()?;
            Ok(())
        })
    }
}

/// Build the GCS resource path for a bucket (`projects/_/buckets/{name}`).
fn bucket_path(name: &str) -> String {
    format!("projects/_/buckets/{name}")
}

/// Parse a generation number out of the opaque [`ObjectVersion`].
fn parse_generation(version: &ObjectVersion) -> Result<i64, ObjectStoreError> {
    version
        .0
        .parse::<i64>()
        .change_context(ObjectStoreError::Metadata)
        .attach_printable_lazy(|| format!("invalid GCS generation token: {}", version.0))
}

impl<T> ToObjectStoreResult for std::result::Result<T, google_cloud_gax::error::Error> {
    type Ok = T;

    fn change_to_object_store_context(self) -> Result<T, ObjectStoreError> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => match err.http_status_code() {
                // GCS returns 412 for generation-match failures and 304 for
                // not-modified; 409 can surface on create races.
                Some(412) | Some(409) => Err(err).change_context(ObjectStoreError::Precondition),
                Some(304) => Err(err).change_context(ObjectStoreError::NotModified),
                Some(404) => Err(err).change_context(ObjectStoreError::NotFound),
                _ => Err(err).change_context(ObjectStoreError::Request),
            },
        }
    }
}
