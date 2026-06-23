use bytes::Bytes;
use error_stack::Result;

use super::{
    azure_blob::AzureBlobClient, gcs::GcsClient, AwsS3Client, DeleteOptions, GetOptions,
    ObjectStoreError, ObjectVersion, PutOptions,
};

#[derive(Clone)]
pub enum ObjectStoreClient {
    AwsS3(AwsS3Client),
    AzureBlob(Box<AzureBlobClient>),
    Gcs(Box<GcsClient>),
}

impl ObjectStoreClient {
    pub async fn has_bucket(&self, name: &str) -> Result<bool, ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.has_bucket(name).await,
            Self::AzureBlob(client) => client.has_bucket(name).await,
            Self::Gcs(client) => client.has_bucket(name).await,
        }
    }

    pub async fn create_bucket(&self, name: &str) -> Result<(), ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.create_bucket(name).await,
            Self::AzureBlob(client) => client.create_bucket(name).await,
            Self::Gcs(client) => client.create_bucket(name).await,
        }
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        options: GetOptions,
    ) -> Result<(ObjectVersion, Bytes), ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.get_object(bucket, key, options).await,
            Self::AzureBlob(client) => client.get_object(bucket, key, options).await,
            Self::Gcs(client) => client.get_object(bucket, key, options).await,
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        options: PutOptions,
    ) -> Result<ObjectVersion, ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.put_object(bucket, key, body, options).await,
            Self::AzureBlob(client) => client.put_object(bucket, key, body, options).await,
            Self::Gcs(client) => client.put_object(bucket, key, body, options).await,
        }
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<String>, ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.list_objects(bucket, prefix).await,
            Self::AzureBlob(client) => client.list_objects(bucket, prefix).await,
            Self::Gcs(client) => client.list_objects(bucket, prefix).await,
        }
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        _options: DeleteOptions,
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.delete_object(bucket, key, _options).await,
            Self::AzureBlob(client) => client.delete_object(bucket, key, _options).await,
            Self::Gcs(client) => client.delete_object(bucket, key, _options).await,
        }
    }
}

impl From<AwsS3Client> for ObjectStoreClient {
    fn from(client: AwsS3Client) -> Self {
        Self::AwsS3(client)
    }
}

impl From<AzureBlobClient> for ObjectStoreClient {
    fn from(client: AzureBlobClient) -> Self {
        Self::AzureBlob(client.into())
    }
}

impl From<GcsClient> for ObjectStoreClient {
    fn from(client: GcsClient) -> Self {
        Self::Gcs(client.into())
    }
}
