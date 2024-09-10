use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::{arrow::datatypes::SchemaRef, dataframe::DataFrame};

use crate::{
    collection::{CollectionAddr, QueryParams},
    error::{Error, Result},
};

///////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_NAMESPACE: &str = "default";

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ServiceContext: Send + Sync {
    fn service_base_url(&self) -> String;

    async fn list_collections(&self) -> Result<Vec<Arc<dyn CollectionContext>>>;

    fn on_unsupported_feature(&self) -> OnUnsupported;
}

#[async_trait::async_trait]
pub trait CollectionContext: Send + Sync {
    fn addr(&self) -> Result<&CollectionAddr>;

    fn service_base_url(&self) -> Result<String>;

    fn collection_base_url(&self) -> Result<String>;

    fn collection_namespace(&self) -> Result<String> {
        Ok(DEFAULT_NAMESPACE.to_string())
    }

    fn collection_name(&self) -> Result<String>;

    // Synthetic column name that will be used to propagate entity IDs
    fn key_column_alias(&self) -> String {
        "__id__".to_string()
    }

    fn key_column(&self) -> Result<String> {
        Err(Error::KeyColumnNotAssigned)
    }

    async fn last_updated_time(&self) -> DateTime<Utc>;

    async fn schema(&self) -> Result<SchemaRef>;

    async fn query(&self, query: QueryParams) -> Result<DataFrame>;

    fn on_unsupported_feature(&self) -> OnUnsupported;
}

pub enum OnUnsupported {
    /// Return an error or crash
    Error,
    /// Log error and recover as gracefully as possible
    Warn,
}
