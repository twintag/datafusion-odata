use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::{arrow::datatypes::SchemaRef, dataframe::DataFrame};

use crate::collection::QueryParams;

///////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_NAMESPACE: &str = "default";

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ServiceContext: Send + Sync {
    fn service_base_url(&self) -> String;

    async fn list_collections(&self) -> Vec<Arc<dyn CollectionContext>>;

    fn on_unsupported_feature(&self) -> OnUnsupported;
}

#[async_trait::async_trait]
pub trait CollectionContext: Send + Sync {
    fn service_base_url(&self) -> String;

    fn collection_base_url(&self) -> String;

    fn collection_namespace(&self) -> String {
        DEFAULT_NAMESPACE.to_string()
    }

    fn collection_name(&self) -> String;

    async fn collection_key(&self) -> String;

    async fn last_updated_time(&self) -> DateTime<Utc>;

    async fn schema(&self) -> SchemaRef;

    async fn query(&self, query: QueryParams) -> datafusion::error::Result<DataFrame>;

    fn on_unsupported_feature(&self) -> OnUnsupported;
}

pub enum OnUnsupported {
    /// Return an error or crash
    Error,
    /// Log error and recover as gracefully as possible
    Warn,
}
