use std::sync::Arc;

use datafusion::{catalog::schema::SchemaProvider, dataframe::DataFrame};

use crate::collection::QueryParams;

///////////////////////////////////////////////////////////////////////////////

pub trait ServiceContext: Send + Sync {
    fn service_base_url(&self) -> String;
    fn schema(&self) -> (String, Arc<dyn SchemaProvider>);
}

#[async_trait::async_trait]
pub trait CollectionContext: ServiceContext {
    async fn query(&self, query: QueryParams) -> datafusion::error::Result<DataFrame>;
    fn collection_name(&self) -> String;
    fn collection_namespace(&self) -> String;
    fn collection_base_url(&self) -> String;
}
