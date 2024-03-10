use std::sync::Arc;

use chrono::Utc;
use datafusion::{arrow::datatypes::Schema, catalog::schema::SchemaProvider, prelude::*};

use test_odata::odata::collection::QueryParams;

const MEDIA_TYPE_ATOM: &str = "application/atom+xml;type=feed;charset=utf-8";
const MEDIA_TYPE_XML: &str = "application/xml;charset=utf-8";

///////////////////////////////////////////////////////////////////////////////
// Mock handlers
///////////////////////////////////////////////////////////////////////////////

async fn mock_odata_service_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("examples/mocks/service.xml").unwrap();

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(body)
        .unwrap()
}

async fn mock_odata_metadata_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("examples/mocks/metadata.xml").unwrap();

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(body)
        .unwrap()
}

async fn mock_odata_collection_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("examples/mocks/collection.xml").unwrap();

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_ATOM)
        .body(body)
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////
// Real handlers
///////////////////////////////////////////////////////////////////////////////

async fn odata_service_handler(
    axum::Extension(odata_ctx): axum::Extension<ODataContext>,
    axum::extract::TypedHeader(host): axum::extract::TypedHeader<axum::headers::Host>,
) -> axum::response::Response<String> {
    use test_odata::odata::service::*;

    let (_, schema) = odata_ctx.get_schema();

    let mut collections = Vec::new();

    for tname in schema.table_names() {
        collections.push(Collection {
            href: tname.clone(),
            title: tname,
        })
    }

    let service = Service::new(
        odata_ctx.service_base_url(&host),
        Workspace {
            title: "Default".to_string(),
            collections,
        },
    );

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(write_object_to_xml("service", &service))
        .unwrap()
}

async fn odata_metadata_handler(
    axum::Extension(odata_ctx): axum::Extension<ODataContext>,
) -> axum::response::Response<String> {
    use test_odata::odata::metadata::*;

    let (schema_name, schema) = odata_ctx.get_schema();

    let mut entity_types = Vec::new();
    let mut entity_container = EntityContainer {
        name: schema_name.clone(),
        is_default: true,
        entity_set: Vec::new(),
    };

    for table_name in schema.table_names() {
        let table = schema.table(&table_name).await.unwrap();

        let mut properties = Vec::new();

        for field in table.schema().fields() {
            let p = Property::primitive(
                field.name(),
                to_edm_type(field.data_type()),
                field.is_nullable(),
            );

            properties.push(p);
        }

        entity_types.push(EntityType {
            name: table_name.clone(),
            key: EntityKey::new(vec![PropertyRef {
                name: "offset".to_string(),
            }]),
            properties,
        });

        entity_container.entity_set.push(EntitySet {
            name: table_name.clone(),
            entity_type: format!("{schema_name}.{table_name}"),
        });
    }

    let metadata = Edmx::new(DataServices::new(vec![Schema::new(
        schema_name.clone(),
        entity_types,
        vec![entity_container],
    )]));

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(write_object_to_xml("edmx:Edmx", &metadata))
        .unwrap()
}

async fn odata_collection_handler(
    axum::Extension(odata_ctx): axum::Extension<ODataContext>,
    axum::extract::TypedHeader(host): axum::extract::TypedHeader<axum::headers::Host>,
    axum::extract::Path(collection_name): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<QueryParams>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response<String> {
    let query = query.decode();
    tracing::debug!(?query, ?headers, "Collection query");

    let df = odata_ctx.query_ctx.table(&collection_name).await.unwrap();
    let df = if query.select.is_empty() {
        df
    } else {
        let select: Vec<_> = query.select.iter().map(String::as_str).collect();
        df.select_columns(&select).unwrap()
    };

    let df = if query.order_by.is_empty() {
        df
    } else {
        df.sort(
            query
                .order_by
                .into_iter()
                .map(|(c, asc)| col(c).sort(asc, true))
                .collect(),
        )
        .unwrap()
    };

    let df = df
        .limit(
            query.skip.unwrap_or(0),
            Some(std::cmp::min(
                query.top.unwrap_or(odata_ctx.default_rows_per_page()),
                odata_ctx.max_rows_per_page(),
            )),
        )
        .unwrap();

    let schema: Schema = df.schema().clone().into();
    let record_batches = df.collect().await.unwrap();

    let num_rows: usize = record_batches.iter().map(|b| b.num_rows()).sum();
    let raw_bytes: usize = record_batches
        .iter()
        .map(|b: &datafusion::arrow::array::RecordBatch| b.get_array_memory_size())
        .sum();

    let mut writer = quick_xml::Writer::new(Vec::<u8>::new());
    test_odata::odata::atom::atom_feed_from_records(
        &schema,
        record_batches,
        &odata_ctx.service_base_url(&host),
        &odata_ctx.collection_base_url(&host, &collection_name),
        &collection_name,
        &collection_name,
        &odata_ctx.namespace_of(&collection_name),
        Utc::now(),
        &mut writer,
    )
    .unwrap();

    let buf = writer.into_inner();
    let body = String::from_utf8(buf).unwrap();

    let xml_bytes = body.len();

    tracing::debug!(
        media_type = MEDIA_TYPE_ATOM,
        num_rows,
        raw_bytes,
        xml_bytes,
        "Prepared a response"
    );

    axum::response::Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_ATOM)
        .body(body)
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

fn write_object_to_xml<T>(tag: &str, object: &T) -> String
where
    T: serde::ser::Serialize,
{
    let mut writer = quick_xml::Writer::new(Vec::<u8>::new());
    writer
        .write_event(quick_xml::events::Event::Decl(
            quick_xml::events::BytesDecl::new("1.0", Some("utf-8"), None),
        ))
        .unwrap();

    writer.write_serializable(tag, object).unwrap();

    String::from_utf8(writer.into_inner()).unwrap()
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ODataContext {
    pub query_ctx: SessionContext,
    service_path: String,
}

impl ODataContext {
    pub fn service_base_url(&self, host: &axum::headers::Host) -> String {
        format!("http://{host}{}", self.service_path)
    }

    pub fn collection_base_url(&self, host: &axum::headers::Host, collection_name: &str) -> String {
        let service_base_url = self.service_base_url(host);
        format!("{service_base_url}{collection_name}")
    }

    pub fn namespace_of(&self, _collection_name: &str) -> String {
        let (schem_name, _) = self.get_schema();
        schem_name
    }

    pub fn get_schema(&self) -> (String, Arc<dyn SchemaProvider>) {
        let cnames = self.query_ctx.catalog_names();
        assert_eq!(
            cnames.len(),
            1,
            "Multiple catalogs not supported: {:?}",
            cnames
        );
        let catalog_name = cnames.first().unwrap();
        let catalog = self.query_ctx.catalog(catalog_name).unwrap();

        let snames = catalog.schema_names();
        assert_eq!(
            snames.len(),
            1,
            "Multiple schemas not supported: {:?}",
            snames
        );
        let schema_name = snames.first().unwrap();
        let schema = catalog.schema(schema_name).unwrap();

        (schema_name.clone(), schema)
    }

    pub fn default_rows_per_page(&self) -> usize {
        100
    }

    pub fn max_rows_per_page(&self) -> usize {
        usize::max_value()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("Initializing");

    ///////////////////////////

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "covid19_canada",
        "examples/data/covid.parquet",
        ParquetReadOptions {
            file_extension: ".parquet",
            ..Default::default()
        },
    )
    .await
    .unwrap();

    ctx.register_parquet(
        "tickers_spy",
        "examples/data/tickers.parquet",
        ParquetReadOptions {
            file_extension: ".parquet",
            ..Default::default()
        },
    )
    .await
    .unwrap();

    ///////////////////////////

    let app = axum::Router::new()
        .route("/", axum::routing::get(odata_service_handler))
        .route("/$metadata", axum::routing::get(odata_metadata_handler))
        .route("/mock", axum::routing::get(mock_odata_service_handler))
        .route("/mock/", axum::routing::get(mock_odata_service_handler))
        .route(
            "/mock/$metadata",
            axum::routing::get(mock_odata_metadata_handler),
        )
        .route(
            "/mock/:collection",
            axum::routing::get(mock_odata_collection_handler),
        )
        .route(
            "/mock/:collection/",
            axum::routing::get(mock_odata_collection_handler),
        )
        .route("/:collection", axum::routing::get(odata_collection_handler))
        .route(
            "/:collection/",
            axum::routing::get(odata_collection_handler),
        )
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(vec![http::Method::GET, http::Method::POST])
                .allow_headers(tower_http::cors::Any),
        )
        .layer(axum::Extension(ODataContext {
            query_ctx: ctx,
            service_path: "/".to_string(),
        }));

    tracing::info!("Runninng");
    let server = axum::Server::bind(&([0, 0, 0, 0], 3000).into()).serve(app.into_make_service());

    if let Err(err) = server.await {
        eprintln!("server error: {}", err);
    }
}
