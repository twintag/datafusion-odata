use std::{str::FromStr, sync::Arc};

use datafusion::{arrow::datatypes::Schema, catalog::schema::SchemaProvider, prelude::*};
use http::{uri::PathAndQuery, Uri};
use test_odata::odata::query::*;

const XML_DECL: &str = r#"<?xml version="1.0" encoding="utf-8"?>"#;

async fn mock_odata_service_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("mock/service.xml").unwrap();

    axum::response::Response::builder()
        .header(
            http::header::CONTENT_TYPE.as_str(),
            "application/xml;charset=utf-8",
        )
        .body(body)
        .unwrap()
}

async fn mock_odata_metadata_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("mock/metadata.xml").unwrap();

    axum::response::Response::builder()
        .header(
            http::header::CONTENT_TYPE.as_str(),
            "application/xml;charset=utf-8",
        )
        .body(body)
        .unwrap()
}

async fn mock_odata_collection_handler() -> axum::response::Response<String> {
    let body = std::fs::read_to_string("mock/collection.xml").unwrap();

    axum::response::Response::builder()
        .header(
            http::header::CONTENT_TYPE.as_str(),
            "application/atom+xml;type=feed;charset=utf-8",
        )
        .body(body)
        .unwrap()
}

async fn odata_service_handler(
    axum::extract::State(ctx): axum::extract::State<SessionContext>,
    axum::extract::TypedHeader(host): axum::extract::TypedHeader<axum::headers::Host>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
) -> axum::response::Response<String> {
    use test_odata::odata::service::*;

    let (_catalog_name_, _schema_name, schema) = get_schema(&ctx);

    let mut collections = Vec::new();

    for tname in schema.table_names() {
        collections.push(Collection {
            href: tname.clone(),
            title: tname,
        })
    }

    let service = Service::new(
        format!("http://{host}{uri}"),
        Workspace {
            title: "Default".to_string(),
            collections,
        },
    );

    let xml = quick_xml::se::to_string_with_root("service", &service).unwrap();

    axum::response::Response::builder()
        .header(
            http::header::CONTENT_TYPE.as_str(),
            "application/xml;charset=utf-8",
        )
        .body(format!("{XML_DECL}{xml}"))
        .unwrap()
}

async fn odata_metadata_handler(
    axum::extract::State(ctx): axum::extract::State<SessionContext>,
) -> axum::response::Response<String> {
    use test_odata::odata::metadata::*;

    let (_catalog_name, schema_name, schema) = get_schema(&ctx);

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

    let xml = quick_xml::se::to_string_with_root("edmx:Edmx", &metadata).unwrap();

    axum::response::Response::builder()
        .header(
            http::header::CONTENT_TYPE.as_str(),
            "application/xml;charset=utf-8",
        )
        .body(format!("{XML_DECL}{xml}"))
        .unwrap()
}

async fn odata_collection_handler(
    axum::extract::State(ctx): axum::extract::State<SessionContext>,
    axum::extract::TypedHeader(host): axum::extract::TypedHeader<axum::headers::Host>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    axum::extract::Path(collection): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<ODataQuery>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response<String> {
    tracing::debug!(?query, ?headers, "Collection query");

    let select_cols = query.select.unwrap_or_default();
    let mut select_cols: Vec<_> = select_cols.split(',').collect();
    select_cols.retain(|c| !c.is_empty());

    let skip = query.skip.unwrap_or_default() as usize;
    let limit = query.top.unwrap_or(100) as usize;

    let df = ctx.table(&collection).await.unwrap();
    let df = if select_cols.is_empty() {
        df
    } else {
        df.select_columns(&select_cols).unwrap()
    };

    let df = if let Some(order_by) = query.order_by {
        let (cname, asc) = if let Some(cname) = order_by.strip_suffix(" asc") {
            (cname, true)
        } else if let Some(cname) = order_by.strip_suffix(" desc") {
            (cname, false)
        } else {
            (order_by.as_str(), true)
        };

        df.sort(vec![col(cname).sort(asc, false)]).unwrap()
    } else {
        df
    };

    let df = df.limit(skip, Some(limit)).unwrap();
    let schema: Schema = df.schema().clone().into();

    let record_batches = df.collect().await.unwrap();

    // Remove query params
    let mut parts = uri.into_parts();
    parts.path_and_query = parts
        .path_and_query
        .map(|pq| PathAndQuery::from_str(pq.path()).unwrap());
    let uri = Uri::from_parts(parts).unwrap();
    let base_url = format!("http://{host}{uri}");

    let mut writer = quick_xml::Writer::new(Vec::<u8>::new());
    test_odata::odata::atom::feed_from_records(&schema, record_batches, &base_url, &mut writer)
        .unwrap();

    let buf = writer.into_inner();
    let body = String::from_utf8(buf).unwrap();

    axum::response::Response::builder()
        .header(
            http::header::CONTENT_TYPE.as_str(),
            "application/atom+xml;type=feed;charset=utf-8",
        )
        .body(body)
        .unwrap()
}

fn get_schema(ctx: &SessionContext) -> (String, String, Arc<dyn SchemaProvider>) {
    let cnames = ctx.catalog_names();
    assert_eq!(
        cnames.len(),
        1,
        "Multiple catalogs not supported: {:?}",
        cnames
    );
    let catalog_name = cnames.first().unwrap();
    let catalog = ctx.catalog(catalog_name).unwrap();

    let snames = catalog.schema_names();
    assert_eq!(
        snames.len(),
        1,
        "Multiple schemas not supported: {:?}",
        snames
    );
    let schema_name = snames.first().unwrap();
    let schema = catalog.schema(schema_name).unwrap();

    (catalog_name.clone(), schema_name.clone(), schema)
}

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
        "data/covid.parquet",
        ParquetReadOptions {
            file_extension: ".parquet",
            ..Default::default()
        },
    )
    .await
    .unwrap();

    ctx.register_parquet(
        "tickers_spy",
        "data/tickers.parquet",
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
        .with_state(ctx);

    tracing::info!("Runninng");
    let server = axum::Server::bind(&([0, 0, 0, 0], 3000).into()).serve(app.into_make_service());

    if let Err(err) = server.await {
        eprintln!("server error: {}", err);
    }
}
