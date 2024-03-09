use datafusion::{arrow::datatypes::DataType, prelude::*};

const XML_DECL: &str = r#"<?xml version="1.0" encoding="utf-8"?>"#;

async fn odata_service_handler(
    axum::extract::State(ctx): axum::extract::State<SessionContext>,
) -> axum::response::Response<String> {
    use test_odata::odata::service::*;

    let mut collections = Vec::new();

    for cname in ctx.catalog_names() {
        let catalog = ctx.catalog(&cname).unwrap();

        for sname in catalog.schema_names() {
            let schema = catalog.schema(&sname).unwrap();

            for tname in schema.table_names() {
                collections.push(Collection {
                    href: tname.clone(),
                    title: tname,
                })
            }
        }
    }

    let service = Service::new(Workspace {
        title: "Default".to_string(),
        collections,
    });

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

    let mut schemas = Vec::new();

    for cname in ctx.catalog_names() {
        let catalog = ctx.catalog(&cname).unwrap();

        for sname in catalog.schema_names() {
            let schema = catalog.schema(&sname).unwrap();

            let mut entity_types = Vec::new();
            for tname in schema.table_names() {
                let table = schema.table(&tname).await.unwrap();

                let mut properties = Vec::new();

                for field in table.schema().fields() {
                    // See: https://www.odata.org/documentation/odata-version-3-0/common-schema-definition-language-csdl/
                    let p = match field.data_type() {
                        DataType::Null => unimplemented!(),
                        DataType::Boolean => {
                            Property::primitive(field.name(), "Edm.Boolean", field.is_nullable())
                        }
                        DataType::Int8 => unimplemented!(),
                        DataType::Int16 => {
                            Property::primitive(field.name(), "Edm.Int16", field.is_nullable())
                        }
                        DataType::Int32 => {
                            Property::primitive(field.name(), "Edm.Int32", field.is_nullable())
                        }
                        DataType::Int64 => {
                            Property::primitive(field.name(), "Edm.Int64", field.is_nullable())
                        }
                        DataType::UInt8 => unimplemented!(),
                        DataType::UInt16 => {
                            Property::primitive(field.name(), "Edm.Int16", field.is_nullable())
                        }
                        DataType::UInt32 => {
                            Property::primitive(field.name(), "Edm.Int32", field.is_nullable())
                        }
                        DataType::UInt64 => {
                            Property::primitive(field.name(), "Edm.Int64", field.is_nullable())
                        }
                        DataType::Utf8 => {
                            Property::string(field.name(), "Edm.String", field.is_nullable())
                        }
                        DataType::Float16 => {
                            Property::primitive(field.name(), "Edm.Boolean", field.is_nullable())
                        }
                        DataType::Float32 => {
                            Property::primitive(field.name(), "Edm.Boolean", field.is_nullable())
                        }
                        DataType::Float64 => {
                            Property::primitive(field.name(), "Edm.Boolean", field.is_nullable())
                        }
                        DataType::Timestamp(_, _) => {
                            Property::primitive(field.name(), "Edm.Boolean", field.is_nullable())
                        }
                        DataType::Date32 => {
                            Property::primitive(field.name(), "Edm.Boolean", field.is_nullable())
                        }
                        DataType::Date64 => {
                            Property::primitive(field.name(), "Edm.Boolean", field.is_nullable())
                        }
                        DataType::Time32(_) => unimplemented!(),
                        DataType::Time64(_) => unimplemented!(),
                        DataType::Duration(_) => unimplemented!(),
                        DataType::Interval(_) => unimplemented!(),
                        DataType::Binary => unimplemented!(),
                        DataType::FixedSizeBinary(_) => unimplemented!(),
                        DataType::LargeBinary => unimplemented!(),
                        DataType::LargeUtf8 => unimplemented!(),
                        DataType::List(_) => unimplemented!(),
                        DataType::FixedSizeList(_, _) => unimplemented!(),
                        DataType::LargeList(_) => unimplemented!(),
                        DataType::Struct(_) => unimplemented!(),
                        DataType::Union(_, _) => unimplemented!(),
                        DataType::Dictionary(_, _) => unimplemented!(),
                        DataType::Decimal128(_, _) => unimplemented!(),
                        DataType::Decimal256(_, _) => unimplemented!(),
                        DataType::Map(_, _) => unimplemented!(),
                        DataType::RunEndEncoded(_, _) => unimplemented!(),
                    };

                    properties.push(p);
                }

                entity_types.push(EntityType {
                    name: tname,
                    key: EntityKey::new(vec![PropertyRef {
                        name: "offset".to_string(),
                    }]),
                    properties,
                });
            }

            schemas.push(Schema::new(sname, entity_types));
        }
    }

    let metadata = Edmx::new(DataServices::new(schemas));

    let xml = quick_xml::se::to_string_with_root("edmx:Edmx", &metadata).unwrap();

    axum::response::Response::builder()
        .header(
            http::header::CONTENT_TYPE.as_str(),
            "application/xml;charset=utf-8",
        )
        .body(format!("{XML_DECL}{xml}"))
        .unwrap()
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
        "covid19.canada",
        "data/covid.parquet",
        ParquetReadOptions {
            file_extension: ".parquet",
            ..Default::default()
        },
    )
    .await
    .unwrap();

    ctx.register_parquet(
        "tickers.spy",
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
