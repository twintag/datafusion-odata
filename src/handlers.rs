use std::sync::Arc;

use axum::{
    extract::Query,
    response::{IntoResponse, Response, Result},
    Extension,
};

use crate::{
    collection::QueryParamsRaw,
    context::{CollectionContext, OnUnsupported, ServiceContext, DEFAULT_NAMESPACE},
    error::Error,
    metadata::{
        to_edm_type, DataServices, Edmx, EntityContainer, EntityKey, EntitySet, EntityType,
        Property, PropertyRef,
    },
    service::{Collection, Service, Workspace},
};

pub const MEDIA_TYPE_ATOM: &str = "application/atom+xml;type=feed;charset=utf-8";
pub const MEDIA_TYPE_XML: &str = "application/xml;charset=utf-8";

const DEFAULT_COLLECTION_RESPONSE_SIZE: usize = 512_000;

pub async fn odata_service_handler(
    Extension(odata_ctx): Extension<Arc<dyn ServiceContext>>,
) -> Result<Response<String>> {
    let mut collections = Vec::new();

    for coll in odata_ctx.list_collections().await? {
        collections.push(Collection {
            href: coll.collection_name(),
            title: coll.collection_name(),
        })
    }

    let service = Service::new(
        odata_ctx.service_base_url(),
        Workspace {
            title: DEFAULT_NAMESPACE.to_string(),
            collections,
        },
    );

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(write_object_to_xml("service", &service)?)
        .map_err(Error::from)?)
}

pub async fn odata_metadata_handler(
    Extension(odata_ctx): Extension<Arc<dyn ServiceContext>>,
) -> Result<Response<String>> {
    let mut entity_types = Vec::new();
    let mut entity_container = EntityContainer {
        name: DEFAULT_NAMESPACE.to_string(),
        is_default: true,
        entity_set: Vec::new(),
    };

    for coll in odata_ctx.list_collections().await? {
        let collection_name = coll.collection_name();
        let mut properties = Vec::new();

        for field in coll.schema().await.fields() {
            let typ = match to_edm_type(field.data_type()) {
                Ok(typ) => typ,
                Err(err) => match odata_ctx.on_unsupported_feature() {
                    OnUnsupported::Error => {
                        return Err(Error::UnsupportedFeature(format!(
                            "Unsupported field type {:?}",
                            field.data_type()
                        ))
                        .into());
                    }
                    OnUnsupported::Warn => {
                        tracing::error!(
                            table = collection_name,
                            field = field.name(),
                            error = %err,
                            error_dbg = ?err,
                            "Unsupported field type - skipping",
                        );
                        continue;
                    }
                },
            };

            properties.push(Property::primitive(field.name(), typ, field.is_nullable()));
        }

        // https://www.odata.org/documentation/odata-version-3-0/common-schema-definition-language-csdl/#csdl6.3
        let prop_ref = match properties.first() {
            Some(name) => name,
            None => todo!(),
        };

        entity_types.push(EntityType {
            name: collection_name.clone(),
            key: EntityKey::new(vec![PropertyRef {
                name: prop_ref.name.clone(),
            }]),
            properties,
        });

        entity_container.entity_set.push(EntitySet {
            name: collection_name.clone(),
            entity_type: format!("{DEFAULT_NAMESPACE}.{collection_name}"),
        });
    }

    let metadata = Edmx::new(DataServices::new(vec![crate::metadata::Schema::new(
        DEFAULT_NAMESPACE.to_string(),
        entity_types,
        vec![entity_container],
    )]));

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_XML)
        .body(write_object_to_xml("edmx:Edmx", &metadata)?)
        .map_err(Error::from)?)
}

pub async fn odata_collection_handler(
    Extension(ctx): Extension<Arc<dyn CollectionContext>>,
    Query(query): Query<QueryParamsRaw>,
    _headers: axum::http::HeaderMap,
) -> Result<Response<String>> {
    let query = query.decode();
    tracing::debug!(?query, "Decoded query");

    let df = ctx.query(query).await.map_err(Error::from)?;

    let schema: datafusion::arrow::datatypes::Schema = df.schema().clone().into();
    let record_batches = df.collect().await.map_err(Error::from)?;

    let num_rows: usize = record_batches.iter().map(|b| b.num_rows()).sum();
    let raw_bytes: usize = record_batches
        .iter()
        .map(|b: &datafusion::arrow::array::RecordBatch| b.get_array_memory_size())
        .sum();

    let mut writer = quick_xml::Writer::new(Vec::<u8>::new());

    if ctx.addr().key.is_none() {
        crate::atom::write_atom_feed_from_records(
            &schema,
            record_batches,
            ctx.as_ref(),
            ctx.last_updated_time().await,
            ctx.on_unsupported_feature(),
            &mut writer,
        )
        .map_err(Error::from)?;
    } else {
        let num_rows: usize = record_batches.iter().map(|b| b.num_rows()).sum();
        // TODO
        assert!(num_rows <= 1, "Request by key returned {} rows", num_rows);
        assert!(
            record_batches.len() <= 1,
            "Request by key returned {} batches",
            record_batches.len()
        );

        if record_batches.len() != 1 || record_batches[0].num_rows() != 1 {
            return Ok(Response::builder()
                .status(http::StatusCode::NOT_FOUND)
                .body("".into())
                .map_err(Error::from)?);
        }

        crate::atom::write_atom_entry_from_record(
            &schema,
            record_batches.into_iter().next().unwrap(),
            ctx.as_ref(),
            ctx.last_updated_time().await,
            ctx.on_unsupported_feature(),
            &mut writer,
        )
        .map_err(Error::from)?;
    }

    let body = String::from_utf8(writer.into_inner()).map_err(Error::from)?;

    tracing::debug!(
        media_type = MEDIA_TYPE_ATOM,
        num_rows,
        raw_bytes,
        xml_bytes = body.len(),
        "Prepared a response"
    );

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE.as_str(), MEDIA_TYPE_ATOM)
        .body(body)
        .map_err(Error::from)?)
}

fn write_object_to_xml<T>(tag: &str, object: &T) -> Result<String>
where
    T: serde::ser::Serialize,
{
    let mut writer =
        quick_xml::Writer::new(Vec::<u8>::with_capacity(DEFAULT_COLLECTION_RESPONSE_SIZE));

    writer
        .write_event(quick_xml::events::Event::Decl(
            quick_xml::events::BytesDecl::new("1.0", Some("utf-8"), None),
        ))
        .map_err(Error::from)?;

    writer
        .write_serializable(tag, object)
        .map_err(Error::from)?;

    Ok(String::from_utf8(writer.into_inner()).map_err(Error::from)?)
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        tracing::error!("Internal Error: {self}");
        (http::StatusCode::INTERNAL_SERVER_ERROR, "Internal error").into_response()
    }
}
