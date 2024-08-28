use std::io;

use thiserror::Error as ThisError;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("IO error {0}")]
    IO(#[from] io::Error),
    #[error("Datafusion error: {0}")]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error("Address parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("Axum error: {0}")]
    AxumError(#[from] axum::Error),
    #[error("Http error: {0}")]
    HttpError(#[from] http::Error),
    #[error("Quickxml error: {0}")]
    QuickXMLError(#[from] quick_xml::Error),
    #[error("Quickxml deserialise error: {0}")]
    QuickXMLDeError(#[from] quick_xml::DeError),
    #[error("From utf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Unsupported data type: {0}")]
    UnsupportedDataType(datafusion::arrow::datatypes::DataType),
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),
}
