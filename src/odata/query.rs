#[derive(Debug, serde::Deserialize)]
pub struct ODataQuery {
    #[serde(rename = "$select")]
    pub select: Option<String>,
    #[serde(rename = "$orderby")]
    pub order_by: Option<String>,
    #[serde(rename = "$skip")]
    pub skip: Option<u64>,
    #[serde(rename = "$top")]
    pub top: Option<u64>,
}
