// <?xml version="1.0" encoding="utf-8"?>
// <service xmlns="http://www.w3.org/2007/app" xmlns:atom="http://www.w3.org/2005/Atom" xml:base="https://services.odata.org/V3/northwind/Northwind.svc/">
// <workspace>
// <atom:title>Default</atom:title>
// <collection href="Categories">
// <atom:title>Categories</atom:title>
// </collection>
#[derive(Debug, serde::Serialize)]
pub struct Service {
    pub workspace: Workspace,
    #[serde(rename = "@xmlns")]
    pub ns: String,
    #[serde(rename = "@xmlns:base")]
    pub ns_base: String,
    #[serde(rename = "@xmlns:atom")]
    pub ns_atom: String,
}

impl Service {
    pub fn new(workspace: Workspace) -> Self {
        Self {
            workspace,
            ns: "http://www.w3.org/2007/app".to_string(),
            ns_base: "https://services.odata.org/V3/northwind/Northwind.svc/".to_string(),
            ns_atom: "http://www.w3.org/2005/Atom".to_string(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Workspace {
    #[serde(rename = "atom:title")]
    pub title: String,
    #[serde(rename = "Collection")]
    pub collections: Vec<Collection>,
}

#[derive(Debug, serde::Serialize)]
pub struct Collection {
    #[serde(rename = "@href")]
    pub href: String,
    #[serde(rename = "atom:title")]
    pub title: String,
}
