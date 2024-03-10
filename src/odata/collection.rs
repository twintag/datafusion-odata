#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    #[serde(rename = "$select")]
    pub select: Option<String>,
    #[serde(rename = "$orderby")]
    pub order_by: Option<String>,
    #[serde(rename = "$skip")]
    pub skip: Option<u64>,
    #[serde(rename = "$top")]
    pub top: Option<u64>,
}

impl QueryParams {
    pub fn decode(self) -> QueryParamsDecoded {
        let select = self.select.unwrap_or_default();
        let mut select: Vec<_> = select.split(',').map(|s| s.to_string()).collect();
        select.retain(|i| !i.is_empty());

        let order_by_s = self.order_by.unwrap_or_default();
        let mut order_by_s: Vec<_> = order_by_s.split(',').collect();
        order_by_s.retain(|i| !i.is_empty());

        let mut order_by = Vec::new();
        for el in order_by_s {
            let (cname, asc) = if let Some(cname) = el.strip_suffix(" asc") {
                (cname, true)
            } else if let Some(cname) = el.strip_suffix(" desc") {
                (cname, false)
            } else {
                (el, true)
            };
            order_by.push((cname.to_string(), asc));
        }

        let skip = self.skip.map(|v| v as usize);
        let top = self.top.map(|v| v as usize);

        QueryParamsDecoded {
            select,
            order_by,
            skip,
            top,
        }
    }
}

#[derive(Debug)]
pub struct QueryParamsDecoded {
    pub select: Vec<String>,
    // (column, ascending)
    pub order_by: Vec<(String, bool)>,
    pub skip: Option<usize>,
    pub top: Option<usize>,
}
