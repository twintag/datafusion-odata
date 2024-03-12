use datafusion::prelude::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
pub struct QueryParamsRaw {
    #[serde(rename = "$select")]
    pub select: Option<String>,
    #[serde(rename = "$orderby")]
    pub order_by: Option<String>,
    #[serde(rename = "$skip")]
    pub skip: Option<u64>,
    #[serde(rename = "$top")]
    pub top: Option<u64>,
}

///////////////////////////////////////////////////////////////////////////////

impl QueryParamsRaw {
    pub fn decode(self) -> QueryParams {
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

        QueryParams {
            select,
            order_by,
            skip,
            top,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct QueryParams {
    /// Column names
    pub select: Vec<String>,
    /// Tuples (column_name, ascending)
    pub order_by: Vec<(String, bool)>,
    /// Number of records to skip
    pub skip: Option<usize>,
    /// Maximum number of records to return
    pub top: Option<usize>,
}

///////////////////////////////////////////////////////////////////////////////

impl QueryParams {
    pub fn apply(
        self,
        df: DataFrame,
        default_rows: usize,
        max_rows: usize,
    ) -> datafusion::error::Result<DataFrame> {
        // Select columns
        let df = if self.select.is_empty() {
            df
        } else {
            let select: Vec<_> = self.select.iter().map(String::as_str).collect();
            df.select_columns(&select)?
        };

        // Order by
        let df = if self.order_by.is_empty() {
            df
        } else {
            df.sort(
                self.order_by
                    .into_iter()
                    .map(|(c, asc)| col(c).sort(asc, true))
                    .collect(),
            )?
        };

        // Skip / limit
        df.limit(
            self.skip.unwrap_or(0),
            Some(std::cmp::min(self.top.unwrap_or(default_rows), max_rows)),
        )
    }
}
