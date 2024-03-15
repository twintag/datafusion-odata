// <edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
//   <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:DataServiceVersion="1.0" m:MaxDataServiceVersion="3.0">
//     <Schema xmlns="http://schemas.microsoft.com/ado/2008/09/edm" Namespace="NorthwindModel">
//       <EntityType Name="Employee">
//         <Key>
//           <PropertyRef Name="EmployeeID"/>
//         </Key>
//         <Property Name="LastName" Type="Edm.String" Nullable="false" MaxLength="20" FixedLength="false" Unicode="true"/>

use datafusion::arrow::datatypes::DataType;

#[derive(Debug, serde::Serialize)]
pub struct Edmx {
    #[serde(rename = "edmx:DataServices")]
    pub ds: DataServices,
    #[serde(rename = "@xmlns:edmx")]
    pub ns_edmx: String,
    #[serde(rename = "@Version")]
    pub version: String,
}

impl Edmx {
    pub fn new(ds: DataServices) -> Self {
        Self {
            ds,
            ns_edmx: "http://schemas.microsoft.com/ado/2007/06/edmx".to_string(),
            version: "1.0".to_string(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct DataServices {
    #[serde(rename = "Schema")]
    pub schemas: Vec<Schema>,
    #[serde(rename = "@xmlns:m")]
    pub ns_m: String,
    #[serde(rename = "@m:DataServiceVersion")]
    pub version: String,
    #[serde(rename = "@m:MaxDataServiceVersion")]
    pub max_version: String,
}

impl DataServices {
    pub fn new(schemas: Vec<Schema>) -> Self {
        Self {
            schemas,
            ns_m: "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata".to_string(),
            version: "3.0".to_string(),
            max_version: "3.0".to_string(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Schema {
    #[serde(rename = "@Namespace")]
    pub namespace: String,
    #[serde(rename = "EntityType")]
    pub entity_types: Vec<EntityType>,
    #[serde(rename = "EntityContainer")]
    pub entity_containers: Vec<EntityContainer>,
    #[serde(rename = "@xmlns")]
    pub ns: String,
}

impl Schema {
    pub fn new(
        namespace: String,
        entity_types: Vec<EntityType>,
        entity_containers: Vec<EntityContainer>,
    ) -> Self {
        Self {
            namespace,
            entity_types,
            entity_containers,
            ns: "http://schemas.microsoft.com/ado/2009/11/edm".to_string(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct EntityType {
    #[serde(rename = "@Name")]
    pub name: String,
    #[serde(rename = "Key")]
    pub key: EntityKey,
    #[serde(rename = "Property")]
    pub properties: Vec<Property>,
}

#[derive(Debug, serde::Serialize)]
pub struct EntityKey {
    #[serde(rename = "PropertyRef")]
    key: Vec<PropertyRef>,
}

impl EntityKey {
    pub fn new(key: Vec<PropertyRef>) -> Self {
        Self { key }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct PropertyRef {
    #[serde(rename = "@Name")]
    pub name: String,
}

/// See: https://www.odata.org/documentation/odata-version-3-0/common-schema-definition-language-csdl/
#[derive(Debug, serde::Serialize)]
pub struct Property {
    #[serde(rename = "@Name")]
    pub name: String,
    #[serde(rename = "@Type")]
    pub typ: String,
    #[serde(rename = "@Nullable")]
    pub nullable: bool,
    #[serde(rename = "@FixedLength")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_length: Option<bool>,
    #[serde(rename = "@Unicode")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unicode: Option<bool>,
}

impl Property {
    pub fn primitive(name: impl Into<String>, typ: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            typ: typ.into(),
            nullable,
            fixed_length: None,
            unicode: None,
        }
    }

    pub fn string(name: impl Into<String>, typ: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            typ: typ.into(),
            nullable,
            fixed_length: Some(false),
            unicode: Some(true),
        }
    }
}

// <EntityContainer Name="DemoService" m:IsDefaultEntityContainer="true">
//   <EntitySet Name="Products" EntityType="ODataDemo.Product"/>

#[derive(Debug, serde::Serialize)]
pub struct EntityContainer {
    #[serde(rename = "@Name")]
    pub name: String,
    #[serde(rename = "@m:IsDefaultEntityContainer")]
    pub is_default: bool,
    #[serde(rename = "EntitySet")]
    pub entity_set: Vec<EntitySet>,
}

#[derive(Debug, serde::Serialize)]
pub struct EntitySet {
    #[serde(rename = "@Name")]
    pub name: String,
    #[serde(rename = "@EntityType")]
    pub entity_type: String,
}

///////////////////////////////////////////////////////////////////////////////

// See: https://www.odata.org/documentation/odata-version-3-0/common-schema-definition-language-csdl/
pub fn to_edm_type(dt: &DataType) -> Result<&'static str, UnsupportedDataType> {
    match dt {
        DataType::Null => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Boolean => Ok("Edm.Boolean"),
        // TODO: Use Edm.Byte / Edm.SByte?
        DataType::Int8 => Ok("Edm.Int16"),
        DataType::Int16 => Ok("Edm.Int16"),
        DataType::Int32 => Ok("Edm.Int32"),
        DataType::Int64 => Ok("Edm.Int64"),
        // TODO: Use Edm.Byte / Edm.SByte?
        DataType::UInt8 => Ok("Edm.Int16"),
        DataType::UInt16 => Ok("Edm.Int16"),
        DataType::UInt32 => Ok("Edm.Int32"),
        DataType::UInt64 => Ok("Edm.Int64"),
        DataType::Utf8 => Ok("Edm.String"),
        DataType::Float16 => Ok("Edm.Single"),
        DataType::Float32 => Ok("Edm.Single"),
        DataType::Float64 => Ok("Edm.Double"),
        DataType::Timestamp(_, _) => Ok("Edm.DateTime"),
        DataType::Date32 => Ok("Edm.DateTime"),
        DataType::Date64 => Ok("Edm.DateTime"),
        DataType::Time32(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Time64(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Duration(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Interval(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Binary => Err(UnsupportedDataType::new(dt.clone())),
        DataType::FixedSizeBinary(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::LargeBinary => Err(UnsupportedDataType::new(dt.clone())),
        DataType::LargeUtf8 => Err(UnsupportedDataType::new(dt.clone())),
        DataType::List(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::FixedSizeList(_, _) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::LargeList(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Struct(_) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Union(_, _) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Dictionary(_, _) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Decimal128(_, _) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Decimal256(_, _) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::Map(_, _) => Err(UnsupportedDataType::new(dt.clone())),
        DataType::RunEndEncoded(_, _) => Err(UnsupportedDataType::new(dt.clone())),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unsupported data type: {data_type:?}")]
pub struct UnsupportedDataType {
    pub data_type: DataType,
}

impl UnsupportedDataType {
    pub fn new(data_type: DataType) -> Self {
        Self { data_type }
    }
}
