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
pub fn to_edm_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Null => unimplemented!(),
        DataType::Boolean => "Edm.Boolean",
        DataType::Int8 => unimplemented!(),
        DataType::Int16 => "Edm.Int16",
        DataType::Int32 => "Edm.Int32",
        DataType::Int64 => "Edm.Int64",
        DataType::UInt8 => unimplemented!(),
        DataType::UInt16 => "Edm.Int16",
        DataType::UInt32 => "Edm.Int32",
        DataType::UInt64 => "Edm.Int64",
        DataType::Utf8 => "Edm.String",
        DataType::Float16 => "Edm.Single",
        DataType::Float32 => "Edm.Single",
        DataType::Float64 => "Edm.Double",
        DataType::Timestamp(_, _) => "Edm.DateTime",
        DataType::Date32 => "Edm.DateTime",
        DataType::Date64 => "Edm.DateTime",
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
    }
}
