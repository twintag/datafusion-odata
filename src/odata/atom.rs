use std::sync::Arc;

use datafusion::arrow::{
    array::{Array, AsArray, RecordBatch},
    datatypes::*,
};
use quick_xml::events::*;

// https://www.odata.org/documentation/odata-version-3-0/atom-format/
//
// <?xml version="1.0" encoding="utf-8"?>
// <feed xml:base="https://services.odata.org/V3/northwind/Northwind.svc/"
//   xmlns="http://www.w3.org/2005/Atom"
//   xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
//   xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
//   <id>https://services.odata.org/V3/Northwind/Northwind.svc/Orders/</id>
//   <title type="text">Orders</title>
//   <updated>2024-03-10T00:11:11Z</updated>
//   <link rel="self" title="Orders" href="Orders" />
//   <entry>
//     <id>https://services.odata.org/V3/northwind/Northwind.svc/Orders(10248)</id>
//     <category term="NorthwindModel.Order" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
//     <link rel="edit" title="Order" href="Orders(10248)" />
//     <title />
//     <updated>2024-03-10T00:11:11Z</updated>
//     <author>
//       <name />
//     </author>
//     <content type="application/xml">
//       <m:properties>
//         <d:OrderID m:type="Edm.Int32">10248</d:OrderID>
//       </m:properties>
//     </content>
//   </entry>
//   <entry>
//     <id>https://services.odata.org/V3/northwind/Northwind.svc/Orders(10249)</id>
//     <category term="NorthwindModel.Order" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
//     <link rel="edit" title="Order" href="Orders(10249)" />
//     <title />
//     <updated>2024-03-10T00:11:11Z</updated>
//     <author>
//       <name />
//     </author>
//     <content type="application/xml">
//       <m:properties>
//         <d:OrderID m:type="Edm.Int32">10249</d:OrderID>
//       </m:properties>
//     </content>
//   </entry>
// </feed>
pub fn feed_from_records<W>(
    schema: &Schema,
    record_batches: Vec<RecordBatch>,
    writer: &mut quick_xml::Writer<W>,
) -> quick_xml::Result<()>
where
    W: std::io::Write,
{
    let columns: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let column_tags: Vec<_> = columns.iter().map(|c| format!("d:{c}")).collect();

    let column_types: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| super::metadata::to_edm_type(f.data_type()))
        .collect();

    writer.write_event(quick_xml::events::Event::Decl(BytesDecl::new(
        "1.0",
        Some("utf-8"),
        None,
    )))?;

    writer.write_event(Event::Start(BytesStart::new("feed")))?;

    writer
        .create_element("id")
        .write_text_content(BytesText::from_escaped("123"))?;
    writer.create_element("category").write_empty()?;
    writer.create_element("link").write_empty()?;
    writer.create_element("title").write_empty()?;
    writer.create_element("updated").write_empty()?;

    for batch in record_batches {
        for row in 0..batch.num_rows() {
            writer.write_event(Event::Start(BytesStart::new("entry")))?;
            writer
                .create_element("id")
                .write_text_content(BytesText::from_escaped("123"))?;
            writer.create_element("category").write_empty()?;
            writer.create_element("link").write_empty()?;
            writer.create_element("title").write_empty()?;
            writer.create_element("updated").write_empty()?;
            writer
                .create_element("author")
                .write_inner_content::<_, quick_xml::Error>(|writer| {
                    writer.create_element("name").write_empty()?;
                    Ok(())
                })?;

            writer.write_event(Event::Start(BytesStart::new("content")))?;

            for (i, (ctag, typ)) in column_tags.iter().zip(&column_types).enumerate() {
                let col = batch.column(i);

                let mut start = BytesStart::new(ctag);
                start.push_attribute(("m:type", *typ));
                writer.write_event(Event::Start(start))?;

                let val = if col.is_null(row) {
                    BytesText::new("null")
                } else {
                    match col.data_type() {
                        datafusion::arrow::datatypes::DataType::Null => todo!(),
                        datafusion::arrow::datatypes::DataType::Boolean => {
                            let arr = col.as_boolean();
                            let val = arr.value(row).to_string();
                            BytesText::from_escaped(val)
                        }
                        datafusion::arrow::datatypes::DataType::Int8 => {
                            encode_primitive::<Int8Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::Int16 => {
                            encode_primitive::<Int16Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::Int32 => {
                            encode_primitive::<Int32Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::Int64 => {
                            encode_primitive::<Int64Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::UInt8 => {
                            encode_primitive::<UInt8Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::UInt16 => {
                            encode_primitive::<UInt16Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::UInt32 => {
                            encode_primitive::<UInt32Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::UInt64 => {
                            encode_primitive::<UInt64Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::Float16 => {
                            encode_primitive::<Float16Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::Float32 => {
                            encode_primitive::<Float32Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::Float64 => {
                            encode_primitive::<Float64Type>(col, row)
                        }
                        datafusion::arrow::datatypes::DataType::Timestamp(_, _) => {
                            let arr = col.as_primitive::<TimestampMillisecondType>();
                            let ticks = arr.value(row);
                            let ts = chrono::DateTime::from_timestamp_millis(ticks).unwrap();
                            let val = ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                            BytesText::from_escaped(val)
                        }
                        datafusion::arrow::datatypes::DataType::Date32 => todo!(),
                        datafusion::arrow::datatypes::DataType::Date64 => todo!(),
                        datafusion::arrow::datatypes::DataType::Time32(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::Time64(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::Duration(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::Interval(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::Binary => todo!(),
                        datafusion::arrow::datatypes::DataType::FixedSizeBinary(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::LargeBinary => todo!(),
                        datafusion::arrow::datatypes::DataType::Utf8 => {
                            let arr = col.as_string::<i32>();
                            let val = arr.value(row);
                            BytesText::from_escaped(quick_xml::escape::escape(val))
                        }
                        datafusion::arrow::datatypes::DataType::LargeUtf8 => todo!(),
                        datafusion::arrow::datatypes::DataType::List(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
                        datafusion::arrow::datatypes::DataType::LargeList(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::Struct(_) => todo!(),
                        datafusion::arrow::datatypes::DataType::Union(_, _) => todo!(),
                        datafusion::arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
                        datafusion::arrow::datatypes::DataType::Decimal128(_, _) => todo!(),
                        datafusion::arrow::datatypes::DataType::Decimal256(_, _) => todo!(),
                        datafusion::arrow::datatypes::DataType::Map(_, _) => todo!(),
                        datafusion::arrow::datatypes::DataType::RunEndEncoded(_, _) => todo!(),
                    }
                };

                writer.write_event(Event::Text(val))?;
                writer.write_event(Event::End(BytesEnd::new(ctag)))?;
            }

            writer.write_event(Event::End(BytesEnd::new("content")))?;
            writer.write_event(Event::End(BytesEnd::new("entry")))?;
        }
    }

    writer.write_event(Event::End(BytesEnd::new("feed")))?;

    Ok(())
}

fn encode_primitive<T>(arr: &Arc<dyn Array>, row: usize) -> BytesText
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: std::fmt::Display,
{
    let arr = arr.as_primitive::<T>();
    let val = arr.value(row).to_string();
    BytesText::from_escaped(val)
}
