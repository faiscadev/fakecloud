//! Parquet input files for a distributed Map's `ItemReader`. Rows become JSON
//! with native types, as recorded from AWS: timestamps are integer epoch
//! milliseconds, and lists and structs nest.

use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use serde_json::{json, Map, Value};

fn field_json(field: &Field) -> Value {
    match field {
        Field::TimestampMillis(millis) => json!(millis),
        Field::TimestampMicros(micros) => json!(micros / 1000),
        Field::Group(row) => row_json(row),
        Field::ListInternal(list) => Value::Array(list.elements().iter().map(field_json).collect()),
        Field::MapInternal(map) => {
            let mut out = Map::new();
            for (key, value) in map.entries() {
                let key = match field_json(key) {
                    Value::String(text) => text,
                    other => other.to_string(),
                };
                out.insert(key, field_json(value));
            }
            Value::Object(out)
        }
        other => other.to_json_value(),
    }
}

fn row_json(row: &Row) -> Value {
    Value::Object(
        row.get_column_iter()
            .map(|(name, field)| (name.clone(), field_json(field)))
            .collect(),
    )
}

/// Every row of a Parquet file as a JSON object.
pub(crate) fn read(key: &str, body: &[u8]) -> Result<Vec<Value>, String> {
    let failed = |e: parquet::errors::ParquetError| format!("failed to read {key} as Parquet: {e}");
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(body)).map_err(failed)?;
    let rows = reader.get_row_iter(None).map_err(failed)?;
    rows.map(|row| row.map(|r| row_json(&r)).map_err(failed))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    /// The two rows of the recorded `map_parquet` file.
    fn recorded_file() -> Vec<u8> {
        let schema = parse_message_type(
            "message schema {
                REQUIRED INT64 i;
                REQUIRED DOUBLE f;
                REQUIRED BYTE_ARRAY s (UTF8);
                REQUIRED BOOLEAN b;
                OPTIONAL INT64 n;
                REQUIRED INT64 d (TIMESTAMP_MILLIS);
                REQUIRED GROUP l (LIST) {
                    REPEATED GROUP list { REQUIRED INT64 element; }
                }
                REQUIRED GROUP st { REQUIRED INT64 x; }
            }",
        )
        .unwrap();
        let mut out = Vec::new();
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = SerializedFileWriter::new(&mut out, Arc::new(schema), props).unwrap();
        let mut group = writer.next_row_group().unwrap();
        macro_rules! column {
            ($ty:ty, $values:expr, $def:expr, $rep:expr) => {{
                let mut column = group.next_column().unwrap().unwrap();
                column
                    .typed::<$ty>()
                    .write_batch($values, $def, $rep)
                    .unwrap();
                column.close().unwrap();
            }};
        }
        column!(Int64Type, &[1, 2], None, None);
        column!(DoubleType, &[1.5, 2.5], None, None);
        column!(
            ByteArrayType,
            &[ByteArray::from("a"), ByteArray::from("b")],
            None,
            None
        );
        column!(BoolType, &[true, false], None, None);
        column!(Int64Type, &[7], Some(&[0, 1]), None);
        column!(Int64Type, &[1794229000000, 1794229001000], None, None);
        column!(Int64Type, &[1, 2, 3], Some(&[1, 1, 1]), Some(&[0, 1, 0]));
        column!(Int64Type, &[1, 2], None, None);
        group.close().unwrap();
        writer.close().unwrap();
        out
    }

    #[test]
    fn rows_match_the_recording() {
        let items = read("k", &recorded_file()).unwrap();
        assert_eq!(
            items,
            vec![
                json!({"i":1,"f":1.5,"s":"a","b":true,"n":null,"d":1794229000000i64,"l":[1,2],"st":{"x":1}}),
                json!({"i":2,"f":2.5,"s":"b","b":false,"n":7,"d":1794229001000i64,"l":[3],"st":{"x":2}}),
            ]
        );
    }

    #[test]
    fn a_non_parquet_body_is_an_error() {
        assert!(read("k", b"not parquet").unwrap_err().contains("k"));
    }
}
