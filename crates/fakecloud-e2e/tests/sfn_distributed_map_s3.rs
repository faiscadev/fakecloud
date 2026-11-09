//! Distributed Map reading its items from S3 (`ItemReader`) and writing its
//! results there (`ResultWriter`), plus S3 reached from an `aws-sdk:s3:*` Task.

mod helpers;

use helpers::TestServer;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

const ROLE: &str = "arn:aws:iam::123456789012:role/sfn-role";
const BUCKET: &str = "map-data";

async fn put(server: &TestServer, key: &str, body: impl Into<Vec<u8>>) {
    server
        .s3_client()
        .await
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(body.into().into())
        .send()
        .await
        .unwrap();
}

async fn create_bucket(server: &TestServer) {
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
}

fn map_definition(mut map_fields: Value) -> Value {
    let map = map_fields.as_object_mut().unwrap();
    map.insert("Type".into(), json!("Map"));
    map.insert(
        "ItemProcessor".into(),
        json!({
            "ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
            "StartAt": "P",
            "States": {"P": {"Type": "Pass", "End": true}}
        }),
    );
    map.insert("MaxConcurrency".into(), json!(2));
    map.insert("End".into(), json!(true));
    json!({"StartAt": "M", "States": {"M": map_fields}})
}

async fn run(
    server: &TestServer,
    definition: Value,
    input: Value,
) -> aws_sdk_sfn::operation::describe_execution::DescribeExecutionOutput {
    let client = server.sfn_client().await;
    let created = client
        .create_state_machine()
        .name(format!("sm-{}", uuid::Uuid::new_v4().simple()))
        .definition(definition.to_string())
        .role_arn(ROLE)
        .send()
        .await
        .unwrap();
    let started = client
        .start_execution()
        .state_machine_arn(created.state_machine_arn())
        .input(input.to_string())
        .send()
        .await
        .unwrap();
    for _ in 0..400 {
        let desc = client
            .describe_execution()
            .execution_arn(started.execution_arn())
            .send()
            .await
            .unwrap();
        if desc.status().as_str() != "RUNNING" {
            return desc;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("execution did not finish");
}

async fn run_ok(server: &TestServer, definition: Value, input: Value) -> Value {
    let desc = run(server, definition, input).await;
    assert_eq!(
        desc.status().as_str(),
        "SUCCEEDED",
        "error={:?} cause={:?}",
        desc.error(),
        desc.cause()
    );
    serde_json::from_str(desc.output().expect("output")).unwrap()
}

async fn read_object(server: &TestServer, key: &str) -> Vec<u8> {
    server
        .s3_client()
        .await
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes()
        .to_vec()
}

fn get_object_reader(key: &str, reader_config: Value) -> Value {
    json!({ "ItemReader": {
        "Resource": "arn:aws:states:::s3:getObject",
        "ReaderConfig": reader_config,
        "Parameters": {"Bucket": BUCKET, "Key": key}
    }})
}

#[tokio::test]
async fn item_reader_reads_a_json_array() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "manifest.json", "[1,2,3]").await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "manifest.json",
            json!({"InputType": "JSON"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(output, json!([1, 2, 3]));
}

// As AWS refuses it: a `getObject` reader must say what it is reading.
#[tokio::test]
async fn get_object_reader_without_reader_config_is_an_invalid_definition() {
    let server = TestServer::start().await;
    let err = server
        .sfn_client()
        .await
        .create_state_machine()
        .name("no-reader-config")
        .role_arn(ROLE)
        .definition(
            map_definition(json!({ "ItemReader": {
                "Resource": "arn:aws:states:::s3:getObject",
                "Parameters": {"Bucket": BUCKET, "Key": "k"}
            }}))
            .to_string(),
        )
        .send()
        .await
        .expect_err("refused at creation")
        .into_service_error();
    assert!(err.is_invalid_definition(), "{err:?}");
    assert_eq!(
        err.meta().message(),
        Some(
            "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: These fields are \
             required: [ReaderConfig] at /States/M/ItemReader'"
        )
    );
}

#[tokio::test]
async fn item_reader_feeds_item_batcher() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "manifest.json", "[1,2,3]").await;

    let mut fields = get_object_reader("manifest.json", json!({"InputType": "JSON"}));
    fields["ItemBatcher"] = json!({"MaxItemsPerBatch": 2});
    let output = run_ok(&server, map_definition(fields), json!({})).await;

    let batches = output.as_array().unwrap();
    assert_eq!(batches.len(), 2, "{output}");
    assert_eq!(batches[0], json!({"Items": [1, 2]}));
    assert_eq!(batches[1], json!({"Items": [3]}));
}

#[tokio::test]
async fn item_reader_resolves_parameter_paths_and_encodes_keys() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    let key = "dir/with space/caf\u{e9}+100%.json";
    put(&server, key, "[\"ok\"]").await;

    let definition = map_definition(json!({
        "ItemReader": {
            "Resource": "arn:aws:states:::s3:getObject",
            "ReaderConfig": {"InputType": "JSON"},
            "Parameters": {"Bucket.$": "$.bucket", "Key.$": "$.key"}
        }
    }));
    let output = run_ok(&server, definition, json!({"bucket": BUCKET, "key": key})).await;

    assert_eq!(output, json!(["ok"]));
}

#[tokio::test]
async fn item_reader_items_pointer_selects_a_nested_array() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "doc.json", r#"{"data":{"rows":["a","b"]}}"#).await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "doc.json",
            json!({"InputType": "JSON", "ItemsPointer": "/data/rows"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(output, json!(["a", "b"]));
}

// Recorded: an object file iterates as its entries, a scalar isn't iterable.
#[tokio::test]
async fn item_reader_iterates_object_entries_and_refuses_scalars() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "object.json", r#"{"v":1}"#).await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "object.json",
            json!({"InputType": "JSON"}),
        )),
        json!({}),
    )
    .await;
    assert_eq!(output, json!([{"Key": "v", "Value": 1}]));

    let desc = run(
        &server,
        map_definition(get_object_reader(
            "object.json",
            json!({"InputType": "JSON", "ItemsPointer": "/v"}),
        )),
        json!({}),
    )
    .await;
    assert_eq!(desc.error(), Some("States.ItemReaderFailed"));
    assert_eq!(
        desc.cause(),
        Some("Attempting to map over non-iterable node.")
    );
}

#[tokio::test]
async fn item_reader_reads_json_lines() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "rows.jsonl", "{\"n\":1}\n\n{\"n\":2}\r\n").await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "rows.jsonl",
            json!({"InputType": "JSONL"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(output, json!([{"n": 1}, {"n": 2}]));
}

#[tokio::test]
async fn item_reader_reads_csv_with_a_header_row() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(
        &server,
        "rows.csv",
        "id,note\n1,\"has, comma\"\r\n2,\"say \"\"hi\"\"\"\n",
    )
    .await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "rows.csv",
            json!({"InputType": "CSV", "CSVHeaderLocation": "FIRST_ROW"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(
        output,
        json!([
            {"id": "1", "note": "has, comma"},
            {"id": "2", "note": "say \"hi\""}
        ])
    );
}

// The state machine and CSV file of AWS's "How Step Functions parses input CSV
// files" page, whose documented output this reproduces.
#[tokio::test]
async fn item_reader_parses_the_documented_csv_example() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(
        &server,
        "myCSVInput.csv",
        "abc,123,\"This string contains commas, a double quote (\"\"), and a newline (\n)\",\
         {\"\"MyKey\"\":\"\"MyValue\"\"},\"[1,2,3]\"\n",
    )
    .await;

    let mut fields = get_object_reader(
        "myCSVInput.csv",
        json!({
            "InputType": "CSV",
            "CSVHeaderLocation": "GIVEN",
            "CSVHeaders": ["MyLetters", "MyNumbers", "MyString", "MyObject", "MyArray"]
        }),
    );
    fields["ItemSelector"] = json!({
        "MyLetters.$": "$$.Map.Item.Value.MyLetters",
        "MyNumbers.$": "States.StringToJson($$.Map.Item.Value.MyNumbers)",
        "MyString.$": "$$.Map.Item.Value.MyString",
        "MyObject.$": "States.StringToJson($$.Map.Item.Value.MyObject)",
        "MyArray.$": "States.StringToJson($$.Map.Item.Value.MyArray)"
    });
    let output = run_ok(&server, map_definition(fields), json!({})).await;

    assert_eq!(
        output,
        json!([{
            "MyNumbers": 123,
            "MyObject": {"MyKey": "MyValue"},
            "MyString": "This string contains commas, a double quote (\"), and a newline (\n)",
            "MyLetters": "abc",
            "MyArray": [1, 2, 3]
        }])
    );
}

#[tokio::test]
async fn item_reader_parses_the_recorded_csv() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(
        &server,
        "c.csv",
        "a,b\n\"\"\"x,y\"\"\",1\n\"plain\",2\n\"\",3\n{\"\"K\"\":\"\"V\"\"},4\n\"\"lead,5\n",
    )
    .await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "c.csv",
            json!({"InputType": "CSV", "CSVHeaderLocation": "FIRST_ROW"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(
        output,
        json!([
            {"a": "\"x,y\"", "b": "1"},
            {"a": "plain", "b": "2"},
            {"a": "", "b": "3"},
            {"a": "{\"K\":\"V\"}", "b": "4"},
            {"a": "lead", "b": "5"}
        ])
    );
}

#[tokio::test]
async fn item_reader_reads_csv_with_given_headers_and_a_delimiter() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "rows.psv", "1|a\n2|b\n").await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "rows.psv",
            json!({
                "InputType": "CSV",
                "CSVHeaderLocation": "GIVEN",
                "CSVHeaders": ["id", "name"],
                "CSVDelimiter": "PIPE"
            }),
        )),
        json!({}),
    )
    .await;

    assert_eq!(
        output,
        json!([{"id": "1", "name": "a"}, {"id": "2", "name": "b"}])
    );
}

#[tokio::test]
async fn item_reader_honors_max_items_and_max_items_path() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "rows.json", "[1,2,3,4]").await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "rows.json",
            json!({"InputType": "JSON", "MaxItems": 2}),
        )),
        json!({}),
    )
    .await;
    assert_eq!(output, json!([1, 2]));

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "rows.json",
            json!({"InputType": "JSON", "MaxItemsPath": "$.limit"}),
        )),
        json!({"limit": 3}),
    )
    .await;
    assert_eq!(output, json!([1, 2, 3]));
}

#[tokio::test]
async fn item_reader_lists_objects_under_a_prefix_across_pages() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    // More than one ListObjectsV2 page of the size the reader asks for.
    for i in 0..5 {
        put(&server, &format!("in/k{i}.txt"), "x").await;
    }
    put(&server, "other/skip.txt", "x").await;

    let output = run_ok(
        &server,
        map_definition(json!({
            "ItemReader": {
                "Resource": "arn:aws:states:::s3:listObjectsV2",
                "Parameters": {"Bucket": BUCKET, "Prefix": "in/", "MaxKeys": 2}
            }
        })),
        json!({}),
    )
    .await;

    let items = output.as_array().unwrap();
    let keys: Vec<&str> = items.iter().map(|i| i["Key"].as_str().unwrap()).collect();
    assert_eq!(
        keys,
        [
            "in/k0.txt",
            "in/k1.txt",
            "in/k2.txt",
            "in/k3.txt",
            "in/k4.txt"
        ]
    );
    let first = &items[0];
    assert_eq!(first["Size"], 1);
    assert_eq!(first["StorageClass"], "STANDARD");
    assert!(first["Etag"].as_str().unwrap().starts_with('"'), "{first}");
    assert!(first["LastModified"].is_f64(), "{first}");
    assert_eq!(first.as_object().unwrap().len(), 5, "{first}");
}

#[tokio::test]
async fn item_reader_keeps_spaces_around_listed_keys() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "in/ spaced .txt", "x").await;

    let output = run_ok(
        &server,
        map_definition(json!({
            "ItemReader": {
                "Resource": "arn:aws:states:::s3:listObjectsV2",
                "Parameters": {"Bucket": BUCKET, "Prefix": "in/"}
            }
        })),
        json!({}),
    )
    .await;

    assert_eq!(output[0]["Key"], "in/ spaced .txt");
}

#[tokio::test]
async fn item_reader_load_and_flatten_reads_every_listed_object() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "in/a.csv", "id\n1\n2\n").await;
    put(&server, "in/b.csv", "id\n3\n").await;

    let output = run_ok(
        &server,
        map_definition(json!({
            "ItemReader": {
                "Resource": "arn:aws:states:::s3:listObjectsV2",
                "ReaderConfig": {"InputType": "CSV", "Transformation": "LOAD_AND_FLATTEN"},
                "Parameters": {"Bucket": BUCKET, "Prefix": "in/"}
            }
        })),
        json!({}),
    )
    .await;

    assert_eq!(output, json!([{"id": "1"}, {"id": "2"}, {"id": "3"}]));
}

fn gzip(data: &str) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data.as_bytes()).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn item_reader_reads_an_s3_inventory_manifest() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(
        &server,
        "inv/data/part-0.csv.gz",
        gzip("\"src\",\"a%20b.txt\",\"10\"\n\"src\",\"c.txt\",\"20\"\n"),
    )
    .await;
    put(
        &server,
        "inv/manifest.json",
        json!({
            "sourceBucket": "src",
            "destinationBucket": format!("arn:aws:s3:::{BUCKET}"),
            "fileFormat": "CSV",
            "fileSchema": "Bucket, Key, Size",
            "files": [{"key": "inv/data/part-0.csv.gz", "size": 1, "MD5checksum": "x"}]
        })
        .to_string(),
    )
    .await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "inv/manifest.json",
            json!({"InputType": "MANIFEST"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(
        output,
        json!([
            {"Bucket": "src", "Key": "a%20b.txt", "Size": "10"},
            {"Bucket": "src", "Key": "c.txt", "Size": "20"}
        ])
    );
}

#[tokio::test]
async fn manifest_type_cannot_join_input_type_manifest() {
    let server = TestServer::start().await;
    let err = server
        .sfn_client()
        .await
        .create_state_machine()
        .name("manifest-both")
        .role_arn(ROLE)
        .definition(
            map_definition(get_object_reader(
                "m",
                json!({"InputType": "MANIFEST", "ManifestType": "S3_INVENTORY"}),
            ))
            .to_string(),
        )
        .send()
        .await
        .expect_err("refused at creation")
        .into_service_error();
    assert_eq!(
        err.meta().message(),
        Some(
            "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Field 'ManifestType' \
             is not supported at /States/M/ItemReader/ReaderConfig'"
        )
    );
}

#[tokio::test]
async fn item_reader_manifest_type_s3_inventory_needs_no_input_type() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "inv/part.csv.gz", gzip("\"src\",\"a.txt\"\n")).await;
    put(
        &server,
        "inv/manifest.json",
        json!({
            "destinationBucket": format!("arn:aws:s3:::{BUCKET}"),
            "fileFormat": "CSV",
            "fileSchema": "Bucket, Key",
            "files": [{"key": "inv/part.csv.gz"}]
        })
        .to_string(),
    )
    .await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "inv/manifest.json",
            json!({"ManifestType": "S3_INVENTORY"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(output, json!([{"Bucket": "src", "Key": "a.txt"}]));
}

#[tokio::test]
async fn item_reader_reads_an_athena_manifest() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "unload/one.gz", gzip("{\"n\":1}\n")).await;
    put(&server, "unload/two", "{\"n\":2}\n").await;
    put(
        &server,
        "unload/q-manifest.csv",
        format!("s3://{BUCKET}/unload/one.gz\ns3://{BUCKET}/unload/two\n"),
    )
    .await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "unload/q-manifest.csv",
            json!({"ManifestType": "ATHENA_DATA", "InputType": "JSONL"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(output, json!([{"n": 1}, {"n": 2}]));
}

#[tokio::test]
async fn item_reader_failure_is_catchable_as_item_reader_failed() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "bad.json", "not json").await;

    let desc = run(
        &server,
        map_definition(get_object_reader(
            "missing.json",
            json!({"InputType": "JSON"}),
        )),
        json!({}),
    )
    .await;
    assert_eq!(desc.status().as_str(), "FAILED");
    assert_eq!(desc.error(), Some("States.ItemReaderFailed"));
    let cause = desc.cause().unwrap();
    assert!(
        cause.starts_with("The specified S3 object does not exist. | Message from S3: The specified key does not exist. (Service: S3, Status Code: 404, Request ID: "),
        "{cause}"
    );
    assert!(cause.ends_with(") (SDK Attempt Count: 1)"), "{cause}");

    let desc = run(
        &server,
        map_definition(get_object_reader("bad.json", json!({"InputType": "JSON"}))),
        json!({}),
    )
    .await;
    assert_eq!(desc.error(), Some("States.ItemReaderFailed"));
    assert!(desc.cause().unwrap().contains("parse"), "{desc:?}");

    // A failed read still happened inside a map run, and history says so.
    let client = server.sfn_client().await;
    let runs = client
        .list_map_runs()
        .execution_arn(desc.execution_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(runs.map_runs().len(), 1);
    let history = client
        .get_execution_history()
        .execution_arn(desc.execution_arn())
        .send()
        .await
        .unwrap();
    let types: Vec<&str> = history
        .events()
        .iter()
        .map(|e| e.r#type().as_str())
        .collect();
    assert!(types.contains(&"MapRunFailed"), "{types:?}");
    assert!(types.contains(&"MapStateFailed"), "{types:?}");
}

#[tokio::test]
async fn item_reader_reads_parquet_rows() {
    use parquet::basic::{ConvertedType, Repetition, Type as PhysicalType};
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;
    use std::sync::Arc;

    let id = Type::primitive_type_builder("id", PhysicalType::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();
    let name = Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .with_converted_type(ConvertedType::UTF8)
        .build()
        .unwrap();
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(id), Arc::new(name)])
        .build()
        .unwrap();
    let mut file = Vec::new();
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(&mut file, Arc::new(schema), props).unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&[1, 2], None, None)
        .unwrap();
    column.close().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<ByteArrayType>()
        .write_batch(&[ByteArray::from("a"), ByteArray::from("b")], None, None)
        .unwrap();
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();

    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "rows.parquet", file).await;
    put(&server, "bad.parquet", "PAR1").await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "rows.parquet",
            json!({"InputType": "PARQUET"}),
        )),
        json!({}),
    )
    .await;
    assert_eq!(
        output,
        json!([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    );

    let desc = run(
        &server,
        map_definition(get_object_reader(
            "bad.parquet",
            json!({"InputType": "PARQUET"}),
        )),
        json!({}),
    )
    .await;
    assert_eq!(desc.status().as_str(), "FAILED");
    assert_eq!(desc.error(), Some("States.ItemReaderFailed"));
    assert!(desc.cause().unwrap().contains("Parquet"), "{desc:?}");
}

#[tokio::test]
async fn item_reader_reads_a_disk_backed_object() {
    let tmp = tempfile::tempdir().unwrap();
    let server = TestServer::start_persistent(tmp.path()).await;
    create_bucket(&server).await;
    put(&server, "manifest.json", "[1,2,3]").await;

    let output = run_ok(
        &server,
        map_definition(get_object_reader(
            "manifest.json",
            json!({"InputType": "JSON"}),
        )),
        json!({}),
    )
    .await;

    assert_eq!(output, json!([1, 2, 3]));
}

#[tokio::test]
async fn result_writer_writes_a_manifest_and_result_files() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "manifest.json", "[1,2]").await;

    let mut fields = get_object_reader("manifest.json", json!({"InputType": "JSON"}));
    fields["ResultWriter"] = json!({
        "Resource": "arn:aws:states:::s3:putObject",
        "Parameters": {"Bucket.$": "$.bucket", "Prefix": "out"}
    });
    let output = run_ok(&server, map_definition(fields), json!({"bucket": BUCKET})).await;

    assert!(
        output["MapRunArn"].as_str().unwrap().contains(":mapRun:"),
        "{output}"
    );
    assert_eq!(output["ResultWriterDetails"]["Bucket"], BUCKET);
    let manifest_key = output["ResultWriterDetails"]["Key"].as_str().unwrap();
    assert!(manifest_key.starts_with("out/"), "{manifest_key}");
    assert!(manifest_key.ends_with("/manifest.json"), "{manifest_key}");

    let manifest: Value =
        serde_json::from_slice(&read_object(&server, manifest_key).await).unwrap();
    assert_eq!(manifest["DestinationBucket"], BUCKET);
    assert_eq!(manifest["MapRunArn"], output["MapRunArn"]);
    assert_eq!(manifest["ResultFiles"]["FAILED"], json!([]));
    assert_eq!(manifest["ResultFiles"]["PENDING"], json!([]));
    let succeeded = manifest["ResultFiles"]["SUCCEEDED"].as_array().unwrap();
    assert_eq!(succeeded.len(), 1, "{manifest}");
    let results_key = succeeded[0]["Key"].as_str().unwrap();
    assert!(results_key.ends_with("/SUCCEEDED_0.json"), "{results_key}");

    let body = read_object(&server, results_key).await;
    assert_eq!(succeeded[0]["Size"], body.len());
    // The recorded stored types.
    let s3 = server.s3_client().await;
    let head = |key: String| {
        let s3 = s3.clone();
        async move {
            s3.head_object()
                .bucket(BUCKET)
                .key(key)
                .send()
                .await
                .unwrap()
                .content_type()
                .map(String::from)
        }
    };
    assert_eq!(
        head(results_key.to_string()).await.as_deref(),
        Some("binary/octet-stream")
    );
    assert_eq!(
        head(manifest_key.to_string()).await.as_deref(),
        Some("application/octet-stream")
    );
    let results: Value = serde_json::from_slice(&body).unwrap();
    let results = results.as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["Status"], "SUCCEEDED");
    assert_eq!(results[0]["Input"], "1");
    assert_eq!(results[0]["Output"], "1");
    assert_eq!(results[1]["Output"], "2");
    assert!(results[0]["ExecutionArn"].is_string());
    assert_eq!(results[0]["RedriveStatus"], "NOT_REDRIVABLE");
    assert_eq!(results[0]["InputDetails"]["Included"], true);
    assert_eq!(results[0]["OutputDetails"]["Included"], true);
    assert!(results[0]["StartDate"].as_str().unwrap().ends_with('Z'));
    // `<prefix>/<map run uuid>/`, the map run ARN's last segment.
    let run_id = output["MapRunArn"]
        .as_str()
        .unwrap()
        .rsplit(':')
        .next()
        .unwrap();
    assert_eq!(uuid::Uuid::parse_str(run_id).unwrap().to_string(), run_id);
    assert_eq!(manifest_key, format!("out/{run_id}/manifest.json"));
}

#[tokio::test]
async fn result_writer_records_tolerated_failures_in_their_own_file() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    let definition = json!({"StartAt":"M","States":{"M":{
        "Type":"Map",
        "ItemProcessor":{
            "ProcessorConfig":{"Mode":"DISTRIBUTED"},
            "StartAt":"C",
            "States":{
                "C":{"Type":"Choice","Choices":[{"Variable":"$","NumericEquals":2,"Next":"F"}],"Default":"P"},
                "P":{"Type":"Pass","End":true},
                "F":{"Type":"Fail","Error":"Bad","Cause":"item two"}
            }
        },
        "ToleratedFailurePercentage":100,
        "ResultWriter":{
            "Resource":"arn:aws:states:::s3:putObject",
            "Parameters":{"Bucket":BUCKET,"Prefix":"out/"}
        },
        "End":true
    }}});
    let output = run_ok(&server, definition, json!([1, 2])).await;

    let manifest_key = output["ResultWriterDetails"]["Key"].as_str().unwrap();
    let manifest: Value =
        serde_json::from_slice(&read_object(&server, manifest_key).await).unwrap();
    let failed_key = manifest["ResultFiles"]["FAILED"][0]["Key"]
        .as_str()
        .unwrap();
    let failed: Value = serde_json::from_slice(&read_object(&server, failed_key).await).unwrap();
    assert_eq!(failed[0]["Status"], "FAILED");
    assert_eq!(failed[0]["Error"], "Bad");
    assert_eq!(failed[0]["Cause"], "item two");
    assert_eq!(failed[0]["Input"], "2");
    assert_eq!(failed[0]["RedriveStatus"], "REDRIVABLE_BY_MAP_RUN");
    assert_eq!(failed[0]["OutputDetails"]["Included"], true);
    assert_eq!(failed[0]["RedriveCount"], 0);
}

fn failing_map(extra: Value) -> Value {
    let mut map = json!({
        "Type":"Map",
        "ItemProcessor":{
            "ProcessorConfig":{"Mode":"DISTRIBUTED"},
            "StartAt":"F",
            "States":{"F":{"Type":"Fail","Error":"Boom","Cause":"c"}}
        },
        "MaxConcurrency":2,
        "ResultWriter":{
            "Resource":"arn:aws:states:::s3:putObject",
            "Parameters":{"Bucket":BUCKET,"Prefix":"out"}
        },
        "End":true
    });
    for (key, value) in extra.as_object().unwrap() {
        map[key] = value.clone();
    }
    json!({"StartAt":"M","States":{"M":map}})
}

// Recorded: with no tolerance the run still reports its results, then fails with
// its own error rather than the iteration's.
#[tokio::test]
async fn map_run_past_tolerance_writes_results_then_fails() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    let desc = run(&server, failing_map(json!({})), json!([1, 2, 3])).await;

    assert_eq!(desc.status().as_str(), "FAILED");
    assert_eq!(desc.error(), Some("States.ExceedToleratedFailureThreshold"));
    assert_eq!(
        desc.cause(),
        Some("The specified tolerated failure threshold was exceeded")
    );

    let client = server.sfn_client().await;
    let history = client
        .get_execution_history()
        .execution_arn(desc.execution_arn())
        .send()
        .await
        .unwrap();
    let failure = history
        .events()
        .iter()
        .find(|e| e.r#type().as_str() == "MapRunFailed")
        .expect("MapRunFailed");
    let details = failure.map_run_failed_event_details().expect("details");
    assert_eq!(
        details.error(),
        Some("States.ExceedToleratedFailureThreshold")
    );
    assert!(history
        .events()
        .iter()
        .any(|e| e.r#type().as_str() == "MapStateFailed"));

    let listed = server
        .s3_client()
        .await
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("out/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = listed.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys.len(), 2, "{keys:?}");
    let manifest_key = keys.iter().find(|k| k.ends_with("/manifest.json")).unwrap();
    let manifest: Value =
        serde_json::from_slice(&read_object(&server, manifest_key).await).unwrap();
    assert_eq!(manifest["ResultFiles"]["SUCCEEDED"], json!([]));
    assert_eq!(manifest["ResultFiles"]["PENDING"], json!([]));
    let failed_key = manifest["ResultFiles"]["FAILED"][0]["Key"]
        .as_str()
        .unwrap();
    let failed: Value = serde_json::from_slice(&read_object(&server, failed_key).await).unwrap();
    assert_eq!(failed.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn tolerated_failure_count_decides_the_map_run() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    let within = run(
        &server,
        failing_map(json!({"ToleratedFailureCount": 5})),
        json!([1, 2, 3]),
    )
    .await;
    assert_eq!(
        within.status().as_str(),
        "SUCCEEDED",
        "{:?}",
        within.cause()
    );
    let output: Value = serde_json::from_str(within.output().unwrap()).unwrap();
    assert!(output["ResultWriterDetails"]["Key"].is_string(), "{output}");

    let past = run(
        &server,
        failing_map(json!({"ToleratedFailureCount": 1})),
        json!([1, 2, 3]),
    )
    .await;
    assert_eq!(past.error(), Some("States.ExceedToleratedFailureThreshold"));

    let by_path = run(
        &server,
        failing_map(json!({
            "ToleratedFailureCountPath": "$.limit",
            "ItemsPath": "$.items"
        })),
        json!({"limit": 3, "items": [1, 2, 3]}),
    )
    .await;
    assert_eq!(
        by_path.status().as_str(),
        "SUCCEEDED",
        "{:?}",
        by_path.cause()
    );
}

#[tokio::test]
async fn tolerated_failure_percentage_decides_the_map_run() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    let definition = |percentage: f64| {
        json!({"StartAt":"M","States":{"M":{
            "Type":"Map",
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"DISTRIBUTED"},
                "StartAt":"C",
                "States":{
                    "C":{"Type":"Choice","Choices":[{"Variable":"$","NumericEquals":1,"Next":"F"}],"Default":"P"},
                    "P":{"Type":"Pass","End":true},
                    "F":{"Type":"Fail","Error":"Bad","Cause":"c"}
                }
            },
            "ToleratedFailurePercentage":percentage,
            "End":true
        }}})
    };

    // One failure of four is 25%.
    let within = run(&server, definition(25.0), json!([1, 2, 3, 4])).await;
    assert_eq!(
        within.status().as_str(),
        "SUCCEEDED",
        "{:?}",
        within.cause()
    );
    let past = run(&server, definition(24.0), json!([1, 2, 3, 4])).await;
    assert_eq!(past.error(), Some("States.ExceedToleratedFailureThreshold"));
}

// Recorded: 40 failing items at concurrency 2 ran 3 and left 37 pending, whose
// records hold only their input.
#[tokio::test]
async fn items_a_failed_map_run_never_started_are_pending() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    let items: Vec<i32> = (0..40).collect();
    let desc = run(&server, failing_map(json!({})), json!(items)).await;
    assert_eq!(desc.error(), Some("States.ExceedToleratedFailureThreshold"));

    let listed = server
        .s3_client()
        .await
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("out/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = listed.contents().iter().filter_map(|o| o.key()).collect();
    let manifest_key = keys.iter().find(|k| k.ends_with("/manifest.json")).unwrap();
    let manifest: Value =
        serde_json::from_slice(&read_object(&server, manifest_key).await).unwrap();
    let pending_key = manifest["ResultFiles"]["PENDING"][0]["Key"]
        .as_str()
        .unwrap();
    let failed_key = manifest["ResultFiles"]["FAILED"][0]["Key"]
        .as_str()
        .unwrap();
    let pending: Value = serde_json::from_slice(&read_object(&server, pending_key).await).unwrap();
    let failed: Value = serde_json::from_slice(&read_object(&server, failed_key).await).unwrap();
    assert_eq!(failed.as_array().unwrap().len(), 3, "{failed}");
    assert_eq!(pending.as_array().unwrap().len(), 37);
    assert_eq!(
        pending[0],
        json!({"Input": "3", "InputDetails": {"Included": true}, "Status": "PENDING"})
    );
}

#[tokio::test]
async fn result_writer_failure_is_catchable_as_result_writer_failed() {
    let server = TestServer::start().await;

    let definition = json!({"StartAt":"M","States":{"M":{
        "Type":"Map",
        "ItemProcessor":{
            "ProcessorConfig":{"Mode":"DISTRIBUTED"},
            "StartAt":"P",
            "States":{"P":{"Type":"Pass","End":true}}
        },
        "ResultWriter":{
            "Resource":"arn:aws:states:::s3:putObject",
            "Parameters":{"Bucket":"no-such-bucket-here","Prefix":"out"}
        },
        "End":true
    }}});
    let desc = run(&server, definition, json!([1])).await;

    assert_eq!(desc.status().as_str(), "FAILED");
    assert_eq!(desc.error(), Some("States.ResultWriterFailed"));
    assert!(desc.cause().unwrap().contains("bucket"), "{desc:?}");
}

#[tokio::test]
async fn aws_sdk_s3_put_serializes_an_object_body() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    run_ok(
        &server,
        json!({"StartAt":"P","States":{"P":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:putObject",
            "Parameters":{"Bucket":BUCKET,"Key":"put.json","Body":{"x":1}},
            "End":true
        }}}),
        json!({}),
    )
    .await;

    assert_eq!(read_object(&server, "put.json").await, b"{\"x\":1}");
    let stored = server
        .s3_client()
        .await
        .head_object()
        .bucket(BUCKET)
        .key("put.json")
        .send()
        .await
        .unwrap();
    assert_eq!(stored.content_type(), Some("text/plain; charset=UTF-8"));
}

#[tokio::test]
async fn aws_sdk_s3_tasks_reach_the_object_api() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    let definition = json!({"StartAt":"Put","States":{
        "Put":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:putObject",
            "Parameters":{"Bucket":BUCKET,"Key":"sdk/with space.txt","Body":"hello","ContentType":"text/plain"},
            "ResultPath":"$.put",
            "Next":"Get"
        },
        "Get":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters":{"Bucket":BUCKET,"Key":"sdk/with space.txt"},
            "ResultPath":"$.get",
            "Next":"Head"
        },
        "Head":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:headObject",
            "Parameters":{"Bucket":BUCKET,"Key":"sdk/with space.txt"},
            "ResultPath":"$.head",
            "Next":"List"
        },
        "List":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:listObjectsV2",
            "Parameters":{"Bucket":BUCKET,"Prefix":"sdk/"},
            "ResultPath":"$.list",
            "Next":"Delete"
        },
        "Delete":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:deleteObject",
            "Parameters":{"Bucket":BUCKET,"Key":"sdk/with space.txt"},
            "ResultPath":"$.delete",
            "End":true
        }
    }});
    let output = run_ok(&server, definition, json!({})).await;

    assert!(output["put"]["ETag"].is_string(), "{output}");
    assert_eq!(output["get"]["Body"], "hello");
    assert_eq!(output["get"]["ContentLength"], 5);
    assert_eq!(output["get"]["ContentType"], "text/plain");
    assert_eq!(output["head"]["ContentLength"], 5);
    assert_eq!(output["list"]["KeyCount"], 1);
    assert_eq!(output["list"]["Contents"][0]["Key"], "sdk/with space.txt");
    assert_eq!(output["list"]["Contents"][0]["Size"], 5);
    // The recorded shapes: ISO dates, metadata always present.
    assert_eq!(output["get"]["Metadata"], json!({}));
    assert_eq!(output["get"]["AcceptRanges"], "bytes");
    assert!(output["get"]["LastModified"]
        .as_str()
        .unwrap()
        .ends_with('Z'));
    assert_eq!(output["put"]["ChecksumType"], "FULL_OBJECT");
    assert!(output["put"]["ChecksumCRC32"].is_string(), "{output}");
    assert_eq!(output["put"]["ServerSideEncryption"], "AES256");
    assert_eq!(output["get"]["ServerSideEncryption"], "AES256");

    // Recorded: the SDK behind a Task sees the missing bucket for what it is.
    let no_bucket = run(
        &server,
        json!({"StartAt":"G","States":{"G":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters":{"Bucket":"no-such-bucket-here","Key":"k"},
            "End":true
        }}}),
        json!({}),
    )
    .await;
    assert_eq!(no_bucket.error(), Some("S3.NoSuchBucketException"));
    assert!(
        no_bucket.cause().unwrap().starts_with(
            "The specified bucket does not exist (Service: S3, Status Code: 404, Request ID: "
        ),
        "{no_bucket:?}"
    );

    let missing = run(
        &server,
        json!({"StartAt":"G","States":{"G":{
            "Type":"Task",
            "Resource":"arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters":{"Bucket":BUCKET,"Key":"missing"},
            "End":true
        }}}),
        json!({}),
    )
    .await;
    assert_eq!(missing.error(), Some("S3.NoSuchKeyException"));
    let cause = missing.cause().unwrap();
    assert!(
        cause.starts_with(
            "The specified key does not exist. (Service: S3, Status Code: 404, Request ID: "
        ),
        "{cause}"
    );

    let gone = server
        .s3_client()
        .await
        .get_object()
        .bucket(BUCKET)
        .key("sdk/with space.txt")
        .send()
        .await;
    assert!(gone.is_err(), "deleteObject must remove the object");
}

#[tokio::test]
async fn invalid_tolerance_paths_fail_the_map_run() {
    let server = TestServer::start().await;
    create_bucket(&server).await;

    for (field, value) in [
        ("ToleratedFailureCountPath", json!("many")),
        ("ToleratedFailurePercentagePath", json!(250)),
    ] {
        let desc = run(
            &server,
            failing_map(json!({ field: "$.limit", "ItemsPath": "$.items" })),
            json!({"limit": value, "items": [1]}),
        )
        .await;
        assert_eq!(desc.error(), Some("States.Runtime"), "{field}");
    }
}

#[tokio::test]
async fn describe_map_run_reports_the_resolved_tolerance() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    let client = server.sfn_client().await;

    let desc = run(
        &server,
        failing_map(json!({
            "ToleratedFailureCountPath": "$.count",
            "ToleratedFailurePercentagePath": "$.percent",
            "ItemsPath": "$.items"
        })),
        json!({"count": 7, "percent": 80, "items": [1, 2]}),
    )
    .await;
    assert_eq!(desc.status().as_str(), "FAILED");

    let runs = client
        .list_map_runs()
        .execution_arn(desc.execution_arn())
        .send()
        .await
        .unwrap();
    let map_run = client
        .describe_map_run()
        .map_run_arn(runs.map_runs()[0].map_run_arn())
        .send()
        .await
        .unwrap();
    assert_eq!(map_run.tolerated_failure_count(), 7);
    assert_eq!(map_run.tolerated_failure_percentage(), 80.0);
}

// What lies past `MaxItems` is never read, so it can't fail the run.
#[tokio::test]
async fn max_items_stops_reading_at_the_limit() {
    let server = TestServer::start().await;
    create_bucket(&server).await;
    put(&server, "in/a.jsonl", "{\"n\":1}\n").await;
    put(&server, "in/b.jsonl", "not json\n").await;

    let output = run_ok(
        &server,
        map_definition(json!({
            "ItemReader": {
                "Resource": "arn:aws:states:::s3:listObjectsV2",
                "ReaderConfig": {
                    "InputType": "JSONL",
                    "Transformation": "LOAD_AND_FLATTEN",
                    "MaxItems": 1
                },
                "Parameters": {"Bucket": BUCKET, "Prefix": "in/"}
            }
        })),
        json!({}),
    )
    .await;
    assert_eq!(output, json!([{"n": 1}]));
}
