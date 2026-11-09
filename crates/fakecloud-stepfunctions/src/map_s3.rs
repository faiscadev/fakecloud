//! A distributed Map's S3 input and output: decoding the objects an
//! `ItemReader` names into items, and the files a `ResultWriter` leaves behind.

use std::io::Read;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};

use crate::s3_integration::S3Caller;
use crate::sdk_protocol::SdkFailure;

type StateError = (String, String);

const LIST_OBJECTS_V2: &str = "s3:listObjectsV2";
const GET_OBJECT: &str = "s3:getObject";

fn reader_failed(cause: impl Into<String>) -> StateError {
    ("States.ItemReaderFailed".to_string(), cause.into())
}

fn writer_failed(cause: impl Into<String>) -> StateError {
    ("States.ResultWriterFailed".to_string(), cause.into())
}

/// An S3 call failing the read, as AWS words it. The exception suffix belongs to
/// the SDK message `S3Caller` builds.
fn reader_s3_failed(s3: &S3Caller<'_>, err: SdkFailure) -> StateError {
    let (_, detail) = crate::sdk_protocol::task_error(s3.service(), &err);
    if err.code == "NoSuchKey" {
        return reader_failed(format!(
            "The specified S3 object does not exist. | Message from S3: {detail}"
        ));
    }
    reader_failed(detail)
}

/// An `ItemReader` once its `Parameters` and `MaxItemsPath` are resolved.
pub(crate) struct ItemReader {
    /// The integration's `s3:<action>` tail.
    pub(crate) action: String,
    pub(crate) params: Value,
    pub(crate) config: Value,
    /// `MaxItems` / `MaxItemsPath`; `None` when unset or zero.
    pub(crate) max_items: Option<usize>,
}

impl ItemReader {
    /// Whether `resource` names an integration an `ItemReader` can read from.
    pub(crate) fn supports(action: &str) -> bool {
        action == GET_OBJECT || action == LIST_OBJECTS_V2
    }

    pub(crate) async fn read(&self, s3: &S3Caller<'_>) -> Result<Vec<Value>, StateError> {
        let bucket = self.params["Bucket"]
            .as_str()
            .ok_or_else(|| reader_failed("ItemReader missing Bucket"))?;
        let input_type = self.config["InputType"].as_str().unwrap_or("JSON");

        let mut items = if self.action == LIST_OBJECTS_V2 {
            self.read_listing(s3, bucket, input_type).await?
        } else {
            let key = self.params["Key"]
                .as_str()
                .ok_or_else(|| reader_failed("ItemReader missing Key"))?;
            let version_id = self.params["VersionId"].as_str();
            let body = s3
                .get_object(bucket, key, version_id)
                .await
                .map_err(|e| reader_s3_failed(s3, e))?;
            let body = decompress(key, &body)?;
            match self.config["ManifestType"].as_str() {
                Some("ATHENA_DATA") => self.read_athena_manifest(s3, key, &body).await?,
                Some("S3_INVENTORY") => self.read_inventory_manifest(s3, &body).await?,
                Some(other) => {
                    return Err(reader_failed(format!(
                        "ItemReader ManifestType {other} is not supported"
                    )))
                }
                None if input_type == "MANIFEST" => self.read_inventory_manifest(s3, &body).await?,
                None => self.decode(input_type, key, &body)?,
            }
        };

        if let Some(max) = self.max_items {
            items.truncate(max);
        }
        Ok(items)
    }

    async fn read_listing(
        &self,
        s3: &S3Caller<'_>,
        bucket: &str,
        input_type: &str,
    ) -> Result<Vec<Value>, StateError> {
        let flatten = self.config["Transformation"].as_str() == Some("LOAD_AND_FLATTEN");
        let mut items = Vec::new();
        let mut params = self.params.clone();
        loop {
            let page = s3
                .list_objects_v2(&params)
                .await
                .map_err(|e| reader_s3_failed(s3, e))?;
            for object in page["Contents"].as_array().into_iter().flatten() {
                if !flatten {
                    items.push(listed_item(object));
                    continue;
                }
                let Some(key) = object["Key"].as_str() else {
                    continue;
                };
                // A zero-byte `dir/` object is a console folder, not data.
                if key.ends_with('/') {
                    continue;
                }
                let body = s3
                    .get_object(bucket, key, None)
                    .await
                    .map_err(|e| reader_s3_failed(s3, e))?;
                let body = decompress(key, &body)?;
                items.extend(self.decode(input_type, key, &body)?);
            }
            if self.max_items.is_some_and(|max| items.len() >= max) {
                return Ok(items);
            }
            match page["NextContinuationToken"].as_str() {
                Some(token) if page["IsTruncated"] == json!(true) => {
                    params["ContinuationToken"] = json!(token);
                }
                _ => return Ok(items),
            }
        }
    }

    /// Items from an Athena `UNLOAD` manifest: one `s3://` data file per line,
    /// each decoded as `InputType`.
    async fn read_athena_manifest(
        &self,
        s3: &S3Caller<'_>,
        manifest_key: &str,
        manifest: &[u8],
    ) -> Result<Vec<Value>, StateError> {
        let input_type = self.config["InputType"].as_str().unwrap_or("CSV");
        let mut items = Vec::new();
        for line in utf8(manifest_key, manifest)?.lines().map(str::trim) {
            if line.is_empty() {
                continue;
            }
            let (bucket, key) = line
                .strip_prefix("s3://")
                .and_then(|rest| rest.split_once('/'))
                .ok_or_else(|| {
                    reader_failed(format!("{manifest_key} lists a non-S3 location: {line}"))
                })?;
            let body = s3
                .get_object(bucket, key, None)
                .await
                .map_err(|e| reader_s3_failed(s3, e))?;
            let body = decompress(key, &body)?;
            items.extend(self.decode(input_type, key, &body)?);
        }
        Ok(items)
    }

    /// Items from an S3 Inventory `manifest.json`: every row of every CSV file
    /// it lists, named by the manifest's `fileSchema`.
    async fn read_inventory_manifest(
        &self,
        s3: &S3Caller<'_>,
        manifest: &[u8],
    ) -> Result<Vec<Value>, StateError> {
        let manifest: Value = serde_json::from_slice(manifest)
            .map_err(|e| reader_failed(format!("failed to parse the manifest as JSON: {e}")))?;
        if let Some(format) = manifest["fileFormat"].as_str().filter(|f| *f != "CSV") {
            return Err(reader_failed(format!(
                "S3 Inventory manifest fileFormat {format} is not supported"
            )));
        }
        let headers: Vec<String> = manifest["fileSchema"]
            .as_str()
            .ok_or_else(|| reader_failed("S3 Inventory manifest has no fileSchema"))?
            .split(',')
            .map(|column| column.trim().to_string())
            .collect();
        let destination = manifest["destinationBucket"]
            .as_str()
            .ok_or_else(|| reader_failed("S3 Inventory manifest has no destinationBucket"))?;
        let bucket = fakecloud_aws::arn::arn_resource(destination, "s3")
            .map_or(destination, |rest| rest.trim_start_matches(':'));
        let files = manifest["files"]
            .as_array()
            .ok_or_else(|| reader_failed("S3 Inventory manifest has no files"))?;

        let mut items = Vec::new();
        for file in files {
            let key = file["key"]
                .as_str()
                .ok_or_else(|| reader_failed("S3 Inventory manifest file has no key"))?;
            let body = s3
                .get_object(bucket, key, None)
                .await
                .map_err(|e| reader_s3_failed(s3, e))?;
            let raw = decompress(key, &body)?;
            let text = utf8(key, &raw)?;
            for row in parse_csv(text, self.csv_delimiter()?) {
                items.push(csv_item(&headers, row));
            }
        }
        Ok(items)
    }

    /// Decode one object's body as `input_type`.
    fn decode(&self, input_type: &str, key: &str, body: &[u8]) -> Result<Vec<Value>, StateError> {
        match input_type {
            "JSON" => self.decode_json(key, body),
            "JSONL" => decode_json_lines(key, body),
            "CSV" => self.decode_csv(key, body),
            "PARQUET" => crate::parquet_items::read(key, body).map_err(reader_failed),
            other => Err(reader_failed(format!(
                "ItemReader InputType {other} is not supported"
            ))),
        }
    }

    fn decode_json(&self, key: &str, body: &[u8]) -> Result<Vec<Value>, StateError> {
        let mut parsed: Value = serde_json::from_slice(body)
            .map_err(|e| reader_failed(format!("failed to parse {key} as JSON: {e}")))?;
        if let Some(pointer) = self.config["ItemsPointer"].as_str() {
            parsed = parsed.pointer(pointer).cloned().ok_or_else(|| {
                reader_failed(format!("ItemsPointer {pointer} selects nothing in {key}"))
            })?;
        }
        // An object iterates as its entries; a scalar isn't iterable.
        match parsed {
            Value::Array(items) => Ok(items),
            Value::Object(entries) => Ok(entries
                .into_iter()
                .map(|(key, value)| json!({"Key": key, "Value": value}))
                .collect()),
            _ => Err(reader_failed("Attempting to map over non-iterable node.")),
        }
    }

    fn csv_delimiter(&self) -> Result<char, StateError> {
        match self.config["CSVDelimiter"].as_str().unwrap_or("COMMA") {
            "COMMA" => Ok(','),
            "PIPE" => Ok('|'),
            "SEMICOLON" => Ok(';'),
            "SPACE" => Ok(' '),
            "TAB" => Ok('\t'),
            other => Err(reader_failed(format!(
                "ItemReader CSVDelimiter {other} is not supported"
            ))),
        }
    }

    fn decode_csv(&self, key: &str, body: &[u8]) -> Result<Vec<Value>, StateError> {
        let delimiter = self.csv_delimiter()?;
        let text = utf8(key, body)?;
        let mut rows = parse_csv(text, delimiter).into_iter();
        let headers: Vec<String> = match self.config["CSVHeaderLocation"]
            .as_str()
            .unwrap_or("FIRST_ROW")
        {
            "FIRST_ROW" => rows.next().unwrap_or_default(),
            "GIVEN" => self.config["CSVHeaders"]
                .as_array()
                .map(|given| {
                    given
                        .iter()
                        .filter_map(|h| h.as_str().map(String::from))
                        .collect()
                })
                .ok_or_else(|| reader_failed("ItemReader CSVHeaders is required with GIVEN"))?,
            other => {
                return Err(reader_failed(format!(
                    "ItemReader CSVHeaderLocation {other} is not supported"
                )))
            }
        };
        Ok(rows.map(|row| csv_item(&headers, row)).collect())
    }
}

/// A ListObjectsV2 entry as the item a Map iteration receives. Step Functions
/// spells these `Etag` and an epoch-seconds `LastModified`, unlike the SDK.
fn listed_item(object: &Value) -> Value {
    let last_modified = object["LastModified"]
        .as_str()
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.timestamp() as f64);
    json!({
        "Etag": object["ETag"],
        "Key": object["Key"],
        "LastModified": last_modified,
        "Size": object["Size"],
        "StorageClass": object["StorageClass"],
    })
}

fn decode_json_lines(key: &str, body: &[u8]) -> Result<Vec<Value>, StateError> {
    utf8(key, body)?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|e| reader_failed(format!("failed to parse a line of {key}: {e}")))
        })
        .collect()
}

fn utf8<'b>(key: &str, body: &'b [u8]) -> Result<&'b str, StateError> {
    std::str::from_utf8(body).map_err(|_| reader_failed(format!("{key} is not UTF-8 text")))
}

/// Input files may be GZIP or ZSTD compressed, recognized by their magic bytes.
fn decompress(key: &str, body: &[u8]) -> Result<Vec<u8>, StateError> {
    let failed = |e: std::io::Error| reader_failed(format!("failed to decompress {key}: {e}"));
    if body.starts_with(&[0x1f, 0x8b]) {
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(body)
            .read_to_end(&mut out)
            .map_err(failed)?;
        Ok(out)
    } else if body.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        zstd::decode_all(body).map_err(failed)
    } else {
        Ok(body.to_vec())
    }
}

/// A row as an item. A short row gets empty strings for its missing columns;
/// the extra fields of a long row are skipped.
fn csv_item(headers: &[String], row: Vec<String>) -> Value {
    let values = row.into_iter().chain(std::iter::repeat(String::new()));
    Value::Object(
        headers
            .iter()
            .cloned()
            .zip(values.map(Value::String))
            .collect(),
    )
}

/// Rows of a delimited file under the Step Functions rules:
///
/// - A field opening with `"` may hold delimiters and line breaks until its
///   closing `"`, and text after that closing quote joins the field: recorded,
///   `""` is an empty field and `""lead` is `lead`.
/// - `""` is a literal `"` inside quotes or in the middle of unquoted text.
/// - `\` escapes `\`, `"` and the delimiter; before anything else it is removed.
/// - Records end in LF or CRLF. Blank lines are not records.
fn parse_csv(text: &str, delimiter: char) -> Vec<Vec<String>> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    // Whether the current record has anything in it, even an empty field.
    let mut in_record = false;
    let mut at_field_start = true;
    let mut chars = text.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                in_record = true;
                at_field_start = false;
                if let Some(next) = chars
                    .peek()
                    .copied()
                    .filter(|next| *next == '\\' || *next == '"' || *next == delimiter)
                {
                    chars.next();
                    field.push(next);
                }
            }
            '"' if quoted => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    quoted = false;
                }
            }
            '"' if at_field_start => {
                in_record = true;
                at_field_start = false;
                quoted = true;
            }
            '"' => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                }
                field.push('"');
            }
            other if quoted => field.push(other),
            '\r' if chars.peek() == Some(&'\n') => {}
            '\n' => {
                if in_record {
                    row.push(std::mem::take(&mut field));
                    rows.push(std::mem::take(&mut row));
                }
                in_record = false;
                at_field_start = true;
            }
            other if other == delimiter => {
                row.push(std::mem::take(&mut field));
                in_record = true;
                at_field_start = true;
            }
            other => {
                field.push(other);
                in_record = true;
                at_field_start = false;
            }
        }
    }
    if in_record {
        row.push(field);
        rows.push(row);
    }
    rows
}

/// How a Map iteration ended.
#[derive(Clone)]
pub(crate) enum IterationOutcome {
    Succeeded(Value),
    Failed(String, String),
    /// Never started, because the map run had already failed.
    Pending,
}

/// One Map iteration, as a `ResultWriter` reports it.
pub(crate) struct IterationRecord {
    pub(crate) input: Value,
    pub(crate) started: DateTime<Utc>,
    pub(crate) stopped: DateTime<Utc>,
    pub(crate) outcome: IterationOutcome,
}

/// The map run a `ResultWriter` reports on.
pub(crate) struct MapRunIdentity<'a> {
    pub(crate) map_run_arn: &'a str,
}

impl MapRunIdentity<'_> {
    /// The label closing a map run ARN.
    fn label(&self) -> &str {
        self.map_run_arn
            .rsplit(':')
            .next()
            .unwrap_or(self.map_run_arn)
    }

    /// `(scope, <sm>/<segment>)` of `<scope>:mapRun:<sm>/<segment>:<label>`.
    fn run_parts(&self) -> Option<(&str, &str)> {
        let (scope, run) = self.map_run_arn.split_once(":mapRun:")?;
        Some((scope, run.rsplit_once(':').map_or(run, |(path, _)| path)))
    }

    /// `<scope>:execution:<sm>/<segment>`, which a child's name completes.
    fn child_arn_prefix(&self) -> String {
        self.run_parts().map_or_else(
            || self.map_run_arn.to_string(),
            |(scope, path)| format!("{scope}:execution:{path}"),
        )
    }

    /// `<scope>:stateMachine:<sm>/<segment>`, the state machine of its children.
    fn child_state_machine_arn(&self) -> String {
        self.run_parts().map_or_else(
            || self.map_run_arn.to_string(),
            |(scope, path)| format!("{scope}:stateMachine:{path}"),
        )
    }
}

fn timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn json_text(value: &Value) -> String {
    serde_json::to_string(value).expect("serde_json::Value serialization is infallible")
}

/// The record of one iteration: the shape `DescribeExecution` gives a child
/// execution of the map run.
fn iteration_record(run: &MapRunIdentity<'_>, record: &IterationRecord) -> Value {
    // An iteration that never started has no execution to describe.
    if matches!(record.outcome, IterationOutcome::Pending) {
        return json!({
            "Input": json_text(&record.input),
            "InputDetails": {"Included": true},
            "Status": "PENDING",
        });
    }
    let name = uuid::Uuid::new_v4().to_string();
    let mut out = json!({
        "ExecutionArn": format!("{}:{name}", run.child_arn_prefix()),
        "Input": json_text(&record.input),
        "InputDetails": {"Included": true},
        "Name": name,
        "StateMachineArn": run.child_state_machine_arn(),
    });
    match &record.outcome {
        IterationOutcome::Succeeded(output) => {
            out["StartDate"] = json!(timestamp(record.started));
            out["StopDate"] = json!(timestamp(record.stopped));
            out["Status"] = json!("SUCCEEDED");
            out["Output"] = json!(json_text(output));
            out["OutputDetails"] = json!({"Included": true});
            out["RedriveCount"] = json!(0);
            out["RedriveStatus"] = json!("NOT_REDRIVABLE");
            out["RedriveStatusReason"] = json!("Execution is SUCCEEDED and cannot be redriven");
        }
        IterationOutcome::Failed(error, cause) => {
            out["StartDate"] = json!(timestamp(record.started));
            out["StopDate"] = json!(timestamp(record.stopped));
            out["Status"] = json!("FAILED");
            out["Error"] = json!(error);
            out["Cause"] = json!(cause);
            out["OutputDetails"] = json!({"Included": true});
            out["RedriveCount"] = json!(0);
            out["RedriveStatus"] = json!("REDRIVABLE_BY_MAP_RUN");
        }
        IterationOutcome::Pending => {}
    }
    out
}

/// The types a map run's files are stored with, as recorded.
const RESULT_FILE_CONTENT_TYPE: &str = "binary/octet-stream";
const MANIFEST_CONTENT_TYPE: &str = "application/octet-stream";

/// The `WriterConfig` settings, once checked.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriterConfig<'a> {
    transformation: &'a str,
    jsonl: bool,
}

impl<'a> WriterConfig<'a> {
    /// Refuse settings that do not exist, before any iteration runs.
    pub(crate) fn parse(result_writer: &'a Value) -> Result<Self, StateError> {
        let config = &result_writer["WriterConfig"];
        let jsonl = match config["OutputType"].as_str().unwrap_or("JSON") {
            "JSON" => false,
            "JSONL" => true,
            other => {
                return Err(writer_failed(format!(
                    "ResultWriter OutputType {other} is not supported"
                )))
            }
        };
        let transformation = config["Transformation"].as_str().unwrap_or("NONE");
        if !matches!(transformation, "NONE" | "COMPACT" | "FLATTEN") {
            return Err(writer_failed(format!(
                "ResultWriter Transformation {transformation} is not supported"
            )));
        }
        Ok(Self {
            transformation,
            jsonl,
        })
    }

    /// Records under the `Transformation`. A failed iteration keeps its full
    /// record whatever the setting.
    fn transform(&self, run: &MapRunIdentity<'_>, records: &[&IterationRecord]) -> Vec<Value> {
        let mut out = Vec::with_capacity(records.len());
        for record in records {
            match (&record.outcome, self.transformation) {
                (IterationOutcome::Succeeded(output), "COMPACT") => out.push(output.clone()),
                (IterationOutcome::Succeeded(Value::Array(items)), "FLATTEN") => {
                    out.extend(items.iter().cloned())
                }
                (IterationOutcome::Succeeded(output), "FLATTEN") => out.push(output.clone()),
                _ => out.push(iteration_record(run, record)),
            }
        }
        out
    }

    fn encode(&self, items: Vec<Value>) -> Vec<u8> {
        if !self.jsonl {
            return json_text(&Value::Array(items)).into_bytes();
        }
        let mut body = String::new();
        for item in &items {
            body.push_str(&json_text(item));
            body.push('\n');
        }
        body.into_bytes()
    }
}

/// The Map state's result for a `ResultWriter` with only a `WriterConfig`: the
/// iterations under its `Transformation`, nothing exported.
pub(crate) fn format_state_output(
    config: WriterConfig<'_>,
    run: &MapRunIdentity<'_>,
    records: &[IterationRecord],
) -> Result<Value, StateError> {
    if config.jsonl {
        return Err(writer_failed(
            "ResultWriter OutputType JSONL requires an S3 destination",
        ));
    }
    let records: Vec<&IterationRecord> = records.iter().collect();
    Ok(Value::Array(config.transform(run, &records)))
}

/// Write the map run's result files and manifest, returning the Map state's
/// result. Iterations are grouped by outcome into `<STATUS>_0.json` under
/// `<prefix>/<map run label>/`, indexed by `manifest.json`.
pub(crate) async fn write_results(
    s3: &S3Caller<'_>,
    bucket: &str,
    prefix: &str,
    config: WriterConfig<'_>,
    run: &MapRunIdentity<'_>,
    records: &[IterationRecord],
) -> Result<Value, StateError> {
    let trimmed = prefix.trim_end_matches('/');
    let base = if trimmed.is_empty() {
        run.label().to_string()
    } else {
        format!("{trimmed}/{}", run.label())
    };

    let mut files = Map::new();
    for status in ["SUCCEEDED", "FAILED", "PENDING"] {
        let group: Vec<&IterationRecord> = records
            .iter()
            .filter(|r| match r.outcome {
                IterationOutcome::Succeeded(_) => status == "SUCCEEDED",
                IterationOutcome::Failed(..) => status == "FAILED",
                IterationOutcome::Pending => status == "PENDING",
            })
            .collect();
        let mut entries = Vec::new();
        if !group.is_empty() {
            let key = format!("{base}/{status}_0.json");
            let body = config.encode(config.transform(run, &group));
            let size = body.len();
            put(s3, bucket, &key, RESULT_FILE_CONTENT_TYPE, body).await?;
            entries.push(json!({"Key": key, "Size": size}));
        }
        files.insert(status.to_string(), Value::Array(entries));
    }

    let manifest_key = format!("{base}/manifest.json");
    let manifest = json!({
        "DestinationBucket": bucket,
        "MapRunArn": run.map_run_arn,
        "ResultFiles": files,
    });
    put(
        s3,
        bucket,
        &manifest_key,
        MANIFEST_CONTENT_TYPE,
        json_text(&manifest).into_bytes(),
    )
    .await?;

    Ok(json!({
        "MapRunArn": run.map_run_arn,
        "ResultWriterDetails": {"Bucket": bucket, "Key": manifest_key},
    }))
}

async fn put(
    s3: &S3Caller<'_>,
    bucket: &str,
    key: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Result<(), StateError> {
    let text = String::from_utf8(body).expect("result files are JSON text");
    s3.put_object(bucket, key, content_type, text)
        .await
        .map_err(|err| {
            let (_, detail) = crate::sdk_protocol::task_error(s3.service(), &err);
            writer_failed(detail)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(text: &str, delimiter: char) -> Vec<Vec<String>> {
        parse_csv(text, delimiter)
    }

    #[test]
    fn csv_handles_quotes_delimiters_crlf_and_blank_lines() {
        assert_eq!(
            rows(
                "a,b\r\n\"x,1\",\"say \"\"hi\"\"\"\n\n\"multi\nline\",\n",
                ','
            ),
            vec![
                vec!["a", "b"],
                vec!["x,1", "say \"hi\""],
                vec!["multi\nline", ""],
            ]
        );
    }

    #[test]
    fn csv_parses_the_documented_example() {
        let text = "abc,123,\"This string contains commas, a double quote (\"\"), and a newline (\n)\",{\"\"MyKey\"\":\"\"MyValue\"\"},\"[1,2,3]\"\n";
        assert_eq!(
            rows(text, ','),
            vec![vec![
                "abc",
                "123",
                "This string contains commas, a double quote (\"), and a newline (\n)",
                "{\"MyKey\":\"MyValue\"}",
                "[1,2,3]",
            ]]
        );
    }

    #[test]
    fn csv_empty_quoted_fields_and_records() {
        assert_eq!(rows("a,\"\",b\n", ','), vec![vec!["a", "", "b"]]);
        // A row of just `""` is a record with one empty field; a blank line isn't.
        assert_eq!(rows("\"\"\n\n\"\"", ','), vec![vec![""], vec![""]]);
        assert_eq!(rows("\"\"\r\nx\n", ','), vec![vec![""], vec!["x"]]);
    }

    /// The recorded file, row for row.
    #[test]
    fn csv_matches_the_recording() {
        assert_eq!(
            rows(
                "a,b\n\"\"\"x,y\"\"\",1\n\"plain\",2\n\"\",3\n{\"\"K\"\":\"\"V\"\"},4\n\"\"lead,5\n",
                ','
            ),
            vec![
                vec!["a", "b"],
                vec!["\"x,y\"", "1"],
                vec!["plain", "2"],
                vec!["", "3"],
                vec!["{\"K\":\"V\"}", "4"],
                vec!["lead", "5"],
            ]
        );
    }

    #[test]
    fn csv_backslash_rules() {
        assert_eq!(
            rows("C:\\\\Program Files\\\\App.exe,1\n", ','),
            vec![vec!["C:\\Program Files\\App.exe", "1"]]
        );
        assert_eq!(rows("a\\,b,c\n", ','), vec![vec!["a,b", "c"]]);
        assert_eq!(rows("a\\|b|c\n", '|'), vec![vec!["a|b", "c"]]);
        // Before anything else the backslash is removed.
        assert_eq!(rows("a\\xb\n", ','), vec![vec!["axb"]]);
        assert_eq!(rows("\"a\\\"b\"\n", ','), vec![vec!["a\"b"]]);
    }

    #[test]
    fn csv_without_a_final_newline_keeps_the_last_row() {
        assert_eq!(rows("a|b\n1|2", '|'), vec![vec!["a", "b"], vec!["1", "2"]]);
    }

    #[test]
    fn csv_strips_a_byte_order_mark() {
        assert_eq!(rows("\u{feff}a\n", ','), vec![vec!["a"]]);
    }

    #[test]
    fn csv_item_fills_short_rows_and_skips_extra_fields() {
        let headers = vec!["a".to_string(), "b".to_string()];
        assert_eq!(
            csv_item(&headers, vec!["1".to_string()]),
            json!({"a": "1", "b": ""})
        );
        assert_eq!(
            csv_item(
                &headers,
                vec!["1".to_string(), "2".to_string(), "3".to_string()]
            ),
            json!({"a": "1", "b": "2"})
        );
    }

    #[test]
    fn plain_bodies_pass_through_decompression() {
        assert_eq!(decompress("k", b"plain").unwrap(), b"plain");
    }

    #[test]
    fn compressed_bodies_are_recognized_by_their_magic_bytes() {
        use std::io::Write;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(b"gzipped").unwrap();
        assert_eq!(decompress("k", &gz.finish().unwrap()).unwrap(), b"gzipped");
        let zst = zstd::encode_all(&b"zstded"[..], 0).unwrap();
        assert_eq!(decompress("k", &zst).unwrap(), b"zstded");
        assert_eq!(
            decompress("k", &[0x1f, 0x8b, 0]).unwrap_err().0,
            "States.ItemReaderFailed"
        );
    }

    fn run() -> MapRunIdentity<'static> {
        MapRunIdentity {
            map_run_arn: "arn:aws:states:us-east-1:1:mapRun:sm/seg:label",
        }
    }

    fn record(outcome: IterationOutcome) -> IterationRecord {
        IterationRecord {
            input: json!(1),
            started: Utc::now(),
            stopped: Utc::now(),
            outcome,
        }
    }

    #[test]
    fn child_arn_follows_the_map_run() {
        assert_eq!(
            run().child_arn_prefix(),
            "arn:aws:states:us-east-1:1:execution:sm/seg"
        );
    }

    #[test]
    fn writer_config_refuses_unknown_settings() {
        assert!(WriterConfig::parse(&json!({})).is_ok());
        for bad in [
            json!({"WriterConfig": {"OutputType": "XML"}}),
            json!({"WriterConfig": {"Transformation": "ROTATE"}}),
        ] {
            assert_eq!(
                WriterConfig::parse(&bad).err().unwrap().0,
                "States.ResultWriterFailed"
            );
        }
    }

    #[test]
    fn transformations_shape_successes_and_keep_failures_whole() {
        let records = [
            record(IterationOutcome::Succeeded(json!([1, 2]))),
            record(IterationOutcome::Succeeded(json!(3))),
            record(IterationOutcome::Failed(
                "Bad".to_string(),
                "why".to_string(),
            )),
        ];
        let writer = json!({"WriterConfig": {"Transformation": "COMPACT"}});
        let compact = WriterConfig::parse(&writer).unwrap();
        let out = format_state_output(compact, &run(), &records).unwrap();
        assert_eq!(out[0], json!([1, 2]));
        assert_eq!(out[1], json!(3));
        assert_eq!(out[2]["Status"], "FAILED");

        let writer = json!({"WriterConfig": {"Transformation": "FLATTEN"}});
        let flatten = WriterConfig::parse(&writer).unwrap();
        let out = format_state_output(flatten, &run(), &records).unwrap();
        assert_eq!(out[0], json!(1));
        assert_eq!(out[1], json!(2));
        assert_eq!(out[2], json!(3));
        assert_eq!(out[3]["Error"], "Bad");

        let writer = json!({});
        let none = WriterConfig::parse(&writer).unwrap();
        let out = format_state_output(none, &run(), &records).unwrap();
        assert_eq!(out[0]["Output"], "[1,2]");
        assert_eq!(
            out[0]["StateMachineArn"],
            "arn:aws:states:us-east-1:1:stateMachine:sm/seg"
        );
        assert_eq!(out[0]["RedriveStatus"], "NOT_REDRIVABLE");
        assert_eq!(out[2]["RedriveStatus"], "REDRIVABLE_BY_MAP_RUN");
        assert_eq!(out[2]["OutputDetails"]["Included"], true);
        assert!(out[2].get("Output").is_none());
    }

    #[test]
    fn jsonl_writes_one_line_per_item_and_needs_a_destination() {
        let writer = json!({"WriterConfig": {"OutputType": "JSONL", "Transformation": "COMPACT"}});
        let config = WriterConfig::parse(&writer).unwrap();
        assert_eq!(
            config.encode(vec![json!(1), json!({"a": 2})]),
            b"1\n{\"a\":2}\n"
        );
        let err = format_state_output(config, &run(), &[]).err().unwrap();
        assert_eq!(err.0, "States.ResultWriterFailed");
    }
}
