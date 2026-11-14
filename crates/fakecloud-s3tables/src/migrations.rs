// Table migrations: an AWS Glue Data Catalog table (an external Hive table
// over Parquet objects in a general purpose bucket) migrated into a new
// table of a table bucket. `include!`d by `service.rs`.
//
// A migration runs synchronously to the end of what it can do on its own:
//
// * planning resolves the Glue table (`resourceNotFound` / `sourceNotSupported`
//   when it is missing or not a Parquet Hive table), lists its objects (the
//   partitions' locations for a partitioned table), creates the destination
//   table with the inferred or requested schema, checks every column and
//   object for compatibility and writes the table and object compatibility
//   reports (blocking findings fail it with the report's code);
// * with `reviewMigrationPlan` it then waits in `pendingReview` until
//   `UpdateTableMigration` queues or cancels it;
// * otherwise it migrates: the planned objects' data is committed to the
//   table (a new Iceberg metadata version) and the object migration report
//   is written, ending in `succeeded`.
//
// A sync re-plans against the source's current objects: objects added since
// the previous migration are migrated, deleted ones removed, and an object
// overwritten since it was migrated fails the sync. `CompleteTableMigration`
// makes the table writable and ends syncing.

/// Migration states in which a migration is still running.
const MIGRATION_IN_PROGRESS: &[&str] = &[
    "new",
    "planning",
    "pendingReview",
    "queued",
    "migrating",
    "cancelling",
];

/// Whether `table_arn` has a migration that has not been completed, which
/// keeps the table read-only.
fn migration_blocks_writes(
    migrations: &std::collections::BTreeMap<String, MigrationRecord>,
    completed: &std::collections::BTreeSet<String>,
    table_arn: &str,
) -> bool {
    !completed.contains(table_arn)
        && migrations
            .values()
            .any(|m| m.table_arn.as_deref() == Some(table_arn))
}

/// The most recent migration into `table_arn`.
fn latest_migration_for_table<'a>(
    migrations: &'a std::collections::BTreeMap<String, MigrationRecord>,
    table_arn: &str,
) -> Option<&'a MigrationRecord> {
    migrations
        .values()
        .filter(|m| m.table_arn.as_deref() == Some(table_arn))
        .max_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then(a.migration_id.cmp(&b.migration_id))
        })
}

fn check_len(v: Option<&str>, member: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    match v {
        Some(s) if !(min..=max).contains(&s.chars().count()) => Err(bad_request(format!(
            "{member} must be between {min} and {max} characters long."
        ))),
        _ => Ok(()),
    }
}

fn check_enum_value(v: &Value, member: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    match v {
        Value::Null => Ok(()),
        Value::String(s) if allowed.contains(&s.as_str()) => Ok(()),
        other => Err(bad_request(format!(
            "Invalid value {other} for {member}. Valid values: {}.",
            allowed.join(", ")
        ))),
    }
}

fn check_int(v: &Value, member: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    match v {
        Value::Null => Ok(()),
        v if v.as_i64().is_some_and(|n| (min..=max).contains(&n)) => Ok(()),
        other => Err(bad_request(format!(
            "{member} must be between {min} and {max}, got {other}."
        ))),
    }
}

/// `MigrationOutputConfiguration` checks shared by start and sync.
fn validate_migration_output(output: &Value) -> Result<(), AwsServiceError> {
    let bucket = output
        .get("bucket")
        .and_then(Value::as_str)
        .ok_or_else(|| bad_request("migrationOutput.bucket is required."))?;
    check_len(Some(bucket), "migrationOutput.bucket", 12, 1024)?;
    for report in [
        "tableCompatibilityReport",
        "objectCompatibilityReport",
        "objectMigrationReport",
    ] {
        check_len(
            output["prefixes"][report].as_str(),
            &format!("migrationOutput.prefixes.{report}"),
            0,
            512,
        )?;
    }
    Ok(())
}

fn validate_role_arn(role: Option<&str>) -> Result<String, AwsServiceError> {
    let role = role.ok_or_else(|| bad_request("roleARN is required."))?;
    check_len(Some(role), "roleARN", 20, 2048)?;
    Ok(role.to_string())
}

/// `DataMigrationSettings` with every default applied, as
/// `DataMigrationOverview` reports it.
fn data_migration_overview(settings: &Value) -> Result<Value, AwsServiceError> {
    let compaction = &settings["maintenanceSettings"]["icebergCompaction"];
    check_int(&compaction["targetFileSizeMB"], "targetFileSizeMB", 64, 512)?;
    check_enum_value(&compaction["strategy"], "strategy", &["binpack"])?;
    let snapshots = &settings["maintenanceSettings"]["icebergSnapshotManagement"];
    check_int(
        &snapshots["minSnapshotsToKeep"],
        "minSnapshotsToKeep",
        3,
        i64::from(i32::MAX),
    )?;
    check_int(
        &snapshots["maxSnapshotAgeHours"],
        "maxSnapshotAgeHours",
        1,
        i64::from(i32::MAX),
    )?;
    let policies = &settings["migrationPolicies"];
    check_enum_value(
        &policies["unmatchedSourceFieldsPolicy"],
        "unmatchedSourceFieldsPolicy",
        &["fail", "drop"],
    )?;
    check_enum_value(
        &policies["incompatibleValuesPolicy"],
        "incompatibleValuesPolicy",
        &["fail", "setNull"],
    )?;
    let or = |v: &Value, d: Value| if v.is_null() { d } else { v.clone() };
    Ok(json!({
        "maintenanceSettings": {
            "icebergCompaction": {
                "targetFileSizeMB": or(&compaction["targetFileSizeMB"], json!(512)),
                "strategy": or(&compaction["strategy"], json!("binpack")),
            },
            "icebergSnapshotManagement": {
                "minSnapshotsToKeep": or(&snapshots["minSnapshotsToKeep"], json!(3)),
                "maxSnapshotAgeHours": or(&snapshots["maxSnapshotAgeHours"], json!(120)),
            },
        },
        "migrationPolicies": {
            "unmatchedSourceFieldsPolicy": or(&policies["unmatchedSourceFieldsPolicy"], json!("fail")),
            "incompatibleValuesPolicy": or(&policies["incompatibleValuesPolicy"], json!("fail")),
        },
    }))
}

/// `DestinationTable` checks: names, encryption, storage class, data file
/// properties, and a sort order that names columns of the given schema.
fn validate_destination(dest: &Value) -> Result<(), AwsServiceError> {
    for (member, max) in [("tableBucketARN", 2048), ("namespace", 255), ("name", 255)] {
        let v = dest
            .get(member)
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request(format!("destinationTable.{member} is required.")))?;
        check_len(Some(v), &format!("destinationTable.{member}"), 1, max)?;
    }
    if let Some(enc) = dest.get("encryptionConfiguration").filter(|v| !v.is_null()) {
        if enc["sseAlgorithm"].is_null() {
            return Err(bad_request(
                "encryptionConfiguration.sseAlgorithm is required.",
            ));
        }
        check_enum_value(&enc["sseAlgorithm"], "sseAlgorithm", &["AES256", "aws:kms"])?;
        if enc["sseAlgorithm"] == "aws:kms" && enc["kmsKeyArn"].is_null() {
            return Err(bad_request(
                "encryptionConfiguration.kmsKeyArn is required for aws:kms.",
            ));
        }
    }
    if let Some(sc) = dest
        .get("storageClassConfiguration")
        .filter(|v| !v.is_null())
    {
        if sc["storageClass"].is_null() {
            return Err(bad_request(
                "storageClassConfiguration.storageClass is required.",
            ));
        }
        check_enum_value(
            &sc["storageClass"],
            "storageClass",
            &["STANDARD", "INTELLIGENT_TIERING"],
        )?;
    }
    let props = &dest["properties"];
    check_enum_value(&props["writeFormat"], "writeFormat", &["parquet"])?;
    check_int(&props["icebergFormatVersion"], "icebergFormatVersion", 2, 3)?;
    check_enum_value(
        &props["compressionCodec"],
        "compressionCodec",
        &["zstd", "snappy", "gzip", "uncompressed"],
    )?;
    let schema = dest.get("schema").filter(|v| !v.is_null());
    if let Some(schema) = schema {
        check_enum_value(&schema["type"], "schema.type", &["struct"])?;
        let fields = schema["fields"]
            .as_array()
            .ok_or_else(|| bad_request("schema.fields is required."))?;
        for f in fields {
            if f["id"].as_i64().is_none() || f["name"].as_str().is_none() || f["type"].is_null() {
                return Err(bad_request(
                    "Each schema field needs an id, a name, a type and required.",
                ));
            }
        }
    }
    if let Some(order) = dest.get("icebergSortOrder").filter(|v| !v.is_null()) {
        let schema = schema.ok_or_else(|| {
            bad_request("icebergSortOrder requires a schema in destinationTable.")
        })?;
        let ids: Vec<i64> = schema["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|f| f["id"].as_i64())
            .collect();
        let fields = order["fields"]
            .as_array()
            .ok_or_else(|| bad_request("icebergSortOrder.fields is required."))?;
        if fields.len() > 5 {
            return Err(bad_request("A sort order can have at most 5 fields."));
        }
        for f in fields {
            let source = f["source-id"].as_i64().unwrap_or(-1);
            if !ids.contains(&source) {
                return Err(bad_request(format!(
                    "Sort field source-id {source} doesn't name a field of the schema."
                )));
            }
            check_enum_value(&f["direction"], "direction", &["asc", "desc"])?;
            check_enum_value(
                &f["null-order"],
                "null-order",
                &["nulls-first", "nulls-last"],
            )?;
        }
    }
    Ok(())
}

/// The bucket name of a general purpose bucket ARN, or `None` for an ARN
/// that names something else (an access point, a directory bucket).
fn report_bucket_name(arn: &str) -> Option<String> {
    let resource = arn.splitn(6, ':').nth(5)?;
    let plain = !resource.is_empty()
        && !resource.contains('/')
        && !resource.ends_with("--x-s3")
        && arn.split(':').nth(2) == Some("s3");
    plain.then(|| resource.to_string())
}

/// The bucket and key prefix of an `s3://bucket/prefix` location.
fn split_s3_location(location: &str) -> Option<(String, String)> {
    let rest = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("s3a://"))
        .or_else(|| location.strip_prefix("s3n://"))?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return None;
    }
    let prefix = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    };
    Some((bucket.to_string(), prefix))
}

/// The Iceberg type of a Hive column type, or `None` for a type the
/// migration can't map (a complex type).
fn iceberg_type(hive: &str) -> Option<String> {
    let t = hive.trim().to_ascii_lowercase();
    let base = t.split('(').next().unwrap_or(&t).trim();
    let mapped = match base {
        "string" | "varchar" | "char" => "string".to_string(),
        "tinyint" | "smallint" | "int" | "integer" => "int".to_string(),
        "bigint" => "long".to_string(),
        "float" => "float".to_string(),
        "double" => "double".to_string(),
        "boolean" => "boolean".to_string(),
        "date" => "date".to_string(),
        "timestamp" => "timestamp".to_string(),
        "binary" => "binary".to_string(),
        "decimal" => {
            let args = t
                .split_once('(')
                .map(|(_, a)| a.trim_end_matches(')').replace(' ', ""))
                .unwrap_or_else(|| "10,0".to_string());
            format!("decimal({args})")
        }
        _ => return None,
    };
    Some(mapped)
}

/// The Glue table a migration reads, reduced to what the migration needs.
struct GlueSource {
    /// Data and partition columns, in order: `(name, hive type)`.
    columns: Vec<(String, String)>,
    partition_keys: Vec<String>,
    bucket: String,
    /// Key prefixes holding the table's data files: the partitions'
    /// locations for a partitioned table, else the table location.
    data_prefixes: Vec<String>,
    /// The table location's prefix (every object under it is scanned).
    table_prefix: String,
}

/// A planning failure: `(MigrationFailureCode, detail)`.
type PlanFailure = (&'static str, Option<String>);

/// A source object as planning sees it.
struct ScannedObject {
    key: String,
    size: u64,
    etag: String,
    /// Why the object isn't migrated, if it isn't; `blocking` findings fail
    /// planning.
    finding: Option<&'static str>,
    blocking: bool,
}

impl S3TablesService {
    fn resolve_glue_source(
        &self,
        account: &str,
        region: &str,
        glue_table: &Value,
    ) -> Result<GlueSource, PlanFailure> {
        let db = glue_table["databaseName"].as_str().unwrap_or_default();
        let name = glue_table["tableName"].as_str().unwrap_or_default();
        let qualified = format!("{db}.{name}");
        let missing = || ("resourceNotFound", Some(qualified.clone()));
        let glue = self.glue_state.as_ref().ok_or_else(missing)?;
        let guard = glue.read();
        let table = guard
            .get(account)
            .and_then(|s| s.dbs_in(region))
            .and_then(|dbs| dbs.get(db))
            .and_then(|d| d.tables.get(name))
            .ok_or_else(missing)?;
        let unsupported = |why: &str| ("sourceNotSupported", Some(why.to_string()));
        let table_type = table.table_type.as_deref().unwrap_or_default();
        if table_type.eq_ignore_ascii_case("VIRTUAL_VIEW") {
            return Err(unsupported("VIEW"));
        }
        let param = |k: &str| {
            table
                .parameters
                .iter()
                .find(|(pk, _)| pk.eq_ignore_ascii_case(k))
                .map(|(_, v)| v.to_ascii_lowercase())
        };
        if matches!(
            param("table_type").as_deref(),
            Some("iceberg" | "delta" | "hudi")
        ) {
            return Err(unsupported("OPEN_TABLE_FORMAT"));
        }
        if param("transactional").as_deref() == Some("true") {
            return Err(unsupported("TRANSACTIONAL_TABLE"));
        }
        if !table_type.eq_ignore_ascii_case("EXTERNAL_TABLE") {
            return Err(unsupported("NOT_EXTERNAL_TABLE"));
        }
        let sd = table
            .storage_descriptor
            .as_ref()
            .ok_or_else(|| unsupported("NO_STORAGE_DESCRIPTOR"))?;
        let parquet =
            |s: Option<&str>| s.is_some_and(|s| s.to_ascii_lowercase().contains("parquet"));
        if !parquet(sd.input_format.as_deref())
            && !parquet(
                sd.serde_info
                    .as_ref()
                    .and_then(|s| s.serialization_library.as_deref()),
            )
        {
            return Err(unsupported("NOT_PARQUET"));
        }
        let (bucket, table_prefix) = sd
            .location
            .as_deref()
            .and_then(split_s3_location)
            .ok_or_else(|| unsupported("LOCATION_NOT_IN_S3"))?;
        let mut data_prefixes = Vec::new();
        if table.partition_keys.is_empty() {
            data_prefixes.push(table_prefix.clone());
        } else {
            for p in table.partitions.values() {
                match p
                    .storage_descriptor
                    .as_ref()
                    .and_then(|s| s.location.as_deref())
                    .and_then(split_s3_location)
                {
                    Some((b, prefix)) if b == bucket => data_prefixes.push(prefix),
                    _ => return Err(unsupported("PARTITION_LOCATION_NOT_SUPPORTED")),
                }
            }
        }
        let mut columns: Vec<(String, String)> = sd
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.column_type.clone()))
            .collect();
        columns.extend(
            table
                .partition_keys
                .iter()
                .map(|c| (c.name.clone(), c.column_type.clone())),
        );
        Ok(GlueSource {
            columns,
            partition_keys: table
                .partition_keys
                .iter()
                .map(|c| c.name.clone())
                .collect(),
            bucket,
            data_prefixes,
            table_prefix,
        })
    }

    /// List (and read, to check the Parquet magic) every object under the
    /// source table's location.
    fn scan_source_objects(
        &self,
        account: &str,
        source: &GlueSource,
    ) -> Result<Vec<ScannedObject>, PlanFailure> {
        let missing = || ("resourceNotFound", Some(source.bucket.clone()));
        let s3 = self.s3_state.as_ref().ok_or_else(missing)?;
        let mut opened = Vec::new();
        {
            let guard = s3.read();
            let bucket = guard
                .get(account)
                .and_then(|a| a.buckets.get(&source.bucket))
                .ok_or_else(missing)?;
            for (key, obj) in bucket
                .objects
                .iter()
                .filter(|(k, _)| k.starts_with(&source.table_prefix))
            {
                if obj.is_delete_marker {
                    continue;
                }
                let reader =
                    fakecloud_s3::body_io::ObjectReader::open(&guard, account, &source.bucket, obj)
                        .ok();
                opened.push((key.clone(), obj.size, obj.etag.clone(), reader));
            }
        }
        let mut out = Vec::with_capacity(opened.len());
        for (key, size, etag, reader) in opened {
            let base = key.rsplit('/').next().unwrap_or(&key);
            let (finding, blocking) = if base.starts_with('_') || base.starts_with('.') {
                (Some("HIDDEN_FILE"), false)
            } else if key.ends_with('/') || size == 0 {
                (Some("EMPTY_OBJECT"), false)
            } else if !source
                .data_prefixes
                .iter()
                .any(|p| key.starts_with(p.as_str()))
            {
                (Some("OUTSIDE_PARTITIONS"), false)
            } else {
                let body = reader.and_then(|r| r.read_all().ok());
                let is_parquet = body.as_deref().is_some_and(|b| {
                    b.len() >= 8 && b.starts_with(b"PAR1") && b.ends_with(b"PAR1")
                });
                if is_parquet {
                    (None, false)
                } else {
                    (Some("NOT_PARQUET"), true)
                }
            };
            out.push(ScannedObject {
                key,
                size,
                etag,
                finding,
                blocking,
            });
        }
        Ok(out)
    }

    /// Write one report object to the migration's output bucket; returns its
    /// key and CRC64NVME checksum.
    fn write_report(
        &self,
        account: &str,
        bucket: &str,
        key: String,
        body: Vec<u8>,
    ) -> Result<(String, String), PlanFailure> {
        let s3 = self
            .s3_state
            .as_ref()
            .ok_or(("resourceNotFound", Some(bucket.to_string())))?;
        let checksum = fakecloud_s3::checksum::checksum(
            fakecloud_s3::checksum::ChecksumAlgorithm::Crc64Nvme,
            &body,
        );
        fakecloud_s3::delivery::put_object_via_store_with_encryption(
            s3,
            self.s3_store.as_deref(),
            account,
            bucket,
            &key,
            body,
            Some("application/json"),
            fakecloud_s3::delivery::DeliveredEncryption::AsDelivered,
        )
        .map_err(|_| ("resourceNotFound", Some(bucket.to_string())))?;
        Ok((key, checksum))
    }

    /// Write a report made of a manifest and one JSON-lines data file.
    fn write_manifest_report(
        &self,
        account: &str,
        bucket: &str,
        base: &str,
        rows: &[Value],
    ) -> Result<(String, String), PlanFailure> {
        let mut data = String::new();
        for row in rows {
            data.push_str(&row.to_string());
            data.push('\n');
        }
        let (data_key, data_checksum) = self.write_report(
            account,
            bucket,
            format!("{base}/data/part-00000.jsonl"),
            data.into_bytes(),
        )?;
        let manifest = json!({
            "files": [{ "key": data_key, "checksumCRC64NVME": data_checksum, "rows": rows.len() }],
        });
        self.write_report(
            account,
            bucket,
            format!("{base}/manifest.json"),
            manifest.to_string().into_bytes(),
        )
    }

    fn update_migration(&self, account: &str, id: &str, f: impl FnOnce(&mut MigrationRecord)) {
        let mut accounts = self.state.write();
        if let Some(m) = accounts
            .get_mut(account)
            .and_then(|s| s.migrations.get_mut(id))
        {
            f(m);
        }
    }

    fn fail_migration(&self, account: &str, id: &str, failure: PlanFailure) {
        self.update_migration(account, id, |m| {
            m.state = "failed".to_string();
            m.state_updated_at = Utc::now();
            m.failure_code = Some(failure.0.to_string());
            m.failure_detail = failure.1;
            if m.has_valid_plan.is_none() {
                m.has_valid_plan = Some(false);
            }
        });
    }

    /// The objects the table holds after the latest settled migration before
    /// `id` (empty for a first start).
    fn table_objects_before(
        &self,
        account: &str,
        table_arn: Option<&str>,
        id: &str,
    ) -> BTreeMap<String, String> {
        let Some(table_arn) = table_arn else {
            return BTreeMap::new();
        };
        let accounts = self.state.read();
        accounts
            .get(account)
            .and_then(|s| {
                s.migrations
                    .values()
                    .filter(|m| m.table_arn.as_deref() == Some(table_arn) && m.migration_id != id)
                    .filter(|m| !MIGRATION_IN_PROGRESS.contains(&m.state.as_str()))
                    .max_by(|a, b| a.created_at.cmp(&b.created_at))
                    .map(|m| m.migrated_objects.clone())
            })
            .unwrap_or_default()
    }

    /// Plan migration `id` and, unless it waits for review, run it.
    fn plan_migration(&self, account: &str, region: &str, id: &str) {
        let Some(m) = self
            .state
            .read()
            .get(account)
            .and_then(|s| s.migrations.get(id).cloned())
        else {
            return;
        };
        let report_bucket = m.migration_output["bucket"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        // The role must be one of the caller's own.
        let role_account = m.role_arn.split(':').nth(4).unwrap_or_default();
        if role_account != account {
            return self.fail_migration(account, id, ("rolePermissions", Some(m.role_arn.clone())));
        }
        let source = match self.resolve_glue_source(account, region, &m.source["glueTable"]) {
            Ok(s) => s,
            Err(f) => return self.fail_migration(account, id, f),
        };
        let objects = match self.scan_source_objects(account, &source) {
            Ok(o) => o,
            Err(f) => return self.fail_migration(account, id, f),
        };
        let Some(report_bucket_name) = report_bucket_name(&report_bucket) else {
            return self.fail_migration(
                account,
                id,
                ("reportBucketNotSupported", Some(report_bucket.clone())),
            );
        };
        let report_bucket_exists = self.s3_state.as_ref().is_some_and(|s3| {
            s3.read()
                .get(account)
                .is_some_and(|a| a.buckets.contains_key(&report_bucket_name))
        });

        // The destination schema: the requested one, or every source column
        // (optional) in order.
        let dest = &m.destination;
        let inferred: Vec<Value> = source
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, (name, ty))| {
                iceberg_type(ty)
                    .map(|t| json!({ "id": i + 1, "name": name, "type": t, "required": false }))
            })
            .collect();
        let requested = dest.get("schema").filter(|v| !v.is_null());
        let schema_fields: Vec<Value> = match requested {
            Some(s) => s["fields"].as_array().cloned().unwrap_or_default(),
            None => inferred,
        };
        let policy = m.data_migration["migrationPolicies"]["unmatchedSourceFieldsPolicy"]
            .as_str()
            .unwrap_or("fail")
            .to_string();
        let mut table_findings: BTreeMap<String, i64> = BTreeMap::new();
        let mut column_rows = Vec::new();
        for (name, hive) in &source.columns {
            let dest_field = schema_fields.iter().find(|f| f["name"] == name.as_str());
            let mapped = iceberg_type(hive);
            let (destination_type, finding) = match (dest_field, &mapped) {
                (_, None) => (Value::Null, Some("UNSUPPORTED_TYPE")),
                (Some(f), Some(_)) => (f["type"].clone(), None),
                (None, Some(_)) if source.partition_keys.contains(name) => {
                    (Value::Null, Some("MISSING_PARTITION_COLUMN"))
                }
                (None, Some(_)) if policy == "fail" => {
                    (Value::Null, Some("UNMATCHED_SOURCE_FIELD"))
                }
                (None, Some(_)) => (Value::Null, None),
            };
            if let Some(f) = finding {
                *table_findings.entry(f.to_string()).or_default() += 1;
            }
            let type_change = match (&mapped, destination_type.as_str()) {
                (Some(src), Some(dst)) if src == dst => "none",
                (Some(_), Some(_)) => "converted",
                _ if dest_field.is_none() && finding.is_none() => "dropped",
                _ => "none",
            };
            column_rows.push(json!({
                "column": name,
                "sourceType": hive,
                "destinationType": destination_type,
                "typeChange": type_change,
                "finding": finding,
            }));
        }
        for f in &schema_fields {
            let name = f["name"].as_str().unwrap_or_default();
            let in_source = source.columns.iter().any(|(n, _)| n == name);
            if !in_source && f["required"].as_bool() == Some(true) {
                *table_findings
                    .entry("REQUIRED_COLUMN_WITHOUT_SOURCE".to_string())
                    .or_default() += 1;
            }
        }

        // Objects: what this migration plans to migrate, and what it would
        // remove or can't take.
        let before = self.table_objects_before(account, m.table_arn.as_deref(), id);
        let mut object_findings: BTreeMap<String, i64> = BTreeMap::new();
        let mut planned: BTreeMap<String, String> = BTreeMap::new();
        let mut object_rows = Vec::new();
        for o in &objects {
            let overwritten = before.get(&o.key).is_some_and(|etag| *etag != o.etag);
            let (finding, blocking) = if overwritten {
                (Some("OBJECT_OVERWRITTEN"), true)
            } else {
                (o.finding, o.blocking)
            };
            if blocking {
                *object_findings
                    .entry(finding.unwrap_or_default().to_string())
                    .or_default() += 1;
            }
            if finding.is_none() {
                planned.insert(o.key.clone(), o.etag.clone());
            }
            object_rows.push(json!({
                "key": o.key,
                "size": o.size,
                "included": finding.is_none(),
                "finding": finding,
            }));
        }
        let removed: Vec<String> = before
            .keys()
            .filter(|k| !objects.iter().any(|o| &o.key == *k))
            .cloned()
            .collect();
        if m.migration_type == "start" && planned.is_empty() && object_findings.is_empty() {
            return self.fail_migration(
                account,
                id,
                ("sourceNotSupported", Some("EMPTY_TABLE".to_string())),
            );
        }

        // A start creates the destination table during planning.
        let table_arn = match &m.table_arn {
            Some(arn) => arn.clone(),
            None => match self.create_migration_table(account, &m, &schema_fields, &source) {
                Ok(arn) => arn,
                Err(f) => return self.fail_migration(account, id, f),
            },
        };
        self.update_migration(account, id, |rec| {
            rec.table_arn = Some(table_arn.clone());
        });

        // The compatibility reports.
        if !report_bucket_exists {
            return self.fail_migration(account, id, ("resourceNotFound", Some(report_bucket)));
        }
        let prefix = |report: &str| {
            m.migration_output[report]["prefix"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        };
        let table_report = json!({
            "migrationId": id,
            "source": format!(
                "{}.{}",
                m.source["glueTable"]["databaseName"].as_str().unwrap_or_default(),
                m.source["glueTable"]["tableName"].as_str().unwrap_or_default()
            ),
            "sampleObject": planned.keys().next(),
            "findings": table_findings,
            "columns": column_rows,
        });
        let table_key = format!(
            "{}{id}/table-compatibility-report.json",
            prefix("tableCompatibilityReport")
        );
        let table_written = match self.write_report(
            account,
            &report_bucket_name,
            table_key,
            table_report.to_string().into_bytes(),
        ) {
            Ok(w) => w,
            Err(f) => return self.fail_migration(account, id, f),
        };
        let object_written = match self.write_manifest_report(
            account,
            &report_bucket_name,
            &format!(
                "{}{id}/object-compatibility-report",
                prefix("objectCompatibilityReport")
            ),
            &object_rows,
        ) {
            Ok(w) => w,
            Err(f) => return self.fail_migration(account, id, f),
        };
        let to_migrate: Vec<String> = planned
            .keys()
            .filter(|k| !before.contains_key(*k))
            .cloned()
            .collect();
        let valid = table_findings.is_empty() && object_findings.is_empty();
        let partition_count = if source.partition_keys.is_empty() {
            0
        } else {
            source.data_prefixes.len()
        };
        let now = Utc::now();
        self.update_migration(account, id, |rec| {
            let report = |w: &(String, String), findings: &BTreeMap<String, i64>| {
                let mut r = json!({ "key": w.0, "checksumCRC64NVME": w.1 });
                if !findings.is_empty() {
                    r["invalidFindings"] = json!(findings);
                }
                r
            };
            merge_report(
                &mut rec.migration_output,
                "tableCompatibilityReport",
                report(&table_written, &table_findings),
            );
            merge_report(
                &mut rec.migration_output,
                "objectCompatibilityReport",
                report(&object_written, &object_findings),
            );
            rec.has_valid_plan = Some(valid);
            let mut planning = json!({
                "objectsScanned": objects.len(),
                "objectsPlanned": planned.len(),
            });
            if valid {
                planning["partitionCount"] = json!(partition_count);
            }
            rec.planning_stats = Some(planning);
            // Earlier migrations' objects that stay count as migrated; what
            // this one adds is pending until it runs.
            rec.migration_stats = Some(json!({
                "objectsMigrated": before.len().saturating_sub(removed.len()),
                "objectsFailedMigration": 0,
                "objectsPendingMigration": to_migrate.len(),
            }));
            rec.planned_objects = planned.clone();
            rec.removed_objects = removed.clone();
            rec.migrated_objects = before.clone();
            rec.state_updated_at = now;
            if !valid {
                rec.state = "failed".to_string();
                rec.failure_code = Some(
                    if table_findings.is_empty() {
                        "objectCompatibilityReport"
                    } else {
                        "tableCompatibilityReport"
                    }
                    .to_string(),
                );
                rec.failure_detail = None;
            } else if rec.review_migration_plan {
                rec.state = "pendingReview".to_string();
            } else {
                rec.state = "queued".to_string();
            }
        });
        if valid && !m.review_migration_plan {
            self.run_migration(account, id);
        }
    }

    /// Create the destination table of a start: the planned schema (with an
    /// identity partition spec over the source's partition columns), the
    /// requested or the bucket's encryption and storage class, the tags, and
    /// the migration's maintenance settings.
    fn create_migration_table(
        &self,
        account: &str,
        m: &MigrationRecord,
        schema_fields: &[Value],
        source: &GlueSource,
    ) -> Result<String, PlanFailure> {
        let dest = &m.destination;
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(account)
            .ok_or(("resourceNotFound", Some(m.table_bucket_arn.clone())))?;
        let b = st
            .table_buckets
            .get_mut(&m.table_bucket_arn)
            .ok_or(("resourceNotFound", Some(m.table_bucket_arn.clone())))?;
        let namespace_id = b
            .namespaces
            .get(&m.namespace)
            .ok_or(("resourceNotFound", Some(m.namespace.clone())))?
            .namespace_id
            .clone();
        if b.tables
            .values()
            .any(|t| t.namespace == m.namespace && t.name == m.name)
        {
            return Err(("destinationPermissions", Some(m.name.clone())));
        }
        let field_id = |name: &str| {
            schema_fields
                .iter()
                .find(|f| f["name"] == name)
                .and_then(|f| f["id"].as_i64())
        };
        let partition_fields: Vec<Value> = source
            .partition_keys
            .iter()
            .enumerate()
            .filter_map(|(i, k)| {
                field_id(k).map(|sid| {
                    json!({ "source-id": sid, "field-id": 1000 + i, "transform": "identity", "name": k })
                })
            })
            .collect();
        let props = &dest["properties"];
        let mut metadata = json!({
            "schemaV2": {
                "type": "struct",
                "fields": schema_fields,
                "schema-id": dest["schema"]["schema-id"].as_i64().unwrap_or(0),
            },
            "partitionSpec": { "spec-id": 0, "fields": partition_fields },
            "properties": {
                "format-version": props["icebergFormatVersion"].as_i64().unwrap_or(2).to_string(),
                "write.format.default": props["writeFormat"].as_str().unwrap_or("parquet"),
                "write.parquet.compression-codec": props["compressionCodec"].as_str().unwrap_or("zstd"),
                "write.parquet.dict-encoding.enabled": props["dictionaryEncoding"].as_bool().unwrap_or(true).to_string(),
            },
        });
        if let Some(order) = dest.get("icebergSortOrder").filter(|v| !v.is_null()) {
            metadata["writeOrder"] = order.clone();
        }
        let table_id = gen_id();
        let tarn = table_arn(&b.arn, &table_id);
        let warehouse = warehouse_location(&table_id);
        let now = Utc::now();
        let dm = &m.data_migration["maintenanceSettings"];
        let mut maintenance = default_table_maintenance();
        maintenance.insert(
            "icebergCompaction".to_string(),
            json!({ "status": "enabled", "settings": { "icebergCompaction": dm["icebergCompaction"] } }),
        );
        maintenance.insert(
            "icebergSnapshotManagement".to_string(),
            json!({ "status": "enabled", "settings": { "icebergSnapshotManagement": dm["icebergSnapshotManagement"] } }),
        );
        let encryption = dest
            .get("encryptionConfiguration")
            .filter(|v| !v.is_null())
            .cloned()
            .or_else(|| b.encryption.clone())
            .unwrap_or_else(|| json!({ "sseAlgorithm": "AES256" }));
        let storage_class = dest
            .get("storageClassConfiguration")
            .filter(|v| !v.is_null())
            .cloned()
            .or_else(|| b.storage_class.clone())
            .unwrap_or_else(|| json!({ "storageClass": "STANDARD" }));
        let record = TableRecord {
            table_id: table_id.clone(),
            name: m.name.clone(),
            namespace: m.namespace.clone(),
            arn: tarn.clone(),
            table_type: "customer".into(),
            format: "ICEBERG".into(),
            created_at: now,
            modified_at: now,
            created_by: account.to_string(),
            modified_by: String::new(),
            owner_account_id: account.to_string(),
            version_token: gen_token(),
            metadata_location: Some(format!(
                "{warehouse}/metadata/00000-{}.metadata.json",
                gen_id()
            )),
            iceberg_metadata: Some(json!({ "iceberg": metadata })),
            warehouse_location: warehouse,
            managed_by_service: None,
            namespace_id,
            encryption: Some(encryption),
            maintenance,
            policy: None,
            record_expiration: None,
            replication: None,
            storage_class: Some(storage_class),
            tags: parse_tags(dest, "tags"),
        };
        b.tables.insert(table_id, record);
        Ok(tarn)
    }

    /// Migrate a planned (queued) migration: commit the planned objects'
    /// data to the table as a new metadata version and write the object
    /// migration report.
    fn run_migration(&self, account: &str, id: &str) {
        let Some(m) = self
            .state
            .read()
            .get(account)
            .and_then(|s| s.migrations.get(id).cloned())
        else {
            return;
        };
        let to_migrate: Vec<(String, String)> = m
            .planned_objects
            .iter()
            .filter(|(k, _)| !m.migrated_objects.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut after = m.migrated_objects.clone();
        for k in &m.removed_objects {
            after.remove(k);
        }
        for (k, v) in &to_migrate {
            after.insert(k.clone(), v.clone());
        }
        let changed = !to_migrate.is_empty() || !m.removed_objects.is_empty();
        let mut report = None;
        if changed {
            let bucket =
                report_bucket_name(m.migration_output["bucket"].as_str().unwrap_or_default())
                    .unwrap_or_default();
            let mut rows: Vec<Value> = to_migrate
                .iter()
                .map(|(k, _)| json!({ "key": k, "status": "migrated", "transformation": "rewritten" }))
                .collect();
            rows.extend(
                m.removed_objects
                    .iter()
                    .map(|k| json!({ "key": k, "status": "removed" })),
            );
            let base = format!(
                "{}{id}/object-migration-report",
                m.migration_output["objectMigrationReport"]["prefix"]
                    .as_str()
                    .unwrap_or_default()
            );
            match self.write_manifest_report(account, &bucket, &base, &rows) {
                Ok(w) => report = Some(w),
                Err(f) => return self.fail_migration(account, id, f),
            }
            // The data is committed to the table as a new metadata version.
            let mut accounts = self.state.write();
            if let Some(t) = accounts.get_mut(account).and_then(|s| {
                let arn = m.table_arn.as_deref()?;
                s.table_buckets
                    .values_mut()
                    .find_map(|b| b.tables.values_mut().find(|t| t.arn == arn))
            }) {
                let version = t
                    .metadata_location
                    .as_deref()
                    .and_then(|l| l.rsplit('/').next())
                    .and_then(|f| f.split('-').next())
                    .and_then(|n| n.parse::<u32>().ok())
                    .map_or(0, |n| n + 1);
                t.metadata_location = Some(format!(
                    "{}/metadata/{version:05}-{}.metadata.json",
                    t.warehouse_location,
                    gen_id()
                ));
                t.version_token = gen_token();
                t.modified_at = Utc::now();
                t.modified_by = account.to_string();
            }
        }
        self.update_migration(account, id, |rec| {
            if let Some(w) = &report {
                merge_report(
                    &mut rec.migration_output,
                    "objectMigrationReport",
                    json!({ "key": w.0, "checksumCRC64NVME": w.1 }),
                );
            }
            rec.migration_stats = Some(json!({
                "objectsMigrated": after.len(),
                "objectsFailedMigration": 0,
                "objectsPendingMigration": 0,
            }));
            rec.migrated_objects = after;
            rec.state = "succeeded".to_string();
            rec.state_updated_at = Utc::now();
        });
    }

    /// The migration a Get / Update names by `migrationId` or `tableARN`
    /// (the table's most recent one), exactly one of which is given.
    fn resolve_migration_id(&self, req: &AwsRequest) -> Result<String, AwsServiceError> {
        let id = query(req, "migrationId");
        let table = query(req, "tableARN");
        let accounts = self.state.read();
        let st = accounts.get(&req.account_id);
        match (id, table) {
            (Some(_), Some(_)) | (None, None) => Err(bad_request(
                "Specify either migrationId or tableARN, not both.",
            )),
            (Some(id), None) => st
                .and_then(|s| s.migrations.get(&id))
                .map(|m| m.migration_id.clone())
                .ok_or_else(|| not_found(format!("The migration {id} does not exist."))),
            (None, Some(arn)) => st
                .and_then(|s| latest_migration_for_table(&s.migrations, &arn))
                .map(|m| m.migration_id.clone())
                .ok_or_else(|| not_found(format!("The table {arn} has no migration."))),
        }
    }

    fn start_table_migration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let role_arn = validate_role_arn(body["roleARN"].as_str())?;
        let dest = body
            .get("destinationTable")
            .filter(|v| v.is_object())
            .ok_or_else(|| bad_request("destinationTable is required."))?;
        validate_destination(dest)?;
        check_enum_value(&body["sourceType"], "sourceType", &["glueTable"])?;
        let source = body
            .get("source")
            .and_then(Value::as_object)
            .filter(|s| s.len() == 1 && s.contains_key("glueTable"))
            .ok_or_else(|| {
                bad_request("source must set exactly one member, matching sourceType.")
            })?;
        if body["sourceType"] != "glueTable" {
            return Err(bad_request(
                "sourceType must match the member set in source.",
            ));
        }
        let glue = &source["glueTable"];
        for member in ["catalogId", "databaseName", "tableName"] {
            let v = glue[member]
                .as_str()
                .ok_or_else(|| bad_request(format!("source.glueTable.{member} is required.")))?;
            check_len(Some(v), &format!("source.glueTable.{member}"), 1, 255)?;
        }
        if glue["catalogId"] != req.account_id.as_str() {
            return Err(bad_request(
                "source.glueTable.catalogId must be your account ID.",
            ));
        }
        let output = body
            .get("migrationOutput")
            .filter(|v| v.is_object())
            .ok_or_else(|| bad_request("migrationOutput is required."))?;
        validate_migration_output(output)?;
        let data_migration = data_migration_overview(&body["dataMigration"])?;
        let review = body["reviewMigrationPlan"].as_bool().unwrap_or(false);
        let bucket_arn = dest["tableBucketARN"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let namespace = dest["namespace"].as_str().unwrap_or_default().to_string();
        let name = dest["name"].as_str().unwrap_or_default().to_string();
        let region = bucket_arn
            .split(':')
            .nth(3)
            .unwrap_or(&req.region)
            .to_string();

        let id = gen_id();
        let now = Utc::now();
        {
            let mut accounts = self.state.write();
            let st = accounts.get_mut(&req.account_id).ok_or_else(|| {
                not_found(format!("The table bucket {bucket_arn} does not exist."))
            })?;
            let b = st.table_buckets.get(&bucket_arn).ok_or_else(|| {
                not_found(format!("The table bucket {bucket_arn} does not exist."))
            })?;
            if !b.namespaces.contains_key(&namespace) {
                return Err(not_found(format!(
                    "The namespace {namespace} does not exist."
                )));
            }
            if b.tables
                .values()
                .any(|t| t.namespace == namespace && t.name == name)
            {
                return Err(conflict(format!("A table named {name} already exists.")));
            }
            if st.migrations.values().any(|m| {
                m.table_bucket_arn == bucket_arn
                    && m.namespace == namespace
                    && m.name == name
                    && MIGRATION_IN_PROGRESS.contains(&m.state.as_str())
            }) {
                return Err(conflict(format!(
                    "A migration into the table {name} is already in progress."
                )));
            }
            let prefixes = &output["prefixes"];
            let overview = |report: &str| match prefixes[report].as_str() {
                Some(p) => json!({ "prefix": p }),
                None => json!({}),
            };
            st.migrations.insert(
                id.clone(),
                MigrationRecord {
                    migration_id: id.clone(),
                    migration_type: "start".to_string(),
                    state: "planning".to_string(),
                    table_bucket_arn: bucket_arn,
                    namespace,
                    name,
                    table_arn: None,
                    role_arn,
                    review_migration_plan: review,
                    has_valid_plan: None,
                    created_at: now,
                    state_updated_at: now,
                    failure_code: None,
                    failure_detail: None,
                    planning_stats: None,
                    migration_stats: None,
                    source: json!({ "glueTable": glue }),
                    destination: dest.clone(),
                    data_migration,
                    migration_output: json!({
                        "bucket": output["bucket"],
                        "tableCompatibilityReport": overview("tableCompatibilityReport"),
                        "objectCompatibilityReport": overview("objectCompatibilityReport"),
                        "objectMigrationReport": overview("objectMigrationReport"),
                    }),
                    planned_objects: BTreeMap::new(),
                    removed_objects: Vec::new(),
                    migrated_objects: BTreeMap::new(),
                },
            );
        }
        self.plan_migration(&req.account_id, &region, &id);
        let state = self.migration_state(&req.account_id, &id);
        Ok(AwsResponse::json_value(
            StatusCode::ACCEPTED,
            json!({ "migrationId": id, "state": state }),
        ))
    }

    fn migration_state(&self, account: &str, id: &str) -> String {
        self.state
            .read()
            .get(account)
            .and_then(|s| s.migrations.get(id))
            .map(|m| m.state.clone())
            .unwrap_or_default()
    }

    fn sync_table_migration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let role_arn = validate_role_arn(body["roleARN"].as_str())?;
        let table_arn = body["tableARN"]
            .as_str()
            .ok_or_else(|| bad_request("tableARN is required."))?
            .to_string();
        check_len(Some(&table_arn), "tableARN", 1, 2048)?;
        let output = body
            .get("migrationOutput")
            .filter(|v| v.is_object())
            .ok_or_else(|| bad_request("migrationOutput is required."))?;
        validate_migration_output(output)?;
        let review = body["reviewMigrationPlan"].as_bool().unwrap_or(false);

        let id = gen_id();
        let now = Utc::now();
        let region;
        {
            let mut accounts = self.state.write();
            let st = accounts
                .get_mut(&req.account_id)
                .ok_or_else(|| not_found(format!("The table {table_arn} does not exist.")))?;
            let table_exists = st
                .table_buckets
                .values()
                .any(|b| b.tables.values().any(|t| t.arn == table_arn));
            if !table_exists {
                return Err(not_found(format!("The table {table_arn} does not exist.")));
            }
            let latest = latest_migration_for_table(&st.migrations, &table_arn)
                .cloned()
                .ok_or_else(|| not_found(format!("The table {table_arn} has no migration.")))?;
            if st.completed_migrations.contains(&table_arn) {
                return Err(conflict(format!(
                    "The migration of the table {table_arn} was completed; it can't be synced."
                )));
            }
            if MIGRATION_IN_PROGRESS.contains(&latest.state.as_str()) {
                return Err(conflict(format!(
                    "A migration of the table {table_arn} is in progress."
                )));
            }
            let has_valid = st.migrations.values().any(|m| {
                m.table_arn.as_deref() == Some(table_arn.as_str()) && m.has_valid_plan == Some(true)
            });
            if !has_valid {
                return Err(conflict(format!(
                    "No earlier migration of the table {table_arn} has a valid plan."
                )));
            }
            region = latest
                .table_bucket_arn
                .split(':')
                .nth(3)
                .unwrap_or(&req.region)
                .to_string();
            let prefixes = &output["prefixes"];
            let overview = |report: &str| match prefixes[report].as_str() {
                Some(p) => json!({ "prefix": p }),
                None => json!({}),
            };
            st.migrations.insert(
                id.clone(),
                MigrationRecord {
                    migration_id: id.clone(),
                    migration_type: "sync".to_string(),
                    state: "planning".to_string(),
                    table_bucket_arn: latest.table_bucket_arn.clone(),
                    namespace: latest.namespace.clone(),
                    name: latest.name.clone(),
                    table_arn: Some(table_arn),
                    role_arn,
                    review_migration_plan: review,
                    has_valid_plan: None,
                    created_at: now,
                    state_updated_at: now,
                    failure_code: None,
                    failure_detail: None,
                    planning_stats: None,
                    migration_stats: None,
                    source: latest.source.clone(),
                    destination: latest.destination.clone(),
                    data_migration: latest.data_migration.clone(),
                    migration_output: json!({
                        "bucket": output["bucket"],
                        "tableCompatibilityReport": overview("tableCompatibilityReport"),
                        "objectCompatibilityReport": overview("objectCompatibilityReport"),
                        "objectMigrationReport": overview("objectMigrationReport"),
                    }),
                    planned_objects: BTreeMap::new(),
                    removed_objects: Vec::new(),
                    migrated_objects: BTreeMap::new(),
                },
            );
        }
        self.plan_migration(&req.account_id, &region, &id);
        let state = self.migration_state(&req.account_id, &id);
        Ok(AwsResponse::json_value(
            StatusCode::ACCEPTED,
            json!({ "migrationId": id, "state": state }),
        ))
    }

    fn update_table_migration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req_body(req);
        let requested = body["requestedState"]
            .as_str()
            .ok_or_else(|| bad_request("requestedState is required."))?
            .to_string();
        check_enum_value(
            &body["requestedState"],
            "requestedState",
            &["queued", "cancelled"],
        )?;
        if let Some(d) = body["updateDescription"].as_str() {
            check_len(Some(d), "updateDescription", 0, 1024)?;
            if d.chars().any(char::is_control) {
                return Err(bad_request(
                    "updateDescription must not contain control characters.",
                ));
            }
        }
        let id = self.resolve_migration_id(req)?;
        let current = self.migration_state(&req.account_id, &id);
        match (requested.as_str(), current.as_str()) {
            ("queued", "pendingReview") => {
                self.update_migration(&req.account_id, &id, |m| {
                    m.state = "queued".to_string();
                    m.state_updated_at = Utc::now();
                });
                self.run_migration(&req.account_id, &id);
            }
            ("queued", other) => {
                return Err(conflict(format!(
                    "The migration {id} is {other}; only a migration in pendingReview can be queued."
                )));
            }
            ("cancelled", s) if MIGRATION_IN_PROGRESS.contains(&s) => {
                // Nothing was migrated yet: the migration ends cancelled, with
                // what it planned still pending.
                self.update_migration(&req.account_id, &id, |m| {
                    m.state = "cancelled".to_string();
                    m.state_updated_at = Utc::now();
                });
            }
            (_, other) => {
                return Err(conflict(format!(
                    "The migration {id} is {other} and can't be cancelled."
                )));
            }
        }
        let accounts = self.state.read();
        let m = accounts
            .get(&req.account_id)
            .and_then(|s| s.migrations.get(&id))
            .ok_or_else(|| not_found(format!("The migration {id} does not exist.")))?;
        let mut out = json!({ "migrationId": m.migration_id, "state": m.state });
        if let Some(arn) = &m.table_arn {
            out["tableARN"] = json!(arn);
        }
        Ok(ok(out))
    }

    fn complete_table_migration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let table_arn =
            query(req, "tableARN").ok_or_else(|| bad_request("tableARN is required."))?;
        check_len(Some(&table_arn), "tableARN", 1, 2048)?;
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| not_found(format!("The table {table_arn} has no migration.")))?;
        let latest = latest_migration_for_table(&st.migrations, &table_arn)
            .ok_or_else(|| not_found(format!("The table {table_arn} has no migration.")))?;
        if st.completed_migrations.contains(&table_arn) {
            return Err(conflict(format!(
                "The migration of the table {table_arn} is already completed."
            )));
        }
        if MIGRATION_IN_PROGRESS.contains(&latest.state.as_str()) {
            return Err(conflict(format!(
                "The table's most recent migration is {}; it must be succeeded, failed or cancelled.",
                latest.state
            )));
        }
        st.completed_migrations.insert(table_arn);
        Ok(empty(204))
    }

    fn get_table_migration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = self.resolve_migration_id(req)?;
        let accounts = self.state.read();
        let m = accounts
            .get(&req.account_id)
            .and_then(|s| s.migrations.get(&id))
            .ok_or_else(|| not_found(format!("The migration {id} does not exist.")))?;
        Ok(ok(migration_json(m, true)))
    }

    fn list_table_migrations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let bucket = query(req, "tableBucketARN");
        let namespace = query(req, "namespace");
        let name = query(req, "name");
        let table = query(req, "tableARN");
        if table.is_some() && (bucket.is_some() || namespace.is_some() || name.is_some()) {
            return Err(bad_request(
                "Specify either tableARN or tableBucketARN with namespace and name, not both.",
            ));
        }
        if table.is_none() && bucket.is_none() {
            return Err(bad_request("Specify tableBucketARN or tableARN."));
        }
        if namespace.is_some() && bucket.is_none() {
            return Err(bad_request("namespace requires tableBucketARN."));
        }
        if name.is_some() && namespace.is_none() {
            return Err(bad_request("name requires tableBucketARN and namespace."));
        }
        if let Some(s) = query(req, "state") {
            check_enum_value(
                &json!(s),
                "state",
                &[
                    "new",
                    "planning",
                    "pendingReview",
                    "queued",
                    "migrating",
                    "succeeded",
                    "failed",
                    "cancelling",
                    "cancelled",
                ],
            )?;
        }
        let state_filter = query(req, "state");
        let valid_filter = match query(req, "hasValidPlan").as_deref() {
            None => None,
            Some("true") => Some(true),
            Some("false") => Some(false),
            Some(other) => {
                return Err(bad_request(format!(
                    "Invalid value {other} for hasValidPlan."
                )));
            }
        };
        let accounts = self.state.read();
        let mut items: Vec<&MigrationRecord> = accounts
            .get(&req.account_id)
            .map(|s| s.migrations.values().collect())
            .unwrap_or_default();
        items.retain(|m| match &table {
            Some(arn) => m.table_arn.as_deref() == Some(arn.as_str()),
            None => {
                bucket.as_deref() == Some(m.table_bucket_arn.as_str())
                    && namespace.as_deref().is_none_or(|n| n == m.namespace)
                    && name.as_deref().is_none_or(|n| n == m.name)
            }
        });
        items.retain(|m| state_filter.as_deref().is_none_or(|s| s == m.state));
        if let Some(v) = valid_filter {
            // Migrations still planning have no plan yet and are never listed.
            items.retain(|m| m.has_valid_plan == Some(v));
        }
        items.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then(b.migration_id.cmp(&a.migration_id))
        });
        let max = query_usize(req, "maxResults").or(Some(100));
        let (start, end, next) = paginate(items.len(), query(req, "nextToken").as_deref(), max);
        let page: Vec<Value> = items[start..end]
            .iter()
            .map(|m| migration_json(m, false))
            .collect();
        let mut out = json!({ "migrations": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        Ok(ok(out))
    }
}

/// Merge the written report's members into the overview of `report`.
fn merge_report(output: &mut Value, report: &str, written: Value) {
    if !output[report].is_object() {
        output[report] = json!({});
    }
    if let (Some(dst), Some(src)) = (output[report].as_object_mut(), written.as_object()) {
        for (k, v) in src {
            dst.insert(k.clone(), v.clone());
        }
    }
}

/// `GetTableMigration` output (`full`) or a `MigrationSummary`.
fn migration_json(m: &MigrationRecord, full: bool) -> Value {
    let mut out = json!({
        "migrationId": m.migration_id,
        "tableBucketARN": m.table_bucket_arn,
        "namespace": m.namespace,
        "name": m.name,
        "type": m.migration_type,
        "state": m.state,
        "createdAt": ts(m.created_at),
        "stateUpdatedAt": ts(m.state_updated_at),
    });
    if let Some(arn) = &m.table_arn {
        out["tableARN"] = json!(arn);
    }
    if let Some(v) = m.has_valid_plan {
        out["hasValidPlan"] = json!(v);
    }
    if !full {
        return out;
    }
    out["roleARN"] = json!(m.role_arn);
    out["reviewMigrationPlan"] = json!(m.review_migration_plan);
    out["sourceType"] = json!("glueTable");
    out["source"] = m.source.clone();
    out["dataMigration"] = m.data_migration.clone();
    out["migrationOutput"] = m.migration_output.clone();
    if m.state == "failed" {
        if let Some(code) = &m.failure_code {
            let mut reason = json!({ "code": code });
            if let Some(d) = &m.failure_detail {
                reason["detail"] = json!(d);
            }
            out["failureReason"] = reason;
        }
    }
    if let Some(p) = &m.planning_stats {
        out["planningStats"] = p.clone();
    }
    if let Some(s) = &m.migration_stats {
        out["migrationStats"] = s.clone();
    }
    out
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsService;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    const ACCOUNT: &str = "123456789012";
    const ROLE: &str = "arn:aws:iam::123456789012:role/migrate";
    const REPORTS: &str = "arn:aws:s3:::reports";

    struct Fixture {
        svc: S3TablesService,
        glue: fakecloud_glue::GlueService,
        s3: fakecloud_s3::SharedS3State,
        bucket_arn: String,
    }

    fn request(method: Method, path: &str, query: &[(&str, &str)], body: Value) -> AwsRequest {
        AwsRequest {
            service: "s3tables".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: ACCOUNT.to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: path.to_string(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn call(
        f: &Fixture,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Value,
    ) -> Result<(u16, Value), AwsServiceError> {
        let resp = f.svc.handle(request(method, path, query, body)).await?;
        let bytes = resp.body.expect_bytes();
        let v = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(bytes).unwrap()
        };
        Ok((resp.status.as_u16(), v))
    }

    async fn glue(f: &Fixture, action: &str, body: Value) {
        let mut req = request(Method::POST, "/", &[], body);
        req.service = "glue".to_string();
        req.action = action.to_string();
        f.glue.handle(req).await.unwrap();
    }

    fn put_object(f: &Fixture, bucket: &str, key: &str, data: &[u8], etag: &str) {
        f.s3.write()
            .get_or_create(ACCOUNT)
            .buckets
            .get_mut(bucket)
            .unwrap()
            .objects
            .insert(
                key.to_string(),
                fakecloud_s3::S3Object {
                    key: key.to_string(),
                    body: fakecloud_s3::memory_body(bytes::Bytes::from(data.to_vec())),
                    size: data.len() as u64,
                    etag: etag.to_string(),
                    ..Default::default()
                },
            );
    }

    fn object(f: &Fixture, bucket: &str, key: &str) -> Option<Vec<u8>> {
        let guard = f.s3.read();
        let acct = guard.get(ACCOUNT)?;
        let obj = acct.buckets.get(bucket)?.objects.get(key)?;
        acct.read_body(&obj.body).ok().map(|b| b.to_vec())
    }

    async fn fixture() -> Fixture {
        let glue_state: fakecloud_glue::SharedGlueState =
            Arc::new(RwLock::new(fakecloud_glue::GlueAccounts::new()));
        let s3: fakecloud_s3::SharedS3State = Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "",
        )));
        for b in ["src", "reports"] {
            s3.write().get_or_create(ACCOUNT).buckets.insert(
                b.to_string(),
                fakecloud_s3::S3Bucket::new(b, "us-east-1", "owner"),
            );
        }
        let svc = S3TablesService::new(Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "",
        ))))
        .with_migration_sources(glue_state.clone(), s3.clone(), None);
        let mut f = Fixture {
            svc,
            glue: fakecloud_glue::GlueService::new(glue_state),
            s3,
            bucket_arn: String::new(),
        };
        let (_, created) = call(&f, Method::PUT, "/buckets", &[], json!({ "name": "bkt" }))
            .await
            .unwrap();
        f.bucket_arn = created["arn"].as_str().unwrap().to_string();
        let earn = percent_encoding::utf8_percent_encode(
            &f.bucket_arn,
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();
        call(
            &f,
            Method::PUT,
            &format!("/namespaces/{earn}"),
            &[],
            json!({ "namespace": ["ns"] }),
        )
        .await
        .unwrap();
        glue(
            &f,
            "CreateDatabase",
            json!({ "DatabaseInput": { "Name": "db" } }),
        )
        .await;
        glue(
            &f,
            "CreateTable",
            json!({
                "DatabaseName": "db",
                "TableInput": {
                    "Name": "events",
                    "TableType": "EXTERNAL_TABLE",
                    "StorageDescriptor": {
                        "Columns": [
                            { "Name": "id", "Type": "bigint" },
                            { "Name": "msg", "Type": "varchar(20)" }
                        ],
                        "Location": "s3://src/events",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        }
                    }
                }
            }),
        )
        .await;
        put_object(&f, "src", "events/part-0.parquet", b"PAR1data0PAR1", "e0");
        put_object(&f, "src", "events/part-1.parquet", b"PAR1data1PAR1", "e1");
        put_object(&f, "src", "events/_SUCCESS", b"", "es");
        f
    }

    fn start_body(f: &Fixture, table: &str, glue_table: &str, review: bool) -> Value {
        json!({
            "roleARN": ROLE,
            "destinationTable": { "tableBucketARN": f.bucket_arn, "namespace": "ns", "name": table },
            "sourceType": "glueTable",
            "source": { "glueTable": { "catalogId": ACCOUNT, "databaseName": "db", "tableName": glue_table } },
            "reviewMigrationPlan": review,
            "migrationOutput": { "bucket": REPORTS, "prefixes": { "tableCompatibilityReport": "tcr/" } },
        })
    }

    async fn get(f: &Fixture, id: &str) -> Value {
        call(
            f,
            Method::GET,
            "/get-table-migration",
            &[("migrationId", id)],
            json!({}),
        )
        .await
        .unwrap()
        .1
    }

    #[tokio::test]
    async fn a_reviewed_migration_runs_syncs_and_completes() {
        let f = fixture().await;
        let (status, started) = call(
            &f,
            Method::POST,
            "/start-table-migration",
            &[],
            start_body(&f, "events", "events", true),
        )
        .await
        .unwrap();
        assert_eq!(status, 202);
        assert_eq!(started["state"], "pendingReview");
        let id = started["migrationId"].as_str().unwrap().to_string();

        let m = get(&f, &id).await;
        assert_eq!(m["type"], "start");
        assert_eq!(m["hasValidPlan"], true);
        assert_eq!(m["planningStats"]["objectsScanned"], 3);
        assert_eq!(m["planningStats"]["objectsPlanned"], 2);
        assert_eq!(m["planningStats"]["partitionCount"], 0);
        assert_eq!(m["migrationStats"]["objectsPendingMigration"], 2);
        assert_eq!(
            m["dataMigration"]["maintenanceSettings"]["icebergCompaction"]["targetFileSizeMB"],
            512
        );
        let report = &m["migrationOutput"]["tableCompatibilityReport"];
        assert_eq!(report["prefix"], "tcr/");
        let key = report["key"].as_str().unwrap();
        assert!(key.starts_with("tcr/"), "{key}");
        assert_eq!(report["checksumCRC64NVME"].as_str().unwrap().len(), 12);
        let written: Value = serde_json::from_slice(&object(&f, "reports", key).unwrap()).unwrap();
        assert_eq!(written["columns"][1]["destinationType"], "string");
        let table_arn = m["tableARN"].as_str().unwrap().to_string();

        // The table exists, readable but not writable.
        let earn = percent_encoding::utf8_percent_encode(
            &f.bucket_arn,
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();
        let (_, table) = call(
            &f,
            Method::GET,
            &format!("/tables/{earn}/ns/events"),
            &[],
            json!({}),
        )
        .await
        .unwrap();
        assert_eq!(table["tableARN"], table_arn.as_str());
        let token = table["versionToken"].as_str().unwrap().to_string();
        let write = json!({ "versionToken": token, "metadataLocation": "s3://x/metadata/1.json" });
        let err = call(
            &f,
            Method::PUT,
            &format!("/tables/{earn}/ns/events/metadata-location"),
            &[],
            write.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "ConflictException");

        // Queue it: the data migrates.
        let (_, updated) = call(
            &f,
            Method::POST,
            "/update-table-migration",
            &[("tableARN", &table_arn)],
            json!({ "requestedState": "queued" }),
        )
        .await
        .unwrap();
        assert_eq!(updated["state"], "succeeded");
        let m = get(&f, &id).await;
        assert_eq!(m["migrationStats"]["objectsMigrated"], 2);
        assert_eq!(m["migrationStats"]["objectsPendingMigration"], 0);
        let manifest = m["migrationOutput"]["objectMigrationReport"]["key"]
            .as_str()
            .unwrap();
        assert!(object(&f, "reports", manifest).is_some());
        assert!(m.get("failureReason").is_none());

        // A sync migrates what was added since.
        put_object(&f, "src", "events/part-2.parquet", b"PAR1data2PAR1", "e2");
        let sync = json!({ "roleARN": ROLE, "tableARN": table_arn, "migrationOutput": { "bucket": REPORTS } });
        let (status, synced) = call(&f, Method::POST, "/sync-table-migration", &[], sync.clone())
            .await
            .unwrap();
        assert_eq!(status, 202);
        assert_eq!(synced["state"], "succeeded");
        let s = get(&f, synced["migrationId"].as_str().unwrap()).await;
        assert_eq!(s["type"], "sync");
        assert_eq!(s["migrationStats"]["objectsMigrated"], 3);

        let (_, listed) = call(
            &f,
            Method::GET,
            "/list-table-migrations",
            &[("tableARN", &table_arn)],
            json!({}),
        )
        .await
        .unwrap();
        let migrations = listed["migrations"].as_array().unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0]["migrationId"], synced["migrationId"]);

        // An overwritten source object fails the next sync.
        put_object(
            &f,
            "src",
            "events/part-0.parquet",
            b"PAR1changedPAR1",
            "e0-new",
        );
        let (_, failed) = call(&f, Method::POST, "/sync-table-migration", &[], sync.clone())
            .await
            .unwrap();
        assert_eq!(failed["state"], "failed");
        let s = get(&f, failed["migrationId"].as_str().unwrap()).await;
        assert_eq!(s["failureReason"]["code"], "objectCompatibilityReport");
        assert_eq!(
            s["migrationOutput"]["objectCompatibilityReport"]["invalidFindings"]
                ["OBJECT_OVERWRITTEN"],
            1
        );

        // Completing makes the table writable and ends syncing.
        let (status, _) = call(
            &f,
            Method::POST,
            "/complete-table-migration",
            &[("tableARN", &table_arn)],
            json!({}),
        )
        .await
        .unwrap();
        assert_eq!(status, 204);
        for (path, body) in [
            ("/complete-table-migration", json!({})),
            ("/sync-table-migration", sync),
        ] {
            let err = call(&f, Method::POST, path, &[("tableARN", &table_arn)], body)
                .await
                .unwrap_err();
            assert_eq!(err.code(), "ConflictException", "{path}");
        }
        let (_, table) = call(
            &f,
            Method::GET,
            &format!("/tables/{earn}/ns/events"),
            &[],
            json!({}),
        )
        .await
        .unwrap();
        let write = json!({ "versionToken": table["versionToken"], "metadataLocation": "s3://x/metadata/1.json" });
        call(
            &f,
            Method::PUT,
            &format!("/tables/{earn}/ns/events/metadata-location"),
            &[],
            write,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn planning_failures_and_cancellation() {
        let f = fixture().await;
        // A missing Glue table fails before the table is created.
        let (_, started) = call(
            &f,
            Method::POST,
            "/start-table-migration",
            &[],
            start_body(&f, "nope", "missing", false),
        )
        .await
        .unwrap();
        assert_eq!(started["state"], "failed");
        let m = get(&f, started["migrationId"].as_str().unwrap()).await;
        assert_eq!(
            m["failureReason"],
            json!({ "code": "resourceNotFound", "detail": "db.missing" })
        );
        assert_eq!(m["hasValidPlan"], false);
        assert!(m.get("tableARN").is_none());

        // A non-Parquet object blocks the plan; the table stays.
        put_object(&f, "src", "events/bad.csv", b"a,b\n1,2\n", "eb");
        let (_, started) = call(
            &f,
            Method::POST,
            "/start-table-migration",
            &[],
            start_body(&f, "blocked", "events", false),
        )
        .await
        .unwrap();
        assert_eq!(started["state"], "failed");
        let m = get(&f, started["migrationId"].as_str().unwrap()).await;
        assert_eq!(m["failureReason"]["code"], "objectCompatibilityReport");
        assert_eq!(
            m["migrationOutput"]["objectCompatibilityReport"]["invalidFindings"]["NOT_PARQUET"],
            1
        );
        assert!(m["tableARN"].is_string());

        // The destination table must not exist yet.
        let err = call(
            &f,
            Method::POST,
            "/start-table-migration",
            &[],
            start_body(&f, "blocked", "events", true),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "ConflictException");

        // A reviewed plan can be cancelled, once.
        f.s3.write()
            .get_or_create(ACCOUNT)
            .buckets
            .get_mut("src")
            .unwrap()
            .objects
            .remove("events/bad.csv");
        let (_, started) = call(
            &f,
            Method::POST,
            "/start-table-migration",
            &[],
            start_body(&f, "later", "events", true),
        )
        .await
        .unwrap();
        let id = started["migrationId"].as_str().unwrap().to_string();
        let cancel = json!({ "requestedState": "cancelled" });
        let (_, cancelled) = call(
            &f,
            Method::POST,
            "/update-table-migration",
            &[("migrationId", &id)],
            cancel.clone(),
        )
        .await
        .unwrap();
        assert_eq!(cancelled["state"], "cancelled");
        let err = call(
            &f,
            Method::POST,
            "/update-table-migration",
            &[("migrationId", &id)],
            cancel,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "ConflictException");

        // A role in another account can't be assumed.
        let mut other_role = start_body(&f, "other", "events", false);
        other_role["roleARN"] = json!("arn:aws:iam::999999999999:role/migrate");
        let (_, started) = call(&f, Method::POST, "/start-table-migration", &[], other_role)
            .await
            .unwrap();
        let m = get(&f, started["migrationId"].as_str().unwrap()).await;
        assert_eq!(m["failureReason"]["code"], "rolePermissions");

        // Identify a migration by exactly one of migrationId / tableARN.
        let err = call(&f, Method::GET, "/get-table-migration", &[], json!({}))
            .await
            .unwrap_err();
        assert_eq!(err.code(), "BadRequestException");
        let err = call(
            &f,
            Method::GET,
            "/get-table-migration",
            &[("migrationId", "missing")],
            json!({}),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "NotFoundException");

        // Listing by bucket, filtered by state.
        let (_, listed) = call(
            &f,
            Method::GET,
            "/list-table-migrations",
            &[("tableBucketARN", &f.bucket_arn), ("state", "failed")],
            json!({}),
        )
        .await
        .unwrap();
        assert_eq!(listed["migrations"].as_array().unwrap().len(), 3);
        let err = call(
            &f,
            Method::GET,
            "/list-table-migrations",
            &[("namespace", "ns")],
            json!({}),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), "BadRequestException");
    }

    #[tokio::test]
    async fn list_migrations_bounds_the_table_arn_length() {
        let f = fixture().await;
        let too_long = "a".repeat(2049);
        for arn in ["", too_long.as_str()] {
            let err = call(
                &f,
                Method::GET,
                "/list-table-migrations",
                &[("tableARN", arn)],
                Value::Null,
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.code(),
                "BadRequestException",
                "tableARN len {}",
                arn.len()
            );
        }
    }
}
