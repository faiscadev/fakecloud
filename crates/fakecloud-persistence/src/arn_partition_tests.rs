use super::*;
use serde_json::json;

fn cn() -> ArnPartitionMigration {
    ArnPartitionMigration::for_server_region("cn-north-1")
}

fn commercial() -> ArnPartitionMigration {
    ArnPartitionMigration::for_server_region("us-east-1")
}

fn rewrite(m: &ArnPartitionMigration, s: &str) -> String {
    m.rewrite_str(s).into_owned()
}

#[test]
fn regional_arns_take_their_region_partition() {
    let m = commercial();
    assert_eq!(
        rewrite(&m, "arn:aws:kms:cn-north-1:123456789012:key/k"),
        "arn:aws-cn:kms:cn-north-1:123456789012:key/k"
    );
    assert_eq!(
        rewrite(&m, "arn:aws:sqs:us-gov-west-1:123456789012:q"),
        "arn:aws-us-gov:sqs:us-gov-west-1:123456789012:q"
    );
    assert_eq!(
        rewrite(&m, "arn:aws:sns:us-iso-east-1:123456789012:t"),
        "arn:aws-iso:sns:us-iso-east-1:123456789012:t"
    );
    assert_eq!(
        rewrite(&m, "arn:aws:sns:us-isob-east-1:123456789012:t"),
        "arn:aws-iso-b:sns:us-isob-east-1:123456789012:t"
    );
    assert_eq!(
        rewrite(&m, "arn:aws:lambda:us-isof-south-1:1:function:f"),
        "arn:aws-iso-f:lambda:us-isof-south-1:1:function:f"
    );
    assert_eq!(
        rewrite(&m, "arn:aws:lambda:eu-isoe-west-1:1:function:f"),
        "arn:aws-iso-e:lambda:eu-isoe-west-1:1:function:f"
    );
}

#[test]
fn commercial_region_arns_are_untouched() {
    for m in [cn(), commercial()] {
        let s = "arn:aws:kms:us-east-1:123456789012:key/k";
        assert!(matches!(m.rewrite_str(s), Cow::Borrowed(_)));
        assert!(!m.needs_rewrite(s.as_bytes()));
    }
}

#[test]
fn already_partitioned_arns_are_untouched_and_rewrite_is_idempotent() {
    let m = cn();
    for s in [
        "arn:aws-cn:kms:cn-north-1:123456789012:key/k",
        "arn:aws-cn:iam::123456789012:role/r",
        "arn:aws-us-gov:sqs:us-gov-west-1:1:q",
    ] {
        assert!(matches!(m.rewrite_str(s), Cow::Borrowed(_)), "{s}");
    }
    let once = rewrite(
        &m,
        "arn:aws:iam::123456789012:role/r arn:aws:sns:cn-north-1:1:t",
    );
    assert_eq!(rewrite(&m, &once), once);
}

#[test]
fn region_less_arns_follow_the_server_partition() {
    let m = cn();
    assert_eq!(
        rewrite(&m, "arn:aws:iam::123456789012:role/r"),
        "arn:aws-cn:iam::123456789012:role/r"
    );
    assert_eq!(
        rewrite(&m, "arn:aws:s3:::bucket/*"),
        "arn:aws-cn:s3:::bucket/*"
    );
    assert_eq!(
        rewrite(&m, "arn:aws:states:::lambda:invoke"),
        "arn:aws-cn:states:::lambda:invoke"
    );
    let gov = ArnPartitionMigration::for_server_region("us-gov-east-1");
    assert_eq!(
        rewrite(&gov, "arn:aws:iam::1:user/u"),
        "arn:aws-us-gov:iam::1:user/u"
    );

    // A commercial server keeps them: they are genuinely `aws`.
    let m = commercial();
    assert_eq!(
        rewrite(&m, "arn:aws:iam::123456789012:role/r"),
        "arn:aws:iam::123456789012:role/r"
    );
    assert!(!m.needs_rewrite(b"arn:aws:s3:::bucket"));
}

#[test]
fn aws_managed_policy_arns_keep_the_aws_partition() {
    let m = cn();
    for s in [
        "arn:aws:iam::aws:policy/AdministratorAccess",
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    ] {
        assert_eq!(rewrite(&m, s), s);
        assert!(!m.needs_rewrite(s.as_bytes()));
    }
}

#[test]
fn free_text_and_malformed_tokens_are_untouched() {
    let m = cn();
    for s in [
        "arn:aws",
        "arn:aws:",
        "the arn:aws prefix",
        "arn:aws:kms",
        "arn:aws:kms:cn-north-1",
        "arn:aws:kms:cn-north-1:",
        "arn:aws:KMS:cn-north-1:1:key/k",
        "arn:aws::cn-north-1:1:key/k",
        "arn:aws:kms:cn north:1:key/k",
        "",
    ] {
        assert_eq!(rewrite(&m, s), s, "{s:?}");
        assert!(!m.needs_rewrite(s.as_bytes()), "{s:?}");
    }
    // Glued to a word: not a token. (The byte pre-check deliberately does
    // not look at the boundary; see the escape test.)
    for s in ["xarn:aws:kms:cn-north-1:1:key/k", "barn:aws:iam::1:role/r"] {
        assert_eq!(rewrite(&m, s), s, "{s:?}");
    }
}

#[test]
fn pre_check_sees_arns_behind_serialized_escapes() {
    // Regression: the raw bytes of `"a\narn:aws:..."` put `n` before the
    // token, which a boundary check on raw bytes rejected, so a file whose
    // only legacy ARN followed an escape was skipped and marked migrated.
    let m = cn();
    let value = json!({"doc": "line\narn:aws:iam::1:role/r\tarn:aws:sns:cn-north-1:1:t"});
    let raw = serde_json::to_vec(&value).unwrap();
    assert!(m.needs_rewrite(&raw));
    let mut value = value;
    assert!(m.rewrite_json(&mut value, &[]));
    assert_eq!(
        value["doc"],
        "line\narn:aws-cn:iam::1:role/r\tarn:aws-cn:sns:cn-north-1:1:t"
    );
}

#[test]
fn policy_wildcards_move_with_the_resources_they_match() {
    // Regression: `*` in the region or account field made the token
    // unparseable, so patterns stayed `arn:aws:` while the resources they
    // match became `arn:aws-cn:` and stored allows stopped matching.
    let m = cn();
    for (legacy, migrated) in [
        (
            "arn:aws:kms:*:123456789012:key/*",
            "arn:aws-cn:kms:*:123456789012:key/*",
        ),
        (
            "arn:aws:sqs:cn-north-1:*:q",
            "arn:aws-cn:sqs:cn-north-1:*:q",
        ),
        ("arn:aws:sqs:cn-*:1:q", "arn:aws-cn:sqs:cn-*:1:q"),
        ("arn:aws:iam::*:role/x", "arn:aws-cn:iam::*:role/x"),
        ("arn:aws:s3:::*", "arn:aws-cn:s3:::*"),
        ("arn:aws:*:*:*:*", "arn:aws-cn:*:*:*:*"),
        (
            "arn:aws:ec2:cn-north-?:1:vpc/*",
            "arn:aws-cn:ec2:cn-north-?:1:vpc/*",
        ),
    ] {
        assert_eq!(rewrite(&m, legacy), migrated, "{legacy}");
    }
    // A pattern that pins a partition keeps doing so on any server.
    let us = commercial();
    assert_eq!(
        rewrite(&us, "arn:aws:sqs:cn-*:1:q"),
        "arn:aws-cn:sqs:cn-*:1:q"
    );
    // A partition-agnostic pattern on a commercial server is genuinely aws.
    assert_eq!(
        rewrite(&us, "arn:aws:kms:*:1:key/*"),
        "arn:aws:kms:*:1:key/*"
    );
    // Any other partition-agnostic pattern follows the server's partition.
    assert_eq!(
        rewrite(&m, "arn:aws:sqs:us-*:1:q"),
        "arn:aws-cn:sqs:us-*:1:q"
    );
}

#[test]
fn embedded_arns_are_rewritten_in_place() {
    let m = cn();
    // A policy document stored as a string keeps its exact formatting.
    let policy = r#"{
  "Version": "2012-10-17",
  "Statement": [{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},
    "Action":"sqs:*","Resource":["arn:aws:sqs:cn-north-1:123456789012:q","arn:aws:sqs:us-east-1:1:other"]}]
}"#;
    let expected = policy
        .replace("arn:aws:iam::", "arn:aws-cn:iam::")
        .replace("arn:aws:sqs:cn-north-1", "arn:aws-cn:sqs:cn-north-1");
    assert_eq!(rewrite(&m, policy), expected);
    assert!(m.needs_rewrite(policy.as_bytes()));

    // Composite keys and delimited lists.
    assert_eq!(
        rewrite(
            &m,
            "123456789012|arn:aws:sns:cn-north-1:1:t,arn:aws:sns:cn-north-1:1:u"
        ),
        "123456789012|arn:aws-cn:sns:cn-north-1:1:t,arn:aws-cn:sns:cn-north-1:1:u"
    );
    // A token at the very end of the string.
    assert_eq!(rewrite(&m, "see arn:aws:s3:::b"), "see arn:aws-cn:s3:::b");
    // Non-ASCII text around a token.
    assert_eq!(
        rewrite(&m, "é arn:aws:sns:cn-north-1:1:t ü"),
        "é arn:aws-cn:sns:cn-north-1:1:t ü"
    );
}

#[test]
fn json_values_keys_and_nesting_are_rewritten() {
    let m = cn();
    let mut v = json!({
        "accounts": {
            "123456789012": {
                "topics": {
                    "arn:aws:sns:cn-north-1:123456789012:t": {
                        "topic_arn": "arn:aws:sns:cn-north-1:123456789012:t",
                        "attributes": {
                            "Policy": "{\"Resource\":\"arn:aws:sns:cn-north-1:123456789012:t\"}"
                        },
                        "count": 3,
                        "flags": [true, null, "arn:aws:iam::123456789012:role/r"]
                    }
                },
                "managed": ["arn:aws:iam::aws:policy/ReadOnlyAccess"],
            }
        }
    });
    assert!(m.rewrite_json(&mut v, &[]));
    assert_eq!(
        v,
        json!({
            "accounts": {
                "123456789012": {
                    "topics": {
                        "arn:aws-cn:sns:cn-north-1:123456789012:t": {
                            "topic_arn": "arn:aws-cn:sns:cn-north-1:123456789012:t",
                            "attributes": {
                                "Policy": "{\"Resource\":\"arn:aws-cn:sns:cn-north-1:123456789012:t\"}"
                            },
                            "count": 3,
                            "flags": [true, null, "arn:aws-cn:iam::123456789012:role/r"]
                        }
                    },
                    "managed": ["arn:aws:iam::aws:policy/ReadOnlyAccess"],
                }
            }
        })
    );
    // Idempotent.
    assert!(!m.rewrite_json(&mut v, &[]));
}

#[test]
fn opaque_keys_are_kept_verbatim() {
    let m = cn();
    let body = "{\"TopicArn\":\"arn:aws:sns:cn-north-1:1:t\"}";
    let mut v = json!({
        "queue_arn": "arn:aws:sqs:cn-north-1:1:q",
        "messages": [{"body": body, "md5_of_body": "x"}]
    });
    assert!(m.rewrite_json(&mut v, opaque_keys_for("sqs")));
    assert_eq!(v["queue_arn"], "arn:aws-cn:sqs:cn-north-1:1:q");
    assert_eq!(v["messages"][0]["body"], body);
}

#[test]
fn legacy_key_colliding_with_a_current_key_is_dropped() {
    let m = cn();
    for (first, second) in [
        (
            "arn:aws:sns:cn-north-1:1:t",
            "arn:aws-cn:sns:cn-north-1:1:t",
        ),
        (
            "arn:aws-cn:sns:cn-north-1:1:t",
            "arn:aws:sns:cn-north-1:1:t",
        ),
    ] {
        let mut map = Map::new();
        map.insert(
            first.to_string(),
            json!(if first.contains("aws-cn") {
                "current"
            } else {
                "legacy"
            }),
        );
        map.insert(
            second.to_string(),
            json!(if second.contains("aws-cn") {
                "current"
            } else {
                "legacy"
            }),
        );
        let mut v = Value::Object(map);
        assert!(m.rewrite_json(&mut v, &[]));
        assert_eq!(v, json!({"arn:aws-cn:sns:cn-north-1:1:t": "current"}));
    }
}

fn legacy_dir() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join(crate::version::VERSION_FILE_NAME),
        "format_version = 1\nfakecloud_version = \"0.40.0\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n",
    )
    .unwrap();
    tmp
}

fn write(path: &Path, contents: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap()
}

#[test]
fn migrates_a_legacy_data_dir_once() {
    let tmp = legacy_dir();
    let dir = tmp.path();
    let kms = dir.join("kms/snapshot.json");
    write(
        &kms,
        r#"{"schema_version":3,"accounts":{"k":{"arn":"arn:aws:kms:cn-north-1:1:key/k"}}}"#,
    );
    let sqs = dir.join("sqs/snapshot.json");
    write(
        &sqs,
        r#"{"q":{"arn":"arn:aws:sqs:cn-north-1:1:q","messages":[{"body":"arn:aws:sqs:cn-north-1:1:q"}]}}"#,
    );
    let logs_manifest = dir.join("logs/manifest.json");
    write(
        &logs_manifest,
        r#"{"group_arn":"arn:aws:logs:cn-north-1:1:log-group:g"}"#,
    );
    let logs_segment = dir.join("logs/0f8fad5b-d9cb-469f-a165-70867728950e.jsonl");
    let segment = "{\"message\":\"arn:aws:logs:cn-north-1:1:log-group:g\"}\n";
    write(&logs_segment, segment);
    let untouched = dir.join("ssm/snapshot.json");
    let untouched_text = r#"{"p":"arn:aws:ssm:us-east-1:1:parameter/p"}"#;
    write(&untouched, untouched_text);
    let policy = dir.join("s3/buckets/b/policy.toml");
    // Subresource files hold the configuration payload verbatim.
    write(&policy, r#"{"Resource":"arn:aws:s3:::b/*"}"#);
    let object_meta = dir.join("s3/buckets/b/objects/k/null.toml");
    write(
        &object_meta,
        "sse_kms_key_id = \"arn:aws:kms:cn-north-1:1:key/k\"\n",
    );
    let body = dir.join("s3/buckets/b/objects/k/null.bin");
    write(&body, "arn:aws:kms:cn-north-1:1:key/k");
    let broken = dir.join("sns/snapshot.json");
    write(&broken, "{\"arn:aws:sns:cn-north-1:1:t\"");

    let report = migrate_data_dir(dir, "cn-north-1").unwrap();
    assert!(!report.already_migrated);
    assert_eq!(report.skipped, vec![broken.clone()]);
    let mut rewritten = report.rewritten.clone();
    rewritten.sort();
    let mut expected = vec![
        kms.clone(),
        sqs.clone(),
        logs_manifest.clone(),
        policy.clone(),
        object_meta.clone(),
    ];
    expected.sort();
    assert_eq!(rewritten, expected);

    let kms_v: Value = serde_json::from_str(&read(&kms)).unwrap();
    assert_eq!(
        kms_v["accounts"]["k"]["arn"],
        "arn:aws-cn:kms:cn-north-1:1:key/k"
    );
    assert_eq!(kms_v["schema_version"], 3);
    let sqs_v: Value = serde_json::from_str(&read(&sqs)).unwrap();
    assert_eq!(sqs_v["q"]["arn"], "arn:aws-cn:sqs:cn-north-1:1:q");
    assert_eq!(
        sqs_v["q"]["messages"][0]["body"],
        "arn:aws:sqs:cn-north-1:1:q"
    );
    assert!(read(&logs_manifest).contains("arn:aws-cn:logs:cn-north-1"));
    assert_eq!(read(&logs_segment), segment);
    assert_eq!(read(&untouched), untouched_text);
    assert_eq!(read(&policy), r#"{"Resource":"arn:aws-cn:s3:::b/*"}"#);
    assert!(read(&object_meta).contains("arn:aws-cn:kms:cn-north-1"));
    assert_eq!(read(&body), "arn:aws:kms:cn-north-1:1:key/k");
    assert_eq!(read(&broken), "{\"arn:aws:sns:cn-north-1:1:t\"");

    // Marked: the version file keeps its fields and gains the marker.
    let version: crate::version::FormatVersion =
        toml::from_str(&read(&dir.join(crate::version::VERSION_FILE_NAME))).unwrap();
    assert!(version.arn_partitions_migrated);
    assert_eq!(version.fakecloud_version, "0.40.0");
    assert_eq!(version.created_at, "2026-01-01T00:00:00Z");

    // A second start reads nothing, even with new legacy-looking state.
    let later = r#"{"arn":"arn:aws:iam::1:role/created-in-us-east-1"}"#;
    write(&dir.join("iam/snapshot.json"), later);
    let again = migrate_data_dir(dir, "cn-north-1").unwrap();
    assert!(again.already_migrated);
    assert_eq!(read(&dir.join("iam/snapshot.json")), later);
}

#[test]
fn commercial_server_rewrites_only_regional_arns() {
    let tmp = legacy_dir();
    let dir = tmp.path();
    let iam = dir.join("iam/snapshot.json");
    let iam_text = r#"{"arn":"arn:aws:iam::1:role/r"}"#;
    write(&iam, iam_text);
    let kms = dir.join("kms/snapshot.json");
    write(&kms, r#"{"arn":"arn:aws:kms:cn-north-1:1:key/k"}"#);

    let report = migrate_data_dir(dir, "us-east-1").unwrap();
    assert_eq!(report.rewritten, vec![kms.clone()]);
    assert_eq!(read(&iam), iam_text);
    assert!(read(&kms).contains("arn:aws-cn:kms:cn-north-1"));
    assert!(crate::version::arn_partitions_migrated(dir).unwrap());
}

#[test]
fn fresh_data_dir_is_born_migrated() {
    let tmp = tempfile::tempdir().unwrap();
    crate::version::ensure_version_file(tmp.path(), "test").unwrap();
    assert!(crate::version::arn_partitions_migrated(tmp.path()).unwrap());
    let report = migrate_data_dir(tmp.path(), "cn-north-1").unwrap();
    assert!(report.already_migrated);
}

#[test]
fn legacy_version_file_without_marker_is_accepted_by_the_version_check() {
    let tmp = legacy_dir();
    crate::version::check_version_file(tmp.path()).unwrap();
    assert!(!crate::version::arn_partitions_migrated(tmp.path()).unwrap());
}

#[test]
fn customer_data_is_kept_verbatim() {
    // Regression: DynamoDB rows, SSM values and secrets were rewritten, so an
    // item keyed by an ARN string was no longer reachable by the key the
    // application wrote, and reads returned values nobody stored.
    let tmp = legacy_dir();
    let dir = tmp.path();
    let role = "arn:aws:iam::123456789012:role/app";
    let ddb = dir.join("dynamodb/snapshot.json");
    write(
        &ddb,
        &json!({
            "table": {
                "arn": "arn:aws:dynamodb:cn-north-1:1:table/t",
                "items": [{"pk": {"S": role}}],
                "stream_records": [{"keys": {"pk": {"S": role}}, "new_image": {"pk": {"S": role}}, "old_image": null}],
                "backups": [{"items": [{"pk": {"S": role}}]}]
            }
        })
        .to_string(),
    );
    let ssm = dir.join("ssm/snapshot.json");
    write(
        &ssm,
        &json!({"p": {"arn": "arn:aws:ssm:cn-north-1:1:parameter/p", "value": role, "history": [{"value": role}]}})
            .to_string(),
    );
    let secrets = dir.join("secretsmanager/snapshot.json");
    write(
        &secrets,
        &json!({"s": {"arn": "arn:aws:secretsmanager:cn-north-1:1:secret:s", "secret_string": role}})
            .to_string(),
    );

    migrate_data_dir(dir, "cn-north-1").unwrap();

    let ddb: Value = serde_json::from_str(&read(&ddb)).unwrap();
    assert_eq!(
        ddb["table"]["arn"],
        "arn:aws-cn:dynamodb:cn-north-1:1:table/t"
    );
    assert_eq!(ddb["table"]["items"][0]["pk"]["S"], role);
    assert_eq!(ddb["table"]["stream_records"][0]["keys"]["pk"]["S"], role);
    assert_eq!(
        ddb["table"]["stream_records"][0]["new_image"]["pk"]["S"],
        role
    );
    assert_eq!(ddb["table"]["backups"][0]["items"][0]["pk"]["S"], role);
    let ssm: Value = serde_json::from_str(&read(&ssm)).unwrap();
    assert_eq!(ssm["p"]["arn"], "arn:aws-cn:ssm:cn-north-1:1:parameter/p");
    assert_eq!(ssm["p"]["value"], role);
    assert_eq!(ssm["p"]["history"][0]["value"], role);
    let secrets: Value = serde_json::from_str(&read(&secrets)).unwrap();
    assert_eq!(
        secrets["s"]["arn"],
        "arn:aws-cn:secretsmanager:cn-north-1:1:secret:s"
    );
    assert_eq!(secrets["s"]["secret_string"], role);
}

#[test]
fn s3_object_keys_metadata_and_tags_are_kept_verbatim() {
    // Regression: rewriting the sidecar's `key` moved the object to a new key
    // while its files stayed in the directory derived from the old one.
    let tmp = legacy_dir();
    let dir = tmp.path();
    let object = dir.join("s3/buckets/b/objects/escaped-key/null.toml");
    write(
        &object,
        "key = \"exports/arn:aws:iam::1:role/x.json\"\n\
         sse_kms_key_id = \"arn:aws:kms:cn-north-1:1:key/k\"\n\
         last_modified = \"2026-01-01T00:00:00Z\"\n\
         part_sizes = [[1, 5]]\n\n\
         [metadata]\nsource = \"arn:aws:sns:cn-north-1:1:t\"\n\n\
         [tags]\nowner = \"arn:aws:iam::1:role/x\"\n",
    );
    let upload = dir.join("s3/buckets/b/mpu/u1/init.toml");
    write(
        &upload,
        "key = \"arn:aws:iam::1:role/x\"\nsse_kms_key_id = \"arn:aws:kms:cn-north-1:1:key/k\"\n",
    );
    let replication = dir.join("s3/buckets/b/replication.toml");
    write(
        &replication,
        "<ReplicationConfiguration><Role>arn:aws:iam::1:role/replicator</Role>\
         <Rule><Destination><Bucket>arn:aws:s3:::dest</Bucket></Destination></Rule>\
         </ReplicationConfiguration>",
    );
    let meta = dir.join("s3/buckets/b/meta.toml");
    let meta_text = "name = \"b\"\nregion = \"cn-north-1\"\n";
    write(&meta, meta_text);

    migrate_data_dir(dir, "cn-north-1").unwrap();

    let object: toml::Table = read(&object).parse().unwrap();
    assert_eq!(
        object["key"].as_str(),
        Some("exports/arn:aws:iam::1:role/x.json")
    );
    assert_eq!(
        object["sse_kms_key_id"].as_str(),
        Some("arn:aws-cn:kms:cn-north-1:1:key/k")
    );
    assert_eq!(
        object["last_modified"].as_str(),
        Some("2026-01-01T00:00:00Z")
    );
    assert_eq!(
        object["part_sizes"],
        toml::Value::Array(vec![toml::Value::Array(vec![1.into(), 5.into()])])
    );
    assert_eq!(
        object["metadata"]["source"].as_str(),
        Some("arn:aws:sns:cn-north-1:1:t")
    );
    assert_eq!(
        object["tags"]["owner"].as_str(),
        Some("arn:aws:iam::1:role/x")
    );
    let upload: toml::Table = read(&upload).parse().unwrap();
    assert_eq!(upload["key"].as_str(), Some("arn:aws:iam::1:role/x"));
    assert_eq!(
        upload["sse_kms_key_id"].as_str(),
        Some("arn:aws-cn:kms:cn-north-1:1:key/k")
    );
    assert_eq!(
        read(&replication),
        "<ReplicationConfiguration><Role>arn:aws-cn:iam::1:role/replicator</Role>\
         <Rule><Destination><Bucket>arn:aws-cn:s3:::dest</Bucket></Destination></Rule>\
         </ReplicationConfiguration>"
    );
    assert_eq!(read(&meta), meta_text);
}

#[test]
fn s3_object_keys_are_kept_verbatim_in_the_per_account_layout() {
    let tmp = legacy_dir();
    let dir = tmp.path();
    let bucket = dir.join("s3/accounts/123456789012/buckets/b");
    let object = bucket.join("objects/escaped-key/null.toml");
    write(
        &object,
        "key = \"exports/arn:aws:iam::1:role/x.json\"\n\
         sse_kms_key_id = \"arn:aws:kms:cn-north-1:1:key/k\"\n",
    );
    // A bucket named `objects` in the flat layout is a bucket, not an object
    // tree, whatever else the store holds.
    let flat_policy = dir.join("s3/buckets/objects/policy.toml");
    write(&flat_policy, "arn:aws:iam::1:role/x");
    let policy = bucket.join("policy.toml");
    write(&policy, "arn:aws:iam::1:role/x");

    migrate_data_dir(dir, "cn-north-1").unwrap();

    let object: toml::Table = read(&object).parse().unwrap();
    assert_eq!(
        object["key"].as_str(),
        Some("exports/arn:aws:iam::1:role/x.json")
    );
    assert_eq!(
        object["sse_kms_key_id"].as_str(),
        Some("arn:aws-cn:kms:cn-north-1:1:key/k")
    );
    assert_eq!(read(&policy), "arn:aws-cn:iam::1:role/x");
    assert_eq!(read(&flat_policy), "arn:aws-cn:iam::1:role/x");
}

#[cfg(unix)]
#[test]
fn unreadable_and_filesystem_artifact_directories_do_not_stop_startup() {
    // Regression: `read_dir` on a root-only `lost+found` at the root of a
    // volume failed the migration, and with it the server's first start.
    use std::os::unix::fs::PermissionsExt;
    let tmp = legacy_dir();
    let dir = tmp.path();
    let kms = dir.join("kms/snapshot.json");
    write(&kms, r#"{"arn":"arn:aws:kms:cn-north-1:1:key/k"}"#);
    let locked = [dir.join("lost+found"), dir.join("elasticache")];
    for path in &locked {
        std::fs::create_dir(path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).unwrap();
    }
    // Running as root ignores the mode; nothing to prove then.
    let enforced = std::fs::read_dir(&locked[1]).is_err();

    let report = migrate_data_dir(dir, "cn-north-1");
    for path in &locked {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let report = report.unwrap();
    assert_eq!(report.rewritten, vec![kms.clone()]);
    if enforced {
        // `lost+found` is never even opened; a service directory the server
        // cannot read is reported and left to its loader.
        assert_eq!(report.skipped, vec![locked[1].clone()]);
    }
    assert!(crate::version::arn_partitions_migrated(dir).unwrap());
}

#[test]
fn policy_and_template_variables_move_with_the_resources_they_match() {
    // Regression: `${...}` in the account or region field made the token
    // unparseable, so a stored `Resource` stopped matching the migrated role.
    let m = cn();
    for (legacy, migrated) in [
        (
            "arn:aws:iam::${aws:PrincipalAccount}:role/app",
            "arn:aws-cn:iam::${aws:PrincipalAccount}:role/app",
        ),
        (
            "arn:aws:sqs:${aws:RequestedRegion}:1:q",
            "arn:aws-cn:sqs:${aws:RequestedRegion}:1:q",
        ),
        (
            "arn:aws:iam::${AWS::AccountId}:role/x",
            "arn:aws-cn:iam::${AWS::AccountId}:role/x",
        ),
        (
            "arn:aws:sns:${AWS::Region}:${AWS::AccountId}:t",
            "arn:aws-cn:sns:${AWS::Region}:${AWS::AccountId}:t",
        ),
        (
            "arn:aws:sqs:cn-north-1:${AWS::AccountId}:q",
            "arn:aws-cn:sqs:cn-north-1:${AWS::AccountId}:q",
        ),
    ] {
        assert_eq!(rewrite(&m, legacy), migrated, "{legacy}");
        assert!(m.needs_rewrite(legacy.as_bytes()), "{legacy}");
    }
    // A variable region on a commercial server stays aws.
    assert_eq!(
        rewrite(&commercial(), "arn:aws:sqs:${aws:RequestedRegion}:1:q"),
        "arn:aws:sqs:${aws:RequestedRegion}:1:q"
    );
    // Not variables: unterminated, empty, or carrying other characters.
    for s in [
        "arn:aws:iam::${aws:PrincipalAccount:role/app",
        "arn:aws:iam::${}:role/app",
        "arn:aws:iam::${a b}:role/app",
    ] {
        assert_eq!(rewrite(&m, s), s, "{s}");
    }
}

#[test]
fn a_bucket_named_like_an_object_tree_keeps_text_rewrites() {
    // Regression: any directory called `objects`/`mpu` switched to the
    // field-by-field path, so a bucket with that name had its raw JSON/XML
    // configuration skipped as unparseable TOML, then marked migrated.
    let tmp = legacy_dir();
    let dir = tmp.path();
    let mut paths = Vec::new();
    for bucket in ["objects", "mpu"] {
        let policy = dir.join(format!("s3/buckets/{bucket}/policy.toml"));
        write(&policy, r#"{"Resource":"arn:aws:s3:::b/*"}"#);
        paths.push(policy);
    }
    let report = migrate_data_dir(dir, "cn-north-1").unwrap();
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    for policy in paths {
        assert_eq!(read(&policy), r#"{"Resource":"arn:aws-cn:s3:::b/*"}"#);
    }
}

#[test]
fn size_measured_payloads_are_kept_verbatim() {
    // Regression: dashboard bodies and archived events were rewritten while
    // the `size_bytes` stored next to them was not.
    let tmp = legacy_dir();
    let dir = tmp.path();
    let body = r#"{"widgets":[{"properties":{"title":"arn:aws:sqs:cn-north-1:1:q"}}]}"#;
    let cw = dir.join("cloudwatch/snapshot.json");
    write(
        &cw,
        &json!({"d": {"arn": "arn:aws:cloudwatch::1:dashboard/d", "body": body, "size_bytes": body.len()}})
            .to_string(),
    );
    let event = json!({"resources": ["arn:aws:sqs:cn-north-1:1:q"]});
    let eb = dir.join("eventbridge/snapshot.json");
    write(
        &eb,
        &json!({
            "events": [event],
            "archives": {"a": {"arn": "arn:aws:events:cn-north-1:1:archive/a", "size_bytes": 10, "events": [event]}}
        })
        .to_string(),
    );

    migrate_data_dir(dir, "cn-north-1").unwrap();

    let cw: Value = serde_json::from_str(&read(&cw)).unwrap();
    assert_eq!(cw["d"]["arn"], "arn:aws-cn:cloudwatch::1:dashboard/d");
    assert_eq!(cw["d"]["body"], body);
    let eb: Value = serde_json::from_str(&read(&eb)).unwrap();
    assert_eq!(
        eb["archives"]["a"]["arn"],
        "arn:aws-cn:events:cn-north-1:1:archive/a"
    );
    assert_eq!(eb["archives"]["a"]["events"][0], event);
    assert_eq!(eb["events"][0], event);
}

#[cfg(unix)]
#[test]
fn an_unreadable_s3_file_does_not_stop_other_buckets() {
    // Regression: the first permission error in `s3/` abandoned every later
    // bucket, and the directory was still marked migrated.
    use std::os::unix::fs::PermissionsExt;
    let tmp = legacy_dir();
    let dir = tmp.path();
    let locked = dir.join("s3/buckets/a/objects/k/null.toml");
    write(
        &locked,
        "sse_kms_key_id = \"arn:aws:kms:cn-north-1:1:key/k\"\n",
    );
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let enforced = std::fs::read(&locked).is_err();
    let later = dir.join("s3/buckets/b/policy.toml");
    write(&later, r#"{"Resource":"arn:aws:s3:::b/*"}"#);

    let report = migrate_data_dir(dir, "cn-north-1");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o644)).unwrap();
    let report = report.unwrap();
    assert_eq!(read(&later), r#"{"Resource":"arn:aws-cn:s3:::b/*"}"#);
    if enforced {
        assert_eq!(report.skipped, vec![locked]);
    }
}

#[test]
fn sent_emails_and_published_messages_are_kept_verbatim() {
    // Regression: SES bodies were rewritten under an unchanged DKIM
    // signature, and SNS messages diverged from the SQS copies of the same
    // publish (which are kept verbatim).
    let tmp = legacy_dir();
    let dir = tmp.path();
    let text = "alarm on arn:aws:sns:cn-north-1:1:t";
    let ses = dir.join("ses/snapshot.json");
    write(
        &ses,
        &json!({
            "identity_arn": "arn:aws:ses:cn-north-1:1:identity/example.com",
            "sent_emails": [{
                "subject": text, "text_body": text, "html_body": text, "raw_data": text,
                "template_data": text, "headers": [["X-Topic", text]], "dkim_signature": "bh=x"
            }]
        })
        .to_string(),
    );
    let sns = dir.join("sns/snapshot.json");
    write(
        &sns,
        &json!({
            "published": [{
                "topic_arn": "arn:aws:sns:cn-north-1:1:t", "message": text, "subject": text,
                "message_attributes": {"src": {"string_value": text}}
            }],
            "sms_messages": [["+15555550100", text]]
        })
        .to_string(),
    );

    migrate_data_dir(dir, "cn-north-1").unwrap();

    let ses: Value = serde_json::from_str(&read(&ses)).unwrap();
    assert_eq!(
        ses["identity_arn"],
        "arn:aws-cn:ses:cn-north-1:1:identity/example.com"
    );
    let email = &ses["sent_emails"][0];
    for field in [
        "subject",
        "text_body",
        "html_body",
        "raw_data",
        "template_data",
    ] {
        assert_eq!(email[field], text, "{field}");
    }
    assert_eq!(email["headers"][0][1], text);
    let sns: Value = serde_json::from_str(&read(&sns)).unwrap();
    let published = &sns["published"][0];
    assert_eq!(published["topic_arn"], "arn:aws-cn:sns:cn-north-1:1:t");
    assert_eq!(published["message"], text);
    assert_eq!(published["subject"], text);
    assert_eq!(published["message_attributes"]["src"]["string_value"], text);
    assert_eq!(sns["sms_messages"][0][1], text);
}

#[cfg(unix)]
#[test]
fn a_failed_rewrite_fails_the_migration_instead_of_being_skipped() {
    // Regression: a write failure (a readable but read-only service
    // directory) was skipped as "unreadable" and the directory still marked
    // migrated, so the loader served the stale ARNs for good.
    use std::os::unix::fs::PermissionsExt;
    let tmp = legacy_dir();
    let dir = tmp.path();
    let sqs_dir = dir.join("sqs");
    let snapshot = sqs_dir.join("snapshot.json");
    write(&snapshot, r#"{"arn":"arn:aws:sqs:cn-north-1:1:q"}"#);
    std::fs::set_permissions(&sqs_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = sqs_dir.join("probe");
    let enforced = std::fs::write(&probe, b"x").is_err();
    let _ = std::fs::remove_file(&probe);

    let result = migrate_data_dir(dir, "cn-north-1");
    std::fs::set_permissions(&sqs_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    if !enforced {
        return; // root ignores the mode
    }
    assert!(result.is_err(), "{result:?}");
    assert!(!crate::version::arn_partitions_migrated(dir).unwrap());
    // Once writable, the next start migrates it.
    migrate_data_dir(dir, "cn-north-1").unwrap();
    assert_eq!(
        read(&snapshot),
        r#"{"arn":"arn:aws-cn:sqs:cn-north-1:1:q"}"#
    );
}

#[test]
fn service_field_question_mark_wildcard_is_a_pattern() {
    // Regression: `?` was accepted in the region and account fields but not
    // the service field, so such a pattern kept matching only `arn:aws:`.
    let m = cn();
    assert_eq!(
        rewrite(&m, "arn:aws:sq?:cn-north-1:1:q"),
        "arn:aws-cn:sq?:cn-north-1:1:q"
    );
    assert_eq!(rewrite(&m, "arn:aws:s?:::b"), "arn:aws-cn:s?:::b");
}

#[test]
fn text_rewrite_accepts_an_arn_behind_an_escape() {
    // Regression: bucket subresource payloads are rewritten as raw text, where
    // `\narn:aws:...` puts `n` before the token. The pre-check accepted the
    // file but the rewrite skipped the token, and the directory was marked
    // migrated with the stale ARN.
    let m = cn();
    let raw = r#"{"Resource":["a\narn:aws:s3:::b/*","\tarn:aws:sns:cn-north-1:1:t"]}"#;
    assert_eq!(
        rewrite(&m, raw),
        r#"{"Resource":["a\narn:aws-cn:s3:::b/*","\tarn:aws-cn:sns:cn-north-1:1:t"]}"#
    );

    let tmp = legacy_dir();
    let dir = tmp.path();
    let policy = dir.join("s3/buckets/b/policy.toml");
    write(&policy, raw);
    migrate_data_dir(dir, "cn-north-1").unwrap();
    assert!(!read(&policy).contains("arn:aws:"), "{}", read(&policy));
    assert!(crate::version::arn_partitions_migrated(dir).unwrap());
}

#[test]
fn multipart_upload_tagging_and_object_headers_are_kept_verbatim() {
    // Regression: a multipart upload stores its tags as `tagging`, which was
    // rewritten although object `tags` were kept.
    let tmp = legacy_dir();
    let dir = tmp.path();
    let init = dir.join("s3/buckets/b/mpu/u1/init.toml");
    write(
        &init,
        "key = \"k\"\n\
         tagging = \"owner=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fx&src=arn:aws:sns:cn-north-1:1:t\"\n\
         content_disposition = \"attachment; filename=arn:aws:iam::1:role/x\"\n\
         sse_kms_key_id = \"arn:aws:kms:cn-north-1:1:key/k\"\n",
    );
    let object = dir.join("s3/buckets/b/objects/k/null.toml");
    write(
        &object,
        "key = \"k\"\n\
         website_redirect_location = \"/arn:aws:iam::1:role/x\"\n\
         sse_kms_key_id = \"arn:aws:kms:cn-north-1:1:key/k\"\n",
    );

    migrate_data_dir(dir, "cn-north-1").unwrap();

    let init: toml::Table = read(&init).parse().unwrap();
    assert_eq!(
        init["tagging"].as_str(),
        Some("owner=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fx&src=arn:aws:sns:cn-north-1:1:t")
    );
    assert_eq!(
        init["content_disposition"].as_str(),
        Some("attachment; filename=arn:aws:iam::1:role/x")
    );
    assert_eq!(
        init["sse_kms_key_id"].as_str(),
        Some("arn:aws-cn:kms:cn-north-1:1:key/k")
    );
    let object: toml::Table = read(&object).parse().unwrap();
    assert_eq!(
        object["website_redirect_location"].as_str(),
        Some("/arn:aws:iam::1:role/x")
    );
    assert_eq!(
        object["sse_kms_key_id"].as_str(),
        Some("arn:aws-cn:kms:cn-north-1:1:key/k")
    );
}
