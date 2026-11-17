//! One-time migration of `arn:aws:` ARNs persisted before fakecloud minted
//! ARNs in the region's partition.
//!
//! Before the ARN partition work, every ARN fakecloud produced used the `aws`
//! partition, so a server run with a China, GovCloud or ISO `--region` persisted
//! values such as `arn:aws:kms:cn-north-1:...` and `arn:aws:iam::...:role/r`.
//! Current code mints `arn:aws-cn:...`, so without a migration responses mix
//! both forms and ARN-keyed state (SNS topics, tag maps, ...) is only reachable
//! through the stale spelling.
//!
//! [`migrate_data_dir`] rewrites those ARNs once per data directory, before any
//! service loads its state, and records that it ran in the directory's
//! `fakecloud.version.toml` so it never runs again.
//!
//! What is rewritten is one rule, applied to every ARN token in the persisted
//! state (values, object keys, and ARNs embedded in stored documents such as
//! IAM/bucket/key policies, CloudFormation templates or state-machine
//! definitions, rewritten in place so the document text is otherwise
//! untouched):
//!
//! * A regional ARN (`arn:aws:<svc>:<region>:...`) whose region belongs to a
//!   non-`aws` partition takes that partition. Such an ARN never names a real
//!   resource, so this holds whatever region the server runs in.
//! * A region-less ARN (`arn:aws:iam::...`, `arn:aws:s3:::bucket`) carries no
//!   partition of its own. Current code mints those in the partition of the
//!   server's `--region`, so they are rewritten only when that partition is not
//!   `aws`. AWS-owned ones (account `aws`, i.e. managed policies
//!   `arn:aws:iam::aws:policy/...`) stay as they are: fakecloud's managed-policy
//!   catalog keeps the `aws` spelling in every partition.
//!
//! Payloads are left alone. Some are verified against a stored content hash:
//! rewriting an SQS message body would break the `MD5OfBody` the SDKs check,
//! and an ECR manifest is addressed by its digest. Others are customer data
//! the service stores rather than describes, where a rewrite would change
//! what the application wrote or break a lookup by it: DynamoDB items, SSM
//! parameter values, Secrets Manager secrets, SES email template content, SNS
//! published messages, EventBridge events, CloudWatch dashboard bodies, S3 object keys, user metadata and object tags, log
//! events. Bulk payloads streamed outside the snapshots
//! (S3 object bodies, CloudWatch Logs event segments, container data volumes)
//! are not read at all.

use std::borrow::Cow;
use std::io;
use std::path::{Path, PathBuf};

use fakecloud_aws::arn::partition_for;
use serde_json::{Map, Value};

const ARN_PREFIX: &str = "arn:aws:";

/// How ARNs are migrated: the partition region-less ARNs move to, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArnPartitionMigration {
    /// The server region's partition. Region-less ARNs are rewritten to it
    /// when it is not `aws`.
    global_partition: &'static str,
}

impl ArnPartitionMigration {
    /// The migration for a server started with `--region server_region`.
    pub fn for_server_region(server_region: &str) -> Self {
        Self {
            global_partition: partition_for(server_region),
        }
    }

    /// The partition the ARN starting at `arn:aws:` + `rest` moves to, or
    /// `None` when it stays in `aws` (or is not an ARN at all).
    ///
    /// Policy wildcards (`*`, `?`) and `${...}` variables are accepted in the
    /// service, region and account fields, so a stored policy's patterns move
    /// with the resources they match: a region pattern that pins a partition
    /// (`cn-*`) takes it, one that does not (`*`, `${aws:RequestedRegion}`) is
    /// treated like a region-less ARN.
    fn target_partition(&self, rest: &[u8]) -> Option<&'static str> {
        let (service, rest) = split_field(rest, |b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'*' | b'?')
        })?;
        if service.is_empty() {
            return None;
        }
        let (region, rest) = split_field(rest, |b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'*' | b'?')
        })?;
        let (account, _) = split_field(rest, |b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'*' | b'?')
        })?;
        // The field was checked to be ASCII.
        let region = std::str::from_utf8(region).ok()?;
        let wildcard = region.contains(['*', '?']) || region.contains("${");
        match partition_for(region) {
            "aws" if region.is_empty() || wildcard => (self.global_partition != "aws"
                && account != b"aws")
                .then_some(self.global_partition),
            "aws" => None,
            partition => Some(partition),
        }
    }

    /// `text` with every ARN token that belongs to another partition
    /// rewritten, borrowed when nothing changes. A token must start the string
    /// or follow a non-alphanumeric byte (`"`, `/`, `:`, whitespace, ...) or
    /// the tail of a backslash escape (`\narn:aws:...` in raw JSON/XML text),
    /// and look like `arn:aws:<service>:<region>:<account>:` -- free text that
    /// merely contains `arn:aws` is left alone.
    pub fn rewrite_str<'a>(&self, text: &'a str) -> Cow<'a, str> {
        let bytes = text.as_bytes();
        let mut out: Option<String> = None;
        let mut copied = 0;
        let mut from = 0;
        while let Some(found) = text[from..].find(ARN_PREFIX) {
            let start = from + found;
            from = start + ARN_PREFIX.len();
            if !at_token_start(bytes, start) {
                continue;
            }
            let Some(partition) = self.target_partition(&bytes[from..]) else {
                continue;
            };
            let buf = out.get_or_insert_with(|| String::with_capacity(text.len() + 16));
            // Replace the `aws` between `arn:` and the following `:`.
            buf.push_str(&text[copied..start + "arn:".len()]);
            buf.push_str(partition);
            copied = start + "arn:aws".len();
        }
        match out {
            Some(mut buf) => {
                buf.push_str(&text[copied..]);
                Cow::Owned(buf)
            }
            None => Cow::Borrowed(text),
        }
    }

    /// Whether `bytes` may hold an ARN token [`Self::rewrite_str`] would
    /// rewrite: a cheap pre-check before parsing a file. Serialized JSON and
    /// TOML spell ARNs verbatim (none of their characters is escaped), but the
    /// byte before one may be the tail of an escape (`\narn:aws:...`), so
    /// unlike [`Self::rewrite_str`] this does not require a token boundary.
    /// It can only err towards parsing a file that needs nothing.
    pub fn needs_rewrite(&self, bytes: &[u8]) -> bool {
        let prefix = ARN_PREFIX.as_bytes();
        let mut from = 0;
        while let Some(found) = find_bytes(&bytes[from..], prefix) {
            from += found + prefix.len();
            if self.target_partition(&bytes[from..]).is_some() {
                return true;
            }
        }
        false
    }

    /// Rewrite every string and object key in `value`, except the values of
    /// keys named in `opaque_keys`, which are kept verbatim. Returns whether
    /// anything changed.
    ///
    /// When a rewritten object key collides with a key already spelled in the
    /// target partition, the existing entry wins: it is the one current code
    /// created and has been serving, the legacy one is the stale duplicate.
    pub fn rewrite_json(&self, value: &mut Value, opaque_keys: &[&str]) -> bool {
        match value {
            Value::String(s) => match self.rewrite_str(s) {
                Cow::Owned(new) => {
                    *s = new;
                    true
                }
                Cow::Borrowed(_) => false,
            },
            Value::Array(items) => {
                let mut changed = false;
                for item in items {
                    changed |= self.rewrite_json(item, opaque_keys);
                }
                changed
            }
            Value::Object(map) => self.rewrite_object(map, opaque_keys),
            Value::Null | Value::Bool(_) | Value::Number(_) => false,
        }
    }

    fn rewrite_object(&self, map: &mut Map<String, Value>, opaque_keys: &[&str]) -> bool {
        let mut changed = false;
        let mut entries = Vec::with_capacity(map.len());
        for (key, mut value) in std::mem::take(map) {
            if !opaque_keys.contains(&key.as_str()) {
                changed |= self.rewrite_json(&mut value, opaque_keys);
            }
            let new_key = match self.rewrite_str(&key) {
                Cow::Owned(new) => Some(new),
                Cow::Borrowed(_) => None,
            };
            entries.push((key, new_key, value));
        }
        let kept: std::collections::HashSet<&str> = entries
            .iter()
            .filter(|(_, new_key, _)| new_key.is_none())
            .map(|(key, _, _)| key.as_str())
            .collect();
        let dropped: Vec<bool> = entries
            .iter()
            .map(|(_, new_key, _)| new_key.as_deref().is_some_and(|k| kept.contains(k)))
            .collect();
        for ((key, new_key, value), dropped) in entries.into_iter().zip(dropped) {
            match new_key {
                None => {
                    map.insert(key, value);
                }
                Some(new_key) if dropped => {
                    tracing::warn!(
                        legacy = %key,
                        current = %new_key,
                        "dropping legacy arn:aws: entry shadowed by an entry already in the region's partition"
                    );
                    changed = true;
                }
                Some(new_key) => {
                    map.insert(new_key, value);
                    changed = true;
                }
            }
        }
        changed
    }
}

/// Whether an ARN token starting at `start` begins a token: at the start of
/// the text, after a non-alphanumeric byte, or after a backslash escape such
/// as `\n` / `\t`, which raw (still-escaped) JSON or XML text puts right
/// before a value's first character.
fn at_token_start(bytes: &[u8], start: usize) -> bool {
    match start {
        0 => true,
        1 => !bytes[0].is_ascii_alphanumeric(),
        _ => !bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 2] == b'\\',
    }
}

/// Split `bytes` at the first `:`, requiring every byte before it to satisfy
/// `allowed` or belong to a `${...}` variable (an IAM policy variable such as
/// `${aws:PrincipalAccount}`, or a CloudFormation `Fn::Sub` reference such as
/// `${AWS::AccountId}`, whose own `:`s do not end the field). `None` when a
/// disallowed byte comes first or there is no `:`.
fn split_field(bytes: &[u8], allowed: impl Fn(u8) -> bool) -> Option<(&[u8], &[u8])> {
    /// Longest variable accepted; anything longer is not a policy variable.
    const MAX_VARIABLE: usize = 128;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b':' => return Some((&bytes[..i], &bytes[i + 1..])),
            b'$' if bytes.get(i + 1) == Some(&b'{') => {
                let close = bytes[i + 2..]
                    .iter()
                    .take(MAX_VARIABLE)
                    .position(|&b| b == b'}')?;
                let name = &bytes[i + 2..i + 2 + close];
                if name.is_empty()
                    || !name.iter().all(|&b| {
                        b.is_ascii_alphanumeric() || matches!(b, b':' | b'_' | b'-' | b'.' | b'/')
                    })
                {
                    return None;
                }
                i += close + 3;
            }
            b if allowed(b) => i += 1,
            _ => return None,
        }
    }
    None
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Keys whose values are kept verbatim, per service data directory: payloads
/// verified against a stored content hash, and customer data the service
/// stores rather than describes. See the module docs.
fn opaque_keys_for(service_dir: &str) -> &'static [&'static str] {
    match service_dir {
        // `md5_of_body` / `MD5OfMessageAttributes` are checked by the SDKs.
        "sqs" => &["body", "message_attributes"],
        // Manifests are addressed by their sha256 digest.
        "ecr" => &["image_manifest"],
        // Log events are bulk payloads; the segmented store never rewrites
        // them, and a legacy whole-state snapshot is treated the same.
        "logs" => &["message"],
        // Table and backup rows, and stream record images: an item keyed by
        // an ARN string must stay reachable by the key the application wrote.
        "dynamodb" => &["items", "keys", "new_image", "old_image"],
        // Parameter values (current and every version).
        "ssm" => &["value"],
        "secretsmanager" => &["secret_string"],
        // Customer-written email content: templates (persisted), and sent
        // emails, whose DKIM signature covers body and headers, should they
        // ever be persisted.
        "ses" => &[
            "subject",
            "text_body",
            "html_body",
            "raw_data",
            "template_data",
            "headers",
        ],
        // Published messages, kept as the SQS copies of the same publish are.
        "sns" => &["message", "subject", "message_attributes", "sms_messages"],
        // Published events (the bus log and archives): customer payloads,
        // and an archive's `size_bytes` measures them.
        "eventbridge" => &["events"],
        // Dashboard bodies: customer documents measured by `size_bytes`.
        "cloudwatch" => &["body"],
        _ => &[],
    }
}

/// Object and multipart-upload sidecar fields that are customer data: the
/// object key (its directory on disk is derived from it, so rewriting the
/// field would strand the object), user metadata, object tags and the
/// client-supplied object headers.
const S3_OBJECT_OPAQUE_KEYS: &[&str] = &[
    "key",
    "metadata",
    "tags",
    // A multipart upload's tags, as the `x-amz-tagging` query string.
    "tagging",
    // Headers the client supplied with the object, returned verbatim.
    "content_type",
    "content_encoding",
    "cache_control",
    "content_disposition",
    "content_language",
    "expires",
    "website_redirect_location",
];

/// What [`migrate_data_dir`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// The directory was already migrated (or created by a binary that mints
    /// partition-aware ARNs); nothing was read.
    pub already_migrated: bool,
    /// Files whose contents were rewritten.
    pub rewritten: Vec<PathBuf>,
    /// Files that held candidate ARNs but could not be parsed, and
    /// directories that could not be read; left untouched for the owning
    /// service's loader to report.
    pub skipped: Vec<PathBuf>,
}

/// Run the ARN partition migration over the persistence directory `dir` for
/// a server started with `--region server_region`, then mark the directory
/// migrated. A no-op when the directory is already marked.
///
/// Covered files: every service's JSON state directly under its directory
/// (`<service>/snapshot.json`, `logs/manifest.json`, ...) and the S3 metadata
/// sidecars (`s3/**/*.toml`: bucket configuration including policies,
/// notifications, replication and encryption, object metadata). Each file is
/// replaced atomically, and the marker is written only after all of them, so
/// an interrupted migration simply runs again (the rewrite is idempotent).
pub fn migrate_data_dir(
    dir: &Path,
    server_region: &str,
) -> Result<MigrationReport, crate::version::VersionError> {
    if crate::version::arn_partitions_migrated(dir)? {
        return Ok(MigrationReport {
            already_migrated: true,
            ..MigrationReport::default()
        });
    }
    let migration = ArnPartitionMigration::for_server_region(server_region);
    let mut report = MigrationReport::default();
    let io_err = |path: &Path| {
        let path = path.to_path_buf();
        move |source: io::Error| crate::version::VersionError::Io { path, source }
    };

    for entry in read_dir_sorted(dir).map_err(io_err(dir))? {
        let Some(name) = entry.file_name() else {
            continue;
        };
        // `lost+found`, `.snapshot` and dotfiles are filesystem artifacts,
        // often unreadable to a non-root server; they hold no state.
        if crate::version::is_benign_entry(name) || !entry.is_dir() {
            continue;
        }
        let Some(service_dir) = name.to_str() else {
            continue;
        };
        let result = if service_dir == "s3" {
            migrate_s3_sidecars(&migration, &entry, 0, false, &mut report)
        } else {
            migrate_service_dir(
                &migration,
                &entry,
                opaque_keys_for(service_dir),
                &mut report,
            )
        };
        skip_unreadable(result, &entry, &mut report).map_err(io_err(&entry))?;
    }

    crate::version::mark_arn_partitions_migrated(dir)?;
    Ok(report)
}

fn migrate_service_dir(
    migration: &ArnPartitionMigration,
    dir: &Path,
    opaque_keys: &[&str],
    report: &mut MigrationReport,
) -> io::Result<()> {
    for file in read_dir_sorted(dir)? {
        if file.is_file() && file.extension().is_some_and(|e| e == "json") {
            let result = migrate_json_file(migration, &file, opaque_keys, report);
            skip_unreadable(result, &file, report)?;
        }
    }
    Ok(())
}

/// Replace `path` with its migrated contents. A failure here is never a
/// skippable "unreadable" file: the loader would read the stale file fine and
/// the marker would stop the migration from ever running again, so it fails
/// the migration (and startup) instead, whatever its kind.
fn write_migrated(path: &Path, bytes: &[u8]) -> io::Result<()> {
    crate::atomic::write_atomic_bytes(path, bytes)
        .map_err(|err| io::Error::other(format!("writing {}: {err}", path.display())))
}

/// Turn a permission error on `path` into a skipped entry. A file or
/// directory the server cannot read is one its own loader cannot read either
/// (the S3 loader isolates such failures per bucket); that loader reports it,
/// and everything else is still migrated.
fn skip_unreadable(
    result: io::Result<()>,
    path: &Path,
    report: &mut MigrationReport,
) -> io::Result<()> {
    match result {
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            tracing::warn!(
                path = %path.display(),
                "skipping ARN partition migration of an unreadable path: {err}"
            );
            report.skipped.push(path.to_path_buf());
            Ok(())
        }
        other => other,
    }
}

fn read_dir_sorted(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut entries = std::fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    entries.sort();
    Ok(entries)
}

fn migrate_json_file(
    migration: &ArnPartitionMigration,
    path: &Path,
    opaque_keys: &[&str],
    report: &mut MigrationReport,
) -> io::Result<()> {
    let bytes = std::fs::read(path)?;
    if !migration.needs_rewrite(&bytes) {
        return Ok(());
    }
    let mut value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                "skipping ARN partition migration of an unparseable file: {err}"
            );
            report.skipped.push(path.to_path_buf());
            return Ok(());
        }
    };
    if !migration.rewrite_json(&mut value, opaque_keys) {
        return Ok(());
    }
    let out = serde_json::to_vec(&value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    write_migrated(path, &out)?;
    report.rewritten.push(path.to_path_buf());
    Ok(())
}

/// Rewrite the S3 store's sidecars under `dir`.
///
/// Bucket-level files (`meta.toml`, and the subresource files such as
/// `policy.toml` / `notification.toml`, which hold the configuration payload
/// verbatim -- JSON, XML or TOML) spell ARNs as plain text and are rewritten
/// as text. Below a bucket's `objects/` and `mpu/` directories each sidecar is
/// an `ObjectMeta`-style TOML table whose [`S3_OBJECT_OPAQUE_KEYS`] must stay
/// verbatim, so those are rewritten structurally. Object bodies (`.bin`) are
/// never read.
fn migrate_s3_sidecars(
    migration: &ArnPartitionMigration,
    dir: &Path,
    depth: usize,
    in_objects: bool,
    report: &mut MigrationReport,
) -> io::Result<()> {
    for path in read_dir_sorted(dir)? {
        let result = if path.is_dir() {
            // `s3/buckets/<bucket>/{objects,mpu}`, or
            // `s3/accounts/<account>/buckets/<bucket>/{objects,mpu}` in the
            // per-account layout: only that level names the object trees (a
            // bucket may itself be called `objects`).
            let objects_depth = if under_accounts(dir, depth) { 5 } else { 3 };
            let objects = in_objects
                || (depth + 1 == objects_depth
                    && path
                        .file_name()
                        .is_some_and(|n| n == "objects" || n == "mpu"));
            migrate_s3_sidecars(migration, &path, depth + 1, objects, report)
        } else if path.extension().is_some_and(|e| e == "toml") {
            migrate_s3_sidecar(migration, &path, in_objects, report)
        } else {
            Ok(())
        };
        skip_unreadable(result, &path, report)?;
    }
    Ok(())
}

/// Whether `dir`, `depth` levels below the `s3` directory, lies in the
/// per-account layout (`s3/accounts/...`).
fn under_accounts(dir: &Path, depth: usize) -> bool {
    dir.ancestors()
        .nth(depth.saturating_sub(1))
        .is_some_and(|top| depth > 0 && top.file_name().is_some_and(|n| n == "accounts"))
}

fn migrate_s3_sidecar(
    migration: &ArnPartitionMigration,
    path: &Path,
    in_objects: bool,
    report: &mut MigrationReport,
) -> io::Result<()> {
    let bytes = std::fs::read(path)?;
    if !migration.needs_rewrite(&bytes) {
        return Ok(());
    }
    let Ok(text) = std::str::from_utf8(&bytes) else {
        report.skipped.push(path.to_path_buf());
        return Ok(());
    };
    let rewritten = if in_objects {
        let Ok(mut table) = text.parse::<toml::Table>() else {
            tracing::warn!(
                path = %path.display(),
                "skipping ARN partition migration of an unparseable file"
            );
            report.skipped.push(path.to_path_buf());
            return Ok(());
        };
        if !migration.rewrite_toml_table(&mut table, S3_OBJECT_OPAQUE_KEYS) {
            return Ok(());
        }
        toml::to_string_pretty(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
    } else {
        match migration.rewrite_str(text) {
            Cow::Owned(new) => new,
            Cow::Borrowed(_) => return Ok(()),
        }
    };
    write_migrated(path, rewritten.as_bytes())?;
    report.rewritten.push(path.to_path_buf());
    Ok(())
}

impl ArnPartitionMigration {
    /// [`Self::rewrite_json`] for a TOML table: strings, keys, arrays and
    /// nested tables, except the values of `opaque_keys`.
    fn rewrite_toml_table(&self, table: &mut toml::Table, opaque_keys: &[&str]) -> bool {
        let mut changed = false;
        let mut renamed = Vec::new();
        for (key, mut value) in std::mem::take(table) {
            if !opaque_keys.contains(&key.as_str()) {
                changed |= self.rewrite_toml(&mut value, opaque_keys);
            }
            match self.rewrite_str(&key) {
                Cow::Owned(new) => renamed.push((new, value)),
                Cow::Borrowed(_) => {
                    table.insert(key, value);
                }
            }
        }
        // As in `rewrite_object`: an entry already in the target partition
        // wins over the legacy one it collides with.
        for (key, value) in renamed {
            changed = true;
            table.entry(key).or_insert(value);
        }
        changed
    }

    fn rewrite_toml(&self, value: &mut toml::Value, opaque_keys: &[&str]) -> bool {
        match value {
            toml::Value::String(s) => match self.rewrite_str(s) {
                Cow::Owned(new) => {
                    *s = new;
                    true
                }
                Cow::Borrowed(_) => false,
            },
            toml::Value::Array(items) => {
                let mut changed = false;
                for item in items {
                    changed |= self.rewrite_toml(item, opaque_keys);
                }
                changed
            }
            toml::Value::Table(table) => self.rewrite_toml_table(table, opaque_keys),
            toml::Value::Integer(_)
            | toml::Value::Float(_)
            | toml::Value::Boolean(_)
            | toml::Value::Datetime(_) => false,
        }
    }
}

#[cfg(test)]
#[path = "arn_partition_tests.rs"]
mod tests;
