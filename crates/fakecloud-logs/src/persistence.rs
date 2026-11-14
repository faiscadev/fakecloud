//! Bounded-size append-only event segments with an atomic metadata manifest.
//! A manifest commits byte lengths only after event files are synced. Recovery
//! ignores uncommitted tails; the next writer truncates them before appending.
//!
//! A save runs in two phases so disk IO never happens under the Logs state
//! lock: `prepare` does memory-only work while holding the state write lock,
//! then `commit` writes segments and the manifest after the lock is released.
//! Both run under the store's own save lock, so saves stay serialized even when
//! the async task that started one is cancelled mid-commit.
use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use parking_lot::RwLock;

use crate::state::{LogEvent, LogGroup, LogsSnapshot, LogsState, LOGS_SNAPSHOT_SCHEMA_VERSION};
use fakecloud_core::multi_account::MultiRegionState;
use fakecloud_persistence::atomic::write_atomic_bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

type Accounts = MultiRegionState<LogsState>;
const SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
const DAY_MS: i64 = 86_400_000;
/// Per-event storage overhead CloudWatch Logs counts toward `storedBytes`.
pub(crate) const EVENT_OVERHEAD_BYTES: i64 = 26;

#[derive(Serialize)]
struct Manifest {
    version: u32,
    metadata: LogsSnapshot,
    streams: BTreeMap<String, StreamFiles>,
}

/// A manifest as read from disk: its metadata may predate the per-region
/// schema and is migrated by [`crate::state::parse_logs_snapshot`].
#[derive(Deserialize)]
struct RawManifest {
    version: u32,
    metadata: serde_json::Value,
    streams: BTreeMap<String, StreamFiles>,
}

impl RawManifest {
    fn migrate(self) -> io::Result<Manifest> {
        let metadata = serde_json::to_vec(&self.metadata).map_err(invalid)?;
        Ok(Manifest {
            version: self.version,
            metadata: crate::state::parse_logs_snapshot(&metadata).map_err(invalid)?,
            streams: self.streams,
        })
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct StreamFiles {
    max_sequence: u64,
    segments: Vec<Segment>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Segment {
    file: String,
    length: u64,
    max_timestamp: i64,
    #[serde(default)]
    expired_before: Option<i64>,
}

/// What the last successful commit put on disk.
struct Committed {
    manifest_exists: bool,
    streams: BTreeMap<String, StreamFiles>,
    /// Order-independent encoding of the committed manifest, used to skip
    /// rewriting an unchanged manifest.
    fingerprint: Vec<u8>,
    /// Legacy snapshot removal or segment GC has not completed since this view
    /// was loaded or last failed; the next commit must not take the idle skip.
    cleanup_pending: bool,
}

/// Memory-only result of `prepare`: event-free metadata plus, per stream, only
/// the events not yet committed to a segment.
struct PendingSave {
    metadata: LogsSnapshot,
    streams: BTreeMap<String, Vec<LogEvent>>,
}

pub struct SegmentedLogsStore {
    directory: PathBuf,
    committed: Mutex<Option<Committed>>,
    save_lock: Mutex<()>,
}

impl SegmentedLogsStore {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            committed: Mutex::new(None),
            save_lock: Mutex::new(()),
        }
    }

    fn read_manifest_bytes(&self) -> io::Result<Option<(Manifest, Vec<u8>)>> {
        let bytes = match std::fs::read(self.directory.join("manifest.json")) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let manifest = serde_json::from_slice::<RawManifest>(&bytes)
            .map_err(invalid)?
            .migrate()?;
        if manifest.version != 1 || manifest.metadata.schema_version > LOGS_SNAPSHOT_SCHEMA_VERSION
        {
            return Err(invalid(format!(
                "unsupported Logs manifest: on-disk version={} schema={}, max supported version=1 \
                 schema={LOGS_SNAPSHOT_SCHEMA_VERSION}",
                manifest.version, manifest.metadata.schema_version,
            )));
        }
        // Filenames are generated UUIDs, never API-provided group/stream names.
        for files in manifest.streams.values() {
            for segment in &files.segments {
                if !is_segment_file_name(&segment.file) {
                    return Err(invalid("invalid Logs segment filename"));
                }
            }
        }
        Ok(Some((manifest, bytes)))
    }

    #[cfg(test)]
    fn read_manifest(&self) -> io::Result<Option<Manifest>> {
        Ok(self.read_manifest_bytes()?.map(|(manifest, _)| manifest))
    }

    /// Load the on-disk state: the segmented manifest, or a legacy whole-state
    /// `snapshot.json` that the first save migrates. Expiration that happened
    /// while the server was down is committed before returning, so a caller
    /// that removes or extends a retention policy cannot resurrect rows.
    pub fn load(&self) -> io::Result<Option<LogsSnapshot>> {
        let Some((mut manifest, manifest_bytes)) = self.read_manifest_bytes()? else {
            let legacy = match File::open(self.directory.join("snapshot.json")) {
                Ok(file) => {
                    let mut bytes = Vec::new();
                    BufReader::new(file).read_to_end(&mut bytes)?;
                    crate::state::parse_logs_snapshot(&bytes).map_err(invalid)?
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            };
            *self.committed.lock() = Some(Committed {
                manifest_exists: false,
                streams: BTreeMap::new(),
                fingerprint: Vec::new(),
                cleanup_pending: true,
            });
            return Ok(Some(legacy));
        };
        let fingerprint = fingerprint(&manifest)?;
        drop(manifest_bytes);
        let accounts = manifest
            .metadata
            .accounts
            .as_mut()
            .ok_or_else(|| invalid("missing Logs accounts"))?;
        for (_, _, account) in accounts.iter_regional_mut() {
            for group in account.log_groups.values_mut() {
                for stream in group.log_streams.values_mut() {
                    if !stream.events.is_empty() {
                        return Err(invalid("events in Logs metadata"));
                    }
                    let files = manifest
                        .streams
                        .get(&stream.persistence_id)
                        .ok_or_else(|| invalid("missing Logs stream segments"))?;
                    for segment in &files.segments {
                        self.read_segment(segment, &mut stream.events)?;
                    }
                    stream.events.sort_by_key(|e| (e.timestamp, e.seq));
                }
            }
        }
        *self.committed.lock() = Some(Committed {
            manifest_exists: true,
            streams: std::mem::take(&mut manifest.streams),
            fingerprint,
            cleanup_pending: true,
        });
        self.save(accounts)?;
        Ok(Some(manifest.metadata))
    }

    fn read_segment(&self, segment: &Segment, events: &mut Vec<LogEvent>) -> io::Result<()> {
        let file = File::open(self.directory.join(&segment.file))?;
        if file.metadata()?.len() < segment.length {
            return Err(invalid("truncated committed Logs segment"));
        }
        let mut reader = BufReader::new(file.take(segment.length));
        let mut line = String::new();
        while reader.read_line(&mut line)? != 0 {
            if !line.ends_with('\n') {
                return Err(invalid("incomplete committed Logs event"));
            }
            let event: LogEvent = serde_json::from_str(&line).map_err(invalid)?;
            if segment
                .expired_before
                .is_none_or(|cutoff| event.timestamp >= cutoff)
            {
                events.push(event);
            }
            line.clear();
        }
        Ok(())
    }

    /// Save exclusively owned state (startup, tests).
    pub fn save(&self, state: &mut Accounts) -> io::Result<()> {
        let _save = self.save_lock.lock();
        let pending = self.prepare(state)?;
        self.commit(pending)
    }

    /// Save state shared with request handlers: the state write lock is held
    /// only for the memory-only prepare phase, never across disk IO. Blocking;
    /// call from a blocking-pool thread.
    pub fn save_shared(&self, state: &RwLock<Accounts>) -> io::Result<()> {
        let _save = self.save_lock.lock();
        let pending = self.prepare(&mut state.write())?;
        self.commit(pending)
    }

    /// Memory-only phase, run under the Logs state write lock: enforce
    /// retention, record durable expiration cutoffs, and copy only the events
    /// newer than each stream's committed sequence watermark. Sequence numbers
    /// only grow, so anything appended after this returns is picked up by the
    /// next save.
    fn prepare(&self, state: &mut Accounts) -> io::Result<PendingSave> {
        let now = chrono::Utc::now().timestamp_millis();
        let mut guard = self.committed.lock();
        if guard.is_none() {
            // First save without a prior `load`: read what is already on disk
            // once so existing segments are appended to, not replaced.
            *guard = Some(match self.read_manifest_bytes()? {
                Some((manifest, _)) => Committed {
                    manifest_exists: true,
                    fingerprint: fingerprint(&manifest)?,
                    streams: manifest.streams,
                    cleanup_pending: true,
                },
                None => Committed {
                    manifest_exists: false,
                    streams: BTreeMap::new(),
                    fingerprint: Vec::new(),
                    cleanup_pending: true,
                },
            });
        }
        let committed = guard.as_mut().expect("committed state initialized");
        let mut streams = BTreeMap::new();
        for (_, _, account) in state.iter_regional_mut() {
            let policies = crate::retention::parsed_policies(&account.account_policies);
            for group in account.log_groups.values_mut() {
                let cutoff = retention_cutoff(&policies, group, now);
                let expired = cutoff.map(|cutoff| (cutoff, prune_group(group, cutoff)));
                if !committed.manifest_exists {
                    // Older builds under-counted ingested and delivered events;
                    // normalize once during migration.
                    group.stored_bytes = group
                        .log_streams
                        .values()
                        .flat_map(|stream| &stream.events)
                        .map(|event| event.message.len() as i64 + EVENT_OVERHEAD_BYTES)
                        .sum();
                }
                for stream in group.log_streams.values_mut() {
                    // v1 snapshots may contain seq=0 or repeated IDs. Assign stable
                    // IDs once during migration; modern snapshots retain their IDs.
                    if !committed.manifest_exists {
                        renumber_legacy_sequences(stream);
                    }
                    let files = committed.streams.get_mut(&stream.persistence_id);
                    let watermark = files.as_ref().map_or(0, |files| files.max_sequence);
                    if let (Some((cutoff, pruned)), Some(files)) = (&expired, files) {
                        // Mark committed segments only when events were really
                        // removed; the mark stays in memory even if this commit
                        // fails, so a later policy removal cannot resurrect them.
                        if pruned.contains(&stream.persistence_id) {
                            files.segments.retain(|s| s.max_timestamp >= *cutoff);
                            for segment in &mut files.segments {
                                segment.expired_before =
                                    Some(segment.expired_before.unwrap_or(i64::MIN).max(*cutoff));
                            }
                        }
                    }
                    let new_events: Vec<LogEvent> = stream
                        .events
                        .iter()
                        .filter(|event| event.seq > watermark)
                        .cloned()
                        .collect();
                    streams.insert(stream.persistence_id.clone(), new_events);
                }
            }
        }
        Ok(PendingSave {
            metadata: LogsSnapshot {
                schema_version: LOGS_SNAPSHOT_SCHEMA_VERSION,
                accounts: Some(state.map(|account| account.map(LogsState::metadata))),
                state: None,
            },
            streams,
        })
    }

    /// Disk phase, run without the Logs state lock: append new events, sync,
    /// then atomically commit the manifest. A failure leaves the committed
    /// view untouched, so the next save retries the same events.
    fn commit(&self, pending: PendingSave) -> io::Result<()> {
        let (mut streams, skip_if_unchanged, previous_fingerprint) = {
            let guard = self.committed.lock();
            let committed = guard
                .as_ref()
                .ok_or_else(|| invalid("Logs save committed before prepare"))?;
            let streams: BTreeMap<String, StreamFiles> = pending
                .streams
                .keys()
                .map(|id| {
                    let files = committed.streams.get(id).cloned().unwrap_or_default();
                    (id.clone(), files)
                })
                .collect();
            (
                streams,
                committed.manifest_exists && !committed.cleanup_pending,
                committed.fingerprint.clone(),
            )
        };
        std::fs::create_dir_all(&self.directory)?;
        for (id, events) in &pending.streams {
            if !events.is_empty() {
                let files = streams.get_mut(id).expect("stream files cloned above");
                self.append_events(files, events)?;
            }
        }
        let manifest = Manifest {
            version: 1,
            metadata: pending.metadata,
            streams: std::mem::take(&mut streams),
        };
        let fingerprint = fingerprint(&manifest)?;
        if skip_if_unchanged && fingerprint == previous_fingerprint {
            // Idle sweep or read-only effect: nothing new to make durable.
            return Ok(());
        }
        let bytes = serde_json::to_vec(&manifest).map_err(invalid)?;
        // Sync directory entries for new segments before committing references.
        File::open(&self.directory)?.sync_all()?;
        let manifest_path = self.directory.join("manifest.json");
        if let Err(error) = write_atomic_bytes(&manifest_path, &bytes) {
            // The rename may have landed before a later step (the parent
            // directory sync) failed. The manifest on disk is then the new one,
            // and keeping the old committed view would make the next append
            // truncate bytes that manifest already references.
            if std::fs::read(&manifest_path).ok().as_deref() != Some(bytes.as_slice()) {
                return Err(error);
            }
            tracing::warn!(%error, "Logs manifest committed but not fully synced");
        }
        let live: HashSet<String> = manifest
            .streams
            .values()
            .flat_map(|s| &s.segments)
            .map(|s| s.file.clone())
            .collect();
        // Once the manifest is durable, the old whole-state file is obsolete.
        // Keeping it would retain data deleted by retention indefinitely.
        let mut cleanup_pending = false;
        if let Err(error) = std::fs::remove_file(self.directory.join("snapshot.json")) {
            if error.kind() != io::ErrorKind::NotFound {
                tracing::warn!(%error, "failed to remove migrated Logs snapshot");
                cleanup_pending = true;
            }
        }
        // Cleanup is after commit: crashes can leak files, never committed data.
        if let Err(e) = self.collect_unreferenced(&live) {
            tracing::warn!(error = %e, "failed to reclaim unreferenced Logs segments");
            cleanup_pending = true;
        }
        *self.committed.lock() = Some(Committed {
            manifest_exists: true,
            streams: manifest.streams,
            fingerprint,
            cleanup_pending,
        });
        Ok(())
    }

    fn append_events(&self, files: &mut StreamFiles, events: &[LogEvent]) -> io::Result<()> {
        let mut writer: Option<BufWriter<File>> = None;
        for event in events {
            let rotate = files.segments.last().is_none_or(|s| {
                s.length >= SEGMENT_BYTES
                    || s.expired_before
                        .is_some_and(|cutoff| event.timestamp < cutoff)
            });
            if rotate {
                if let Some(mut writer) = writer.take() {
                    writer.flush()?;
                    writer.get_ref().sync_all()?;
                }
                files.segments.push(Segment {
                    file: format!("{}.jsonl", uuid::Uuid::new_v4()),
                    length: 0,
                    max_timestamp: i64::MIN,
                    expired_before: None,
                });
            }
            let segment = files.segments.last_mut().expect("segment created");
            if writer.is_none() {
                let path = self.directory.join(&segment.file);
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(path)?;
                if file.metadata()?.len() < segment.length {
                    return Err(invalid("truncated committed Logs segment"));
                }
                file.set_len(segment.length)?;
                file.seek(SeekFrom::Start(segment.length))?;
                writer = Some(BufWriter::new(file));
            }
            // At most one event-sized temporary buffer, never historical state.
            let bytes = serde_json::to_vec(event).map_err(invalid)?;
            let writer = writer.as_mut().expect("writer opened");
            writer.write_all(&bytes)?;
            writer.write_all(b"\n")?;
            segment.length += bytes.len() as u64 + 1;
            segment.max_timestamp = segment.max_timestamp.max(event.timestamp);
            files.max_sequence = files.max_sequence.max(event.seq);
        }
        if let Some(mut writer) = writer {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    fn collect_unreferenced(&self, live: &HashSet<String>) -> io::Result<()> {
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if is_segment_file_name(&name) && !live.contains(name.as_ref()) {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

/// Retention is a storage policy, not merely a query filter. Used directly in
/// memory mode; persistent saves apply it inside [`SegmentedLogsStore::prepare`].
pub fn prune_expired(state: &mut Accounts, now: i64) {
    for (_, _, account) in state.iter_regional_mut() {
        let policies = crate::retention::parsed_policies(&account.account_policies);
        for group in account.log_groups.values_mut() {
            if let Some(cutoff) = retention_cutoff(&policies, group, now) {
                prune_group(group, cutoff);
            }
        }
    }
}

/// Expiry cutoff from the group's effective retention: an applicable
/// account-level retention policy overrides the group's own setting.
fn retention_cutoff(
    policies: &[crate::retention::ParsedPolicy],
    group: &LogGroup,
    now: i64,
) -> Option<i64> {
    crate::retention::effective_with(policies, group)
        .map(|days| now.saturating_sub(i64::from(days).saturating_mul(DAY_MS)))
}

/// Drop events older than `cutoff`, returning the persistence ids of streams
/// that lost at least one event. The sequence high-water mark survives even
/// when every event is removed.
fn prune_group(group: &mut LogGroup, cutoff: i64) -> HashSet<String> {
    let mut pruned = HashSet::new();
    let mut removed_bytes = 0i64;
    for stream in group.log_streams.values_mut() {
        let mut max_removed_seq = 0u64;
        let before = stream.events.len();
        stream.events.retain(|event| {
            if event.timestamp >= cutoff {
                return true;
            }
            max_removed_seq = max_removed_seq.max(event.seq);
            removed_bytes += event.message.len() as i64 + EVENT_OVERHEAD_BYTES;
            false
        });
        if stream.events.len() < before {
            stream.last_sequence = stream.last_sequence.max(max_removed_seq);
            stream.events.shrink_to_fit();
            pruned.insert(stream.persistence_id.clone());
        }
    }
    group.stored_bytes = group.stored_bytes.saturating_sub(removed_bytes).max(0);
    pruned
}

fn renumber_legacy_sequences(stream: &mut crate::state::LogStream) {
    let mut seen = HashSet::new();
    for event in &mut stream.events {
        stream.last_sequence = stream.last_sequence.max(event.seq);
    }
    for event in &mut stream.events {
        if event.seq == 0 || !seen.insert(event.seq) {
            stream.last_sequence += 1;
            event.seq = stream.last_sequence;
            seen.insert(event.seq);
        }
    }
}

/// Manifest encoding that does not depend on the account map's hash order.
fn fingerprint(manifest: &Manifest) -> io::Result<Vec<u8>> {
    let mut accounts: Vec<(&str, &str, &LogsState)> = manifest
        .metadata
        .accounts
        .as_ref()
        .map(|accounts| accounts.iter_regional().collect())
        .unwrap_or_default();
    accounts.sort_by_key(|(account, region, _)| (*account, *region));
    serde_json::to_vec(&(accounts, &manifest.streams)).map_err(invalid)
}

fn is_segment_file_name(name: &str) -> bool {
    name.strip_suffix(".jsonl")
        .is_some_and(|stem| uuid::Uuid::parse_str(stem).is_ok())
}

fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
#[path = "persistence_tests.rs"]
mod tests;
