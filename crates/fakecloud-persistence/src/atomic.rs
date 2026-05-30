use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

fn tmp_path(path: &Path) -> PathBuf {
    // Unique temp name per write. A fixed `<path>.tmp` let two concurrent
    // writers to the same path (e.g. the KMS snapshot_lock-guarded save and the
    // lock-free auto-provision snapshot hook firing from another worker)
    // truncate+write the SAME temp file and interleave their bytes, producing a
    // corrupt blob that fails to parse on restart -> KMS keys + all ciphertext
    // permanently lost (bug-audit 2026-05-28, 4.1). A process id + monotonic
    // counter make every in-flight temp distinct; the rename stays atomic.
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut os = path.as_os_str().to_owned();
    os.push(format!(".{}.{}.tmp", std::process::id(), seq));
    PathBuf::from(os)
}

fn fsync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}

fn write_atomic_bytes_inner(tmp: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(tmp, path)?;
    fsync_parent(path)?;
    Ok(())
}

pub fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    match write_atomic_bytes_inner(&tmp, path, bytes) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

pub fn write_atomic_toml<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let text = toml::to_string_pretty(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    write_atomic_bytes(path, text.as_bytes())
}

fn write_atomic_from_file_inner(src: &Path, dst: &Path) -> io::Result<()> {
    {
        let f = File::open(src)?;
        f.sync_all()?;
    }
    std::fs::rename(src, dst)?;
    fsync_parent(dst)?;
    Ok(())
}

pub fn write_atomic_from_file(src: &Path, dst: &Path) -> io::Result<()> {
    match write_atomic_from_file_inner(src, dst) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Best-effort cleanup: remove any stray tmp the caller might see.
            let tmp = tmp_path(dst);
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

fn write_atomic_copy_from_file_inner(tmp: &Path, src: &Path, dst: &Path) -> io::Result<()> {
    {
        let mut input = File::open(src)?;
        let mut out = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp)?;
        io::copy(&mut input, &mut out)?;
        out.sync_all()?;
    }
    std::fs::rename(tmp, dst)?;
    fsync_parent(dst)?;
    Ok(())
}

/// Copy `src` into `dst` atomically, leaving `src` untouched. Used by the
/// S3 store to replicate disk-backed object bodies without round-tripping
/// through RAM.
pub fn write_atomic_copy_from_file(src: &Path, dst: &Path) -> io::Result<()> {
    let tmp = tmp_path(dst);
    match write_atomic_copy_from_file_inner(&tmp, src, dst) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_write_leaves_no_tmp() {
        // Writing into a non-existent parent directory should fail without
        // leaving a lingering `.tmp` sibling. Use a tempdir so the test is
        // hermetic.
        let tmp = tempfile::tempdir().unwrap();
        let bogus = tmp.path().join("does/not/exist/target.bin");
        let err = write_atomic_bytes(&bogus, b"hello").unwrap_err();
        let tmp_sibling = tmp_path(&bogus);
        assert!(!tmp_sibling.exists(), "stray tmp: {:?}", tmp_sibling);
        let _ = err;
    }

    #[test]
    fn write_atomic_bytes_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.bin");
        write_atomic_bytes(&path, b"hello world").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    }

    #[test]
    fn write_atomic_bytes_overwrites() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.bin");
        write_atomic_bytes(&path, b"v1").unwrap();
        write_atomic_bytes(&path, b"v2").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"v2");
    }

    #[test]
    fn write_atomic_toml_round_trip() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Config {
            name: String,
            count: i64,
        }
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cfg.toml");
        let cfg = Config {
            name: "test".to_string(),
            count: 42,
        };
        write_atomic_toml(&path, &cfg).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("name"));
        assert!(content.contains("test"));
    }
}
