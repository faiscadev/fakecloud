use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const FORMAT_VERSION: u32 = 1;
pub const VERSION_FILE_NAME: &str = "fakecloud.version.toml";

/// Filesystem artifacts that may exist in an otherwise-empty data directory and
/// must not count toward the "non-empty" emptiness check — e.g. ext4 always creates
/// `lost+found` at the root of a freshly formatted volume, and NetApp exposes
/// `.snapshot`. Dot-prefixed names are also ignored (see [`is_benign_entry`]).
const IGNORED_DIR_ENTRIES: &[&str] = &["lost+found", ".snapshot"];

/// Whether a directory entry is a benign filesystem artifact that should not make
/// the data directory count as "non-empty".
fn is_benign_entry(name: &OsStr) -> bool {
    match name.to_str() {
        Some(s) => s.starts_with('.') || IGNORED_DIR_ENTRIES.contains(&s),
        None => false, // non-UTF8 name => treat as a real entry, be conservative
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatVersion {
    pub format_version: u32,
    pub fakecloud_version: String,
    pub created_at: String,
}

#[derive(Debug, Error)]
pub enum VersionError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize version file: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error(
        "persistence format version mismatch at {path}: on-disk format_version={on_disk}, binary expects {expected}. \
         Either point --data-path at a matching directory, or delete the directory to start fresh."
    )]
    FormatMismatch {
        path: PathBuf,
        on_disk: u32,
        expected: u32,
    },
    #[error(
        "persistence data directory {dir} is not empty but has no {file} file. \
         Refusing to initialize it: either point --data-path at an empty directory or restore the missing version file."
    )]
    NonEmptyDirectoryWithoutVersionFile { dir: PathBuf, file: String },
}

fn version_file_path(dir: &Path) -> PathBuf {
    dir.join(VERSION_FILE_NAME)
}

pub fn write_version_file(dir: &Path, fakecloud_version: &str) -> Result<(), VersionError> {
    let path = version_file_path(dir);
    let value = FormatVersion {
        format_version: FORMAT_VERSION,
        fakecloud_version: fakecloud_version.to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    crate::atomic::write_atomic_toml(&path, &value).map_err(|source| VersionError::Io {
        path: path.clone(),
        source,
    })?;
    Ok(())
}

pub fn check_version_file(dir: &Path) -> Result<(), VersionError> {
    let path = version_file_path(dir);
    if !path.exists() {
        return Ok(());
    }
    let text = std::fs::read_to_string(&path).map_err(|source| VersionError::Io {
        path: path.clone(),
        source,
    })?;
    let parsed: FormatVersion = toml::from_str(&text).map_err(|source| VersionError::Parse {
        path: path.clone(),
        source,
    })?;
    if parsed.format_version != FORMAT_VERSION {
        return Err(VersionError::FormatMismatch {
            path,
            on_disk: parsed.format_version,
            expected: FORMAT_VERSION,
        });
    }
    Ok(())
}

pub fn ensure_version_file(dir: &Path, fakecloud_version: &str) -> Result<(), VersionError> {
    let path = version_file_path(dir);
    if path.exists() {
        return check_version_file(dir);
    }
    if dir.exists() {
        let has_real_entry = std::fs::read_dir(dir)
            .map_err(|source| VersionError::Io {
                path: dir.to_path_buf(),
                source,
            })?
            .filter_map(Result::ok)
            .any(|entry| !is_benign_entry(&entry.file_name()));
        if has_real_entry {
            return Err(VersionError::NonEmptyDirectoryWithoutVersionFile {
                dir: dir.to_path_buf(),
                file: VERSION_FILE_NAME.to_string(),
            });
        }
    }
    write_version_file(dir, fakecloud_version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_creates_version_file_in_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        ensure_version_file(tmp.path(), "test").unwrap();
        assert!(tmp.path().join(VERSION_FILE_NAME).exists());
    }

    #[test]
    fn ensure_rejects_non_empty_dir_without_version_file() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("stray.txt"), b"hello").unwrap();
        let err = ensure_version_file(tmp.path(), "test").unwrap_err();
        matches!(
            err,
            VersionError::NonEmptyDirectoryWithoutVersionFile { .. }
        );
    }

    #[test]
    fn ensure_ok_when_dir_has_only_lost_found() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("lost+found")).unwrap();
        ensure_version_file(tmp.path(), "test").unwrap();
        assert!(tmp.path().join(VERSION_FILE_NAME).exists());
    }

    #[test]
    fn ensure_ok_when_dir_has_only_dotfiles() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".DS_Store"), b"junk").unwrap();
        std::fs::create_dir(tmp.path().join(".snapshot")).unwrap();
        ensure_version_file(tmp.path(), "test").unwrap();
        assert!(tmp.path().join(VERSION_FILE_NAME).exists());
    }

    #[test]
    fn ensure_rejects_real_file_alongside_lost_found() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("lost+found")).unwrap();
        std::fs::write(tmp.path().join("data.bin"), b"real data").unwrap();
        let err = ensure_version_file(tmp.path(), "test").unwrap_err();
        assert!(matches!(
            err,
            VersionError::NonEmptyDirectoryWithoutVersionFile { .. }
        ));
        assert!(!tmp.path().join(VERSION_FILE_NAME).exists());
    }

    #[test]
    fn ensure_ok_when_version_file_already_present() {
        let tmp = tempfile::tempdir().unwrap();
        write_version_file(tmp.path(), "test").unwrap();
        std::fs::write(tmp.path().join("stray.txt"), b"hello").unwrap();
        ensure_version_file(tmp.path(), "test").unwrap();
    }
}
