//! External extensions shipped in layers (`/opt/extensions/*`).
//!
//! On Lambda the platform starts every executable in `/opt/extensions`
//! alongside the runtime; they register with the Extensions API and receive
//! `INVOKE` / `SHUTDOWN` events (served with the Runtime API, see
//! [`super::api`]). Here the instance's entrypoint is wrapped so the same
//! executables start first, and the sandbox is told how many to wait for.

use std::collections::BTreeMap;

/// The base images' entrypoint, which runs the runtime.
pub(crate) const BASE_IMAGE_ENTRYPOINT: &str = "/lambda-entrypoint.sh";

/// Shell script (`sh -c <this> sh <entrypoint> <args>`) that starts each
/// executable in `/opt/extensions` in the background, as Lambda does, then
/// runs the entrypoint in its place.
pub(crate) const LAUNCHER_SCRIPT: &str = r#"for e in /opt/extensions/*; do if [ -f "$e" ] && [ -x "$e" ]; then "$e" & fi; done; exec "$@""#;

/// How many extensions `layers` (in attach order, later ones overwriting
/// earlier paths) put in `/opt/extensions`: executable files directly in the
/// layer's `extensions/` directory.
pub(crate) fn layer_extension_count(layers: &[Vec<u8>]) -> usize {
    let mut found: BTreeMap<String, bool> = BTreeMap::new();
    for layer in layers {
        let Ok(mut archive) = zip::ZipArchive::new(std::io::Cursor::new(layer.as_slice())) else {
            continue;
        };
        for i in 0..archive.len() {
            let Ok(entry) = archive.by_index_raw(i) else {
                continue;
            };
            let Ok(raw_name) = entry.name() else {
                continue;
            };
            let name = raw_name.trim_start_matches("./");
            let Some(file) = name.strip_prefix("extensions/") else {
                continue;
            };
            if file.is_empty() || file.contains('/') || entry.is_dir() {
                continue;
            }
            let executable = entry.unix_mode().is_some_and(|m| m & 0o111 != 0);
            found.insert(file.to_string(), executable);
        }
    }
    found.values().filter(|x| **x).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn layer(entries: &[(&str, u32)]) -> Vec<u8> {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        for (name, mode) in entries {
            if name.ends_with('/') {
                w.add_directory(*name, zip::write::SimpleFileOptions::default())
                    .unwrap();
                continue;
            }
            w.start_file(
                *name,
                zip::write::SimpleFileOptions::default().unix_permissions(*mode),
            )
            .unwrap();
            w.write_all(b"#!/bin/sh\n").unwrap();
        }
        w.finish().unwrap().into_inner()
    }

    #[test]
    fn counts_executables_directly_in_extensions() {
        let a = layer(&[
            ("extensions/", 0o755),
            ("extensions/one", 0o755),
            ("extensions/readme.txt", 0o644),
            ("extensions/one-lib/helper", 0o755),
            ("python/mod.py", 0o755),
        ]);
        assert_eq!(layer_extension_count(std::slice::from_ref(&a)), 1);
        let b = layer(&[("extensions/two", 0o700), ("extensions/one", 0o644)]);
        // The later layer's `one` is not executable.
        assert_eq!(layer_extension_count(&[a, b]), 1);
        assert_eq!(layer_extension_count(&[]), 0);
        assert_eq!(layer_extension_count(&[b"not a zip".to_vec()]), 0);
    }
}
