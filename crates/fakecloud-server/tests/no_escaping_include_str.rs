//! Guard against `include_str!` / `include_bytes!` paths that escape a crate's
//! own directory. Such a path (e.g. `../../../aws-models/foo.json`) resolves
//! fine in the workspace but is NOT included in the crate's published tarball
//! (`cargo package` only bundles files under the crate root), so the crate
//! fails to compile from crates.io — which shipped broken model crates in
//! v0.41.0 (`cargo install fakecloud` could not build). Vendor the data inside
//! the crate instead and reference it with a non-escaping relative path.

use std::path::{Path, PathBuf};

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// A relative `include_*!` argument escapes the crate when, resolved against the
/// source file's directory, it climbs above the crate root. Count `../` hops
/// versus the source file's depth below `crates/<crate>/`.
fn escapes_crate(src_file: &Path, crate_src_root: &Path, include_arg: &str) -> bool {
    // Directory the include is resolved relative to = the .rs file's parent.
    let base = src_file.parent().unwrap_or(src_file);
    // Depth of `base` below the crate root (…/crates/<crate>/).
    let depth_below_root = base
        .strip_prefix(crate_src_root.parent().unwrap())
        .map(|p| p.components().count())
        .unwrap_or(0);
    let up_hops = include_arg.split('/').filter(|c| *c == "..").count();
    up_hops > depth_below_root
}

#[test]
fn no_include_str_escapes_its_crate() {
    // CARGO_MANIFEST_DIR = <repo>/crates/fakecloud-server
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    assert!(
        crates_dir.ends_with("crates"),
        "unexpected layout: {crates_dir:?}"
    );

    let re_arg = regex_lite_arg;
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(&crates_dir).unwrap().flatten() {
        let crate_dir = entry.path();
        let src_root = crate_dir.join("src");
        if !src_root.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        rs_files(&src_root, &mut files);
        for f in &files {
            let Ok(text) = std::fs::read_to_string(f) else {
                continue;
            };
            for macro_name in ["include_str!", "include_bytes!"] {
                let mut from = 0;
                while let Some(pos) = text[from..].find(macro_name) {
                    let start = from + pos + macro_name.len();
                    if let Some(arg) = re_arg(&text[start..]) {
                        if arg.starts_with("..") && escapes_crate(f, &src_root, &arg) {
                            offenders.push(format!("{}: {macro_name}(\"{arg}\")", f.display()));
                        }
                    }
                    from = start;
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "include_str!/include_bytes! escaping the crate dir breaks published tarballs; \
         vendor the file inside the crate:\n{}",
        offenders.join("\n")
    );
}

/// Extract the first string-literal argument from `(\"...\")`, else None.
fn regex_lite_arg(rest: &str) -> Option<String> {
    let rest = rest.trim_start();
    let rest = rest.strip_prefix('(')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}
