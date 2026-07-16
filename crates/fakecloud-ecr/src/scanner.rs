//! Real image vulnerability scanning via the optional Trivy CLI.
//!
//! ECR's `StartImageScan` traditionally points at AWS Inspector. fakecloud
//! shells out to `trivy image --input <tar>` when the binary is available
//! and parses the JSON output into an `ImageScanFindings`. When `trivy`
//! is not installed we fall back to a synthetic empty result with
//! `scan_status=COMPLETE` so the API surface stays consistent — tests
//! that don't care about real CVEs keep working, and security-tooling
//! integration tests can install Trivy to get real findings.
//!
//! Trivy is invoked in a subprocess so it doesn't add a Rust-side
//! cargo dependency and so it stays opt-in. The detection logic mirrors
//! the docker/podman CLI detection used elsewhere in fakecloud.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;

use base64::Engine;
use serde_json::{json, Value};

use crate::state::{ImageScanFindings, Layer};

/// Resolve the path to the `trivy` binary. Honors `FAKECLOUD_TRIVY_BIN`
/// for explicit overrides (e.g. CI installing into a non-default
/// location). Returns `None` when the binary is not found, in which
/// case callers fall back to the synthetic empty-findings path.
pub fn detect_trivy() -> Option<String> {
    if let Ok(custom) = std::env::var("FAKECLOUD_TRIVY_BIN") {
        if cli_available(&custom) {
            return Some(custom);
        }
        return None;
    }
    if cli_available("trivy") {
        return Some("trivy".to_string());
    }
    None
}

/// Map the Rust build-target architecture name to the value Docker/OCI
/// image configs use, so the synthetic manifest fed to Trivy matches the
/// host it actually runs on (arm64 laptops, arm64 CI, etc.) instead of
/// always claiming amd64.
fn docker_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

fn cli_available(cli: &str) -> bool {
    std::process::Command::new(cli)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run Trivy against a single-layer tarball reconstructed from the
/// repository's stored layers. Returns parsed findings on success;
/// `None` if Trivy isn't installed or the scan fails (caller logs +
/// falls back to synthetic).
pub async fn scan_layers(image_digest: &str, layers: &[Layer]) -> Option<ImageScanFindings> {
    let trivy = detect_trivy()?;
    let tmp = tempfile::tempdir().ok()?;
    let tar_path = tmp.path().join("image.tar");
    if let Err(err) = build_image_tar(&tar_path, layers).await {
        tracing::warn!(%err, "ECR scanner: failed to build image tar for trivy; using synthetic");
        return None;
    }

    let output = match tokio::process::Command::new(&trivy)
        .args([
            "image",
            "--quiet",
            "--no-progress",
            "--format",
            "json",
            "--scanners",
            "vuln",
            "--input",
        ])
        .arg(&tar_path)
        .output()
        .await
    {
        Ok(o) => o,
        Err(err) => {
            tracing::warn!(%err, "ECR scanner: trivy invocation failed; using synthetic");
            return None;
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::warn!(
            stderr = %stderr,
            "ECR scanner: trivy exited non-zero; using synthetic"
        );
        return None;
    }

    parse_trivy_output(image_digest, &output.stdout)
}

async fn build_image_tar(tar_path: &Path, layers: &[Layer]) -> std::io::Result<()> {
    use std::io::Write;
    let file = std::fs::File::create(tar_path)?;
    let mut builder = tar::Builder::new(file);
    let engine = base64::engine::general_purpose::STANDARD;

    let mut layer_filenames: Vec<String> = Vec::new();
    for (idx, layer) in layers.iter().enumerate() {
        // Surface base64-decode failures explicitly. Silently
        // substituting empty bytes would have Trivy scan a 0-byte
        // layer and report "no findings" on what is actually a
        // corrupted blob — the caller falls back to synthetic-empty
        // findings, which is the same outcome but logged rather
        // than masquerading as a clean scan.
        let bytes = engine.decode(&layer.blob_b64).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("layer {} base64 decode failed: {}", layer.digest, err),
            )
        })?;
        let filename = format!("layer-{idx}.tar");
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append_data(&mut header, &filename, bytes.as_slice())?;
        layer_filenames.push(filename);
    }

    // Minimal manifest.json so trivy treats this as a docker-format archive.
    let config = json!({
        "architecture": docker_arch(),
        "os": "linux",
        "rootfs": {
            "type": "layers",
            "diff_ids": layers
                .iter()
                .map(|l| l.digest.clone())
                .collect::<Vec<_>>(),
        },
        "config": {},
    });
    let config_bytes = serde_json::to_vec(&config)?;
    let mut config_header = tar::Header::new_gnu();
    config_header.set_size(config_bytes.len() as u64);
    config_header.set_mode(0o644);
    config_header.set_cksum();
    builder.append_data(&mut config_header, "config.json", config_bytes.as_slice())?;

    let manifest = json!([{
        "Config": "config.json",
        "RepoTags": ["fakecloud-scan:latest"],
        "Layers": layer_filenames,
    }]);
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let mut manifest_header = tar::Header::new_gnu();
    manifest_header.set_size(manifest_bytes.len() as u64);
    manifest_header.set_mode(0o644);
    manifest_header.set_cksum();
    builder.append_data(
        &mut manifest_header,
        "manifest.json",
        manifest_bytes.as_slice(),
    )?;

    let mut writer = builder.into_inner()?;
    writer.flush()?;
    Ok(())
}

fn parse_trivy_output(image_digest: &str, stdout: &[u8]) -> Option<ImageScanFindings> {
    let parsed: Value = serde_json::from_slice(stdout).ok()?;
    let mut findings = Vec::new();
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();

    let results = parsed
        .get("Results")?
        .as_array()
        .cloned()
        .unwrap_or_default();
    for result in results {
        if let Some(vulns) = result.get("Vulnerabilities").and_then(|v| v.as_array()) {
            for vuln in vulns {
                let severity =
                    map_severity(vuln.get("Severity").and_then(Value::as_str).unwrap_or(""));
                *counts.entry(severity.clone()).or_insert(0) += 1;
                let name = vuln
                    .get("VulnerabilityID")
                    .and_then(Value::as_str)
                    .unwrap_or("UNKNOWN")
                    .to_string();
                let description = vuln
                    .get("Description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let uri = vuln
                    .get("PrimaryURL")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let pkg = vuln
                    .get("PkgName")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let installed = vuln
                    .get("InstalledVersion")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let fixed = vuln
                    .get("FixedVersion")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                findings.push(json!({
                    "name": name,
                    "description": description,
                    "uri": uri,
                    "severity": severity,
                    "attributes": [
                        {"key": "package_name", "value": pkg},
                        {"key": "package_version", "value": installed},
                        {"key": "fixed_version", "value": fixed},
                    ],
                }));
            }
        }
    }

    Some(ImageScanFindings {
        image_digest: image_digest.to_string(),
        scan_status: "COMPLETE".to_string(),
        scan_completed_at: Some(chrono::Utc::now()),
        vulnerability_source_updated_at: Some(chrono::Utc::now()),
        finding_severity_counts: counts,
        findings,
    })
}

fn map_severity(trivy: &str) -> String {
    match trivy.to_uppercase().as_str() {
        "CRITICAL" => "CRITICAL".to_string(),
        "HIGH" => "HIGH".to_string(),
        "MEDIUM" => "MEDIUM".to_string(),
        "LOW" => "LOW".to_string(),
        _ => "UNDEFINED".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_trivy_output_extracts_vulnerabilities_and_counts() {
        let sample = serde_json::json!({
            "Results": [{
                "Target": "alpine:3.10",
                "Vulnerabilities": [
                    {
                        "VulnerabilityID": "CVE-2020-1971",
                        "PkgName": "libcrypto1.1",
                        "InstalledVersion": "1.1.1g-r0",
                        "FixedVersion": "1.1.1i-r0",
                        "Severity": "HIGH",
                        "Description": "openssl null pointer deref",
                        "PrimaryURL": "https://avd.aquasec.com/nvd/cve-2020-1971"
                    },
                    {
                        "VulnerabilityID": "CVE-2021-3711",
                        "PkgName": "libssl1.1",
                        "InstalledVersion": "1.1.1g-r0",
                        "Severity": "CRITICAL"
                    }
                ]
            }]
        })
        .to_string();
        let findings = parse_trivy_output("sha256:abc", sample.as_bytes()).expect("parsed");
        assert_eq!(findings.scan_status, "COMPLETE");
        assert_eq!(findings.findings.len(), 2);
        assert_eq!(findings.finding_severity_counts.get("HIGH"), Some(&1));
        assert_eq!(findings.finding_severity_counts.get("CRITICAL"), Some(&1));
    }

    #[test]
    fn parse_trivy_output_handles_unknown_severity() {
        let sample = serde_json::json!({
            "Results": [{
                "Vulnerabilities": [
                    {"VulnerabilityID": "CVE-X", "Severity": "WAT"}
                ]
            }]
        })
        .to_string();
        let findings = parse_trivy_output("d", sample.as_bytes()).unwrap();
        assert_eq!(findings.finding_severity_counts.get("UNDEFINED"), Some(&1));
    }

    #[test]
    fn parse_trivy_output_returns_some_with_no_findings_section() {
        let sample = r#"{"Results": []}"#;
        let findings = parse_trivy_output("d", sample.as_bytes()).unwrap();
        assert!(findings.findings.is_empty());
    }

    #[test]
    fn docker_arch_maps_rust_names_and_matches_host() {
        // The mapping table is exhaustive for the arches we ship on and
        // falls through to the raw name otherwise.
        assert_eq!(map_rust_arch("x86_64"), "amd64");
        assert_eq!(map_rust_arch("aarch64"), "arm64");
        assert_eq!(map_rust_arch("riscv64"), "riscv64");
        // The live helper must resolve to a non-empty docker arch for the
        // host actually running the test.
        assert!(!docker_arch().is_empty());
        assert_eq!(docker_arch(), map_rust_arch(std::env::consts::ARCH));
    }

    // Pure mapping mirror of `docker_arch`, testable independent of the
    // host's real architecture.
    fn map_rust_arch(arch: &str) -> &str {
        match arch {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            other => other,
        }
    }

    #[test]
    fn detect_trivy_respects_env_override() {
        // When the override is bogus, detect_trivy returns None
        std::env::set_var("FAKECLOUD_TRIVY_BIN", "/nonexistent/trivy-binary");
        assert!(detect_trivy().is_none());
        std::env::remove_var("FAKECLOUD_TRIVY_BIN");
    }
}
