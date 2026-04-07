use std::collections::HashMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{PatchBaseline, PatchGroup};

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn create_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("Name", &name, 3, 128)?;
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
                "AMAZON_LINUX_2023",
            ],
        )?;
        let operating_system = body["OperatingSystem"]
            .as_str()
            .unwrap_or("WINDOWS")
            .to_string();
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 1024)?;
        validate_optional_enum(
            "ApprovedPatchesComplianceLevel",
            body["ApprovedPatchesComplianceLevel"].as_str(),
            &[
                "CRITICAL",
                "HIGH",
                "MEDIUM",
                "LOW",
                "INFORMATIONAL",
                "UNSPECIFIED",
            ],
        )?;
        validate_optional_enum(
            "RejectedPatchesAction",
            body["RejectedPatchesAction"].as_str(),
            &["ALLOW_AS_DEPENDENCY", "BLOCK"],
        )?;
        validate_optional_enum(
            "AvailableSecurityUpdatesComplianceStatus",
            body["AvailableSecurityUpdatesComplianceStatus"].as_str(),
            &["COMPLIANT", "NON_COMPLIANT"],
        )?;
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 1, 64)?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let approval_rules = body.get("ApprovalRules").cloned();
        let approved_patches: Vec<String> = body["ApprovedPatches"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let rejected_patches: Vec<String> = body["RejectedPatches"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let approved_patches_compliance_level = body["ApprovedPatchesComplianceLevel"]
            .as_str()
            .unwrap_or("UNSPECIFIED")
            .to_string();
        let rejected_patches_action = body["RejectedPatchesAction"]
            .as_str()
            .unwrap_or("ALLOW_AS_DEPENDENCY")
            .to_string();
        let global_filters = body.get("GlobalFilters").cloned();
        let sources: Vec<Value> = body["Sources"].as_array().cloned().unwrap_or_default();
        let approved_patches_enable_non_security = body["ApprovedPatchesEnableNonSecurity"]
            .as_bool()
            .unwrap_or(false);
        let tags: HashMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let available_security_updates_compliance_status = body
            ["AvailableSecurityUpdatesComplianceStatus"]
            .as_str()
            .map(|s| s.to_string());
        let client_token = body["ClientToken"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();

        // Idempotency: if a baseline with the same ClientToken already exists, return it
        if let Some(ref token) = client_token {
            if let Some(existing) = state
                .patch_baselines
                .values()
                .find(|pb| pb.client_token.as_deref() == Some(token))
            {
                return Ok(json_resp(json!({ "BaselineId": existing.id })));
            }
        }

        let baseline_id = format!(
            "pb-{}",
            &uuid::Uuid::new_v4().to_string().replace('-', "")[..17]
        );

        let pb = PatchBaseline {
            id: baseline_id.clone(),
            name,
            operating_system,
            description,
            approval_rules,
            approved_patches,
            rejected_patches,
            tags,
            approved_patches_compliance_level,
            rejected_patches_action,
            global_filters,
            sources,
            approved_patches_enable_non_security,
            available_security_updates_compliance_status,
            client_token,
        };

        state.patch_baselines.insert(baseline_id.clone(), pb);

        Ok(json_resp(json!({ "BaselineId": baseline_id })))
    }

    pub(super) fn delete_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        validate_string_length("BaselineId", baseline_id, 20, 128)?;

        let mut state = self.state.write();
        state.patch_baselines.remove(baseline_id);
        // Also remove any patch group associations
        state
            .patch_groups
            .retain(|pg| pg.baseline_id != baseline_id);

        Ok(json_resp(json!({ "BaselineId": baseline_id })))
    }

    pub(super) fn describe_patch_baselines(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let all_baselines: Vec<Value> = state
            .patch_baselines
            .values()
            .filter(|pb| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "NAME_PREFIX" => {
                                if !values.iter().any(|v| pb.name.starts_with(v)) {
                                    return false;
                                }
                            }
                            "OWNER" => {
                                // We don't track owner, but "Self" means user-created
                                if values.contains(&"AWS") {
                                    return false;
                                }
                            }
                            "OPERATING_SYSTEM" => {
                                if !values.contains(&pb.operating_system.as_str()) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|pb| {
                let mut v = json!({
                    "BaselineId": pb.id,
                    "BaselineName": pb.name,
                    "OperatingSystem": pb.operating_system,
                    "DefaultBaseline": false,
                });
                if let Some(ref desc) = pb.description {
                    v["BaselineDescription"] = json!(desc);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all_baselines.len() {
            &all_baselines[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let baselines: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "BaselineIdentities": baselines });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    pub(super) fn get_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        validate_string_length("BaselineId", baseline_id, 20, 128)?;

        let state = self.state.read();
        let pb = state.patch_baselines.get(baseline_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Baseline {baseline_id} does not exist."),
            )
        })?;

        let mut resp = json!({
            "BaselineId": pb.id,
            "Name": pb.name,
            "OperatingSystem": pb.operating_system,
            "ApprovedPatches": pb.approved_patches,
            "RejectedPatches": pb.rejected_patches,
            "ApprovedPatchesComplianceLevel": pb.approved_patches_compliance_level,
            "RejectedPatchesAction": pb.rejected_patches_action,
            "ApprovedPatchesEnableNonSecurity": pb.approved_patches_enable_non_security,
            "Sources": pb.sources,
            "PatchGroups": state.patch_groups.iter()
                .filter(|pg| pg.baseline_id == baseline_id)
                .map(|pg| pg.patch_group.clone())
                .collect::<Vec<_>>(),
        });
        if let Some(ref desc) = pb.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref rules) = pb.approval_rules {
            resp["ApprovalRules"] = rules.clone();
        }
        if let Some(ref gf) = pb.global_filters {
            resp["GlobalFilters"] = gf.clone();
        }
        if let Some(ref status) = pb.available_security_updates_compliance_status {
            resp["AvailableSecurityUpdatesComplianceStatus"] = json!(status);
        }

        Ok(json_resp(resp))
    }

    pub(super) fn register_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?
            .to_string();
        validate_string_length("BaselineId", &baseline_id, 20, 128)?;
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?
            .to_string();
        validate_string_length("PatchGroup", &patch_group, 1, 256)?;

        let mut state = self.state.write();

        // Check baseline exists (AWS returns "Maintenance window" in this error, not "Patch baseline")
        if !state.patch_baselines.contains_key(&baseline_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Maintenance window {baseline_id} does not exist"),
            ));
        }

        // Check if this patch group is already registered to a baseline with same OS
        let os = state.patch_baselines[&baseline_id].operating_system.clone();
        if let Some(existing) = state
            .patch_groups
            .iter()
            .find(|pg| pg.patch_group == patch_group)
        {
            if let Some(existing_pb) = state.patch_baselines.get(&existing.baseline_id) {
                if existing_pb.operating_system == os {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AlreadyExistsException",
                        format!(
                            "Patch Group baseline already has a baseline registered for OperatingSystem {os}."
                        ),
                    ));
                }
            }
        }

        state.patch_groups.push(PatchGroup {
            baseline_id: baseline_id.clone(),
            patch_group: patch_group.clone(),
        });

        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "PatchGroup": patch_group,
        })))
    }

    pub(super) fn deregister_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        validate_string_length("BaselineId", baseline_id, 20, 128)?;
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        validate_string_length("PatchGroup", patch_group, 1, 256)?;

        let mut state = self.state.write();

        // Check if the association exists
        let exists = state
            .patch_groups
            .iter()
            .any(|pg| pg.baseline_id == baseline_id && pg.patch_group == patch_group);
        if exists {
            state
                .patch_groups
                .retain(|pg| !(pg.baseline_id == baseline_id && pg.patch_group == patch_group));
        } else {
            // Allow deregistering default baselines (they are implicitly registered)
            let is_default = is_default_patch_baseline(baseline_id);
            if !is_default {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    "Patch Baseline to be retrieved does not exist.",
                ));
            }
        }

        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "PatchGroup": patch_group,
        })))
    }

    pub(super) fn get_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        validate_string_length("PatchGroup", patch_group, 1, 256)?;
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "AMAZON_LINUX_2023",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
            ],
        )?;
        let operating_system = body["OperatingSystem"].as_str().unwrap_or("WINDOWS");

        let state = self.state.read();

        // Find a patch group association matching both patch group and OS
        let found = state.patch_groups.iter().find(|pg| {
            pg.patch_group == patch_group
                && state
                    .patch_baselines
                    .get(&pg.baseline_id)
                    .is_some_and(|pb| pb.operating_system == operating_system)
        });

        if let Some(pg) = found {
            Ok(json_resp(json!({
                "BaselineId": pg.baseline_id,
                "PatchGroup": pg.patch_group,
                "OperatingSystem": operating_system,
            })))
        } else {
            // Fall back to default baseline for the region/OS
            let mut resp = json!({
                "PatchGroup": patch_group,
                "OperatingSystem": operating_system,
            });
            if let Some(baseline_id) = default_patch_baseline(&req.region, operating_system) {
                resp["BaselineId"] = json!(baseline_id);
            }
            Ok(json_resp(resp))
        }
    }

    pub(super) fn describe_patch_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let all_mappings: Vec<Value> = state
            .patch_groups
            .iter()
            .filter(|pg| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "NAME_PREFIX" => {
                                if !values.iter().any(|v| pg.patch_group.starts_with(v)) {
                                    return false;
                                }
                            }
                            "OPERATING_SYSTEM" => {
                                if let Some(pb) = state.patch_baselines.get(&pg.baseline_id) {
                                    if !values.contains(&pb.operating_system.as_str()) {
                                        return false;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|pg| {
                let mut baseline_identity = json!({
                    "BaselineId": pg.baseline_id,
                    "DefaultBaseline": false,
                });
                if let Some(pb) = state.patch_baselines.get(&pg.baseline_id) {
                    baseline_identity["BaselineName"] = json!(pb.name);
                    baseline_identity["OperatingSystem"] = json!(pb.operating_system);
                    if let Some(ref desc) = pb.description {
                        baseline_identity["BaselineDescription"] = json!(desc);
                    }
                }
                json!({
                    "PatchGroup": pg.patch_group,
                    "BaselineIdentity": baseline_identity,
                })
            })
            .collect();

        let page = if next_token_offset < all_mappings.len() {
            &all_mappings[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let mappings: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Mappings": mappings });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    // -----------------------------------------------------------------------
    // Associations
    // -----------------------------------------------------------------------

    pub(super) fn update_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;

        let mut state = self.state.write();
        let pb = state.patch_baselines.get_mut(baseline_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Patch baseline {baseline_id} does not exist"),
            )
        })?;

        if let Some(name) = body["Name"].as_str() {
            pb.name = name.to_string();
        }
        if body.get("Description").is_some() {
            pb.description = body["Description"].as_str().map(|s| s.to_string());
        }
        if let Some(rules) = body.get("ApprovalRules") {
            pb.approval_rules = Some(rules.clone());
        }
        if let Some(arr) = body["ApprovedPatches"].as_array() {
            pb.approved_patches = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(arr) = body["RejectedPatches"].as_array() {
            pb.rejected_patches = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(level) = body["ApprovedPatchesComplianceLevel"].as_str() {
            pb.approved_patches_compliance_level = level.to_string();
        }
        if let Some(action) = body["RejectedPatchesAction"].as_str() {
            pb.rejected_patches_action = action.to_string();
        }
        if let Some(gf) = body.get("GlobalFilters") {
            pb.global_filters = Some(gf.clone());
        }
        if let Some(arr) = body["Sources"].as_array() {
            pb.sources = arr.clone();
        }
        if let Some(enable) = body["ApprovedPatchesEnableNonSecurity"].as_bool() {
            pb.approved_patches_enable_non_security = enable;
        }

        let mut resp = json!({
            "BaselineId": pb.id,
            "Name": pb.name,
            "OperatingSystem": pb.operating_system,
            "ApprovedPatches": pb.approved_patches,
            "RejectedPatches": pb.rejected_patches,
            "ApprovedPatchesComplianceLevel": pb.approved_patches_compliance_level,
            "RejectedPatchesAction": pb.rejected_patches_action,
            "ApprovedPatchesEnableNonSecurity": pb.approved_patches_enable_non_security,
            "Sources": pb.sources,
        });
        if let Some(ref desc) = pb.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref rules) = pb.approval_rules {
            resp["ApprovalRules"] = rules.clone();
        }
        if let Some(ref gf) = pb.global_filters {
            resp["GlobalFilters"] = gf.clone();
        }

        Ok(json_resp(resp))
    }

    pub(super) fn describe_instance_patch_states(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let _instance_ids = body["InstanceIds"]
            .as_array()
            .ok_or_else(|| missing("InstanceIds"))?;
        // Return empty - no real instances in emulator
        Ok(json_resp(json!({ "InstancePatchStates": [] })))
    }

    pub(super) fn describe_instance_patch_states_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("PatchGroup", body["PatchGroup"].as_str(), 1, 256)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let _patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        Ok(json_resp(json!({ "InstancePatchStates": [] })))
    }

    pub(super) fn describe_instance_patches(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let _instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        Ok(json_resp(json!({ "Patches": [] })))
    }

    pub(super) fn describe_effective_patches_for_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("BaselineId", body["BaselineId"].as_str(), 20, 128)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let _baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        Ok(json_resp(json!({ "EffectivePatches": [] })))
    }

    pub(super) fn get_deployable_patch_snapshot_for_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SnapshotId", body["SnapshotId"].as_str(), 36, 36)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let snapshot_id = body["SnapshotId"]
            .as_str()
            .ok_or_else(|| missing("SnapshotId"))?;

        Ok(json_resp(json!({
            "InstanceId": instance_id,
            "SnapshotId": snapshot_id,
            "Product": "{}",
            "SnapshotDownloadUrl": "",
        })))
    }

    // ── Resource Data Sync ────────────────────────────────────────

    pub(super) fn describe_patch_group_state(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("PatchGroup", body["PatchGroup"].as_str(), 1, 256)?;
        let _patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        Ok(json_resp(json!({
            "Instances": 0,
            "InstancesWithInstalledPatches": 0,
            "InstancesWithInstalledOtherPatches": 0,
            "InstancesWithInstalledRejectedPatches": 0,
            "InstancesWithInstalledPendingRebootPatches": 0,
            "InstancesWithMissingPatches": 0,
            "InstancesWithFailedPatches": 0,
            "InstancesWithNotApplicablePatches": 0,
            "InstancesWithUnreportedNotApplicablePatches": 0,
            "InstancesWithCriticalNonCompliantPatches": 0,
            "InstancesWithSecurityNonCompliantPatches": 0,
            "InstancesWithOtherNonCompliantPatches": 0,
        })))
    }

    pub(super) fn describe_patch_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_required("OperatingSystem", &body["OperatingSystem"])?;
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
                "AMAZON_LINUX_2023",
            ],
        )?;
        validate_required("Property", &body["Property"])?;
        validate_optional_enum(
            "Property",
            body["Property"].as_str(),
            &[
                "PRODUCT",
                "PRODUCT_FAMILY",
                "CLASSIFICATION",
                "MSRC_SEVERITY",
                "PRIORITY",
                "SEVERITY",
            ],
        )?;
        validate_optional_enum(
            "PatchSet",
            body["PatchSet"].as_str(),
            &["OS", "APPLICATION"],
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        Ok(json_resp(json!({ "Properties": [] })))
    }

    pub(super) fn get_default_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
                "AMAZON_LINUX_2023",
            ],
        )?;
        let operating_system = body["OperatingSystem"].as_str().unwrap_or("WINDOWS");

        let state = self.state.read();

        // Check if a custom default has been registered
        if let Some(ref baseline_id) = state.default_patch_baseline_id {
            return Ok(json_resp(json!({
                "BaselineId": baseline_id,
                "OperatingSystem": operating_system,
            })));
        }

        // Otherwise look up from defaults
        let baseline_id =
            default_patch_baseline(&state.region, operating_system).unwrap_or_default();
        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "OperatingSystem": operating_system,
        })))
    }

    pub(super) fn register_default_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?
            .to_string();

        let mut state = self.state.write();

        // Verify baseline exists (custom or default)
        if !state.patch_baselines.contains_key(&baseline_id)
            && !is_default_patch_baseline(&baseline_id)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Patch baseline {baseline_id} does not exist"),
            ));
        }

        state.default_patch_baseline_id = Some(baseline_id.clone());
        Ok(json_resp(json!({
            "BaselineId": baseline_id,
        })))
    }

    pub(super) fn describe_available_patches(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        Ok(json_resp(json!({ "Patches": [] })))
    }
}

/// Look up the default patch baseline for a given region and OS.
pub(super) fn default_patch_baseline(region: &str, operating_system: &str) -> Option<String> {
    static DEFAULT_BASELINES: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("../default_baselines.json")).unwrap_or(json!({}))
    });
    DEFAULT_BASELINES
        .get(region)
        .and_then(|r| r.get(operating_system))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Check if a baseline ID is a known default baseline.
pub(super) fn is_default_patch_baseline(baseline_id: &str) -> bool {
    static DEFAULT_BASELINES: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("../default_baselines.json")).unwrap_or(json!({}))
    });
    if let Some(obj) = DEFAULT_BASELINES.as_object() {
        for region_data in obj.values() {
            if let Some(region_obj) = region_data.as_object() {
                for val in region_obj.values() {
                    if val.as_str() == Some(baseline_id) {
                        return true;
                    }
                }
            }
        }
    }
    false
}
