//! `AWS::EFS::FileSystem`, `AWS::EFS::MountTarget` and `AWS::EFS::AccessPoint`
//! CloudFormation provisioning. Each resource is written through to the
//! `elasticfilesystem` (EFS) service state as the same `Description`-shaped JSON
//! the direct `CreateFileSystem` / `CreateMountTarget` / `CreateAccessPoint`
//! handlers store, so a CFN-created resource reads back identically on
//! `DescribeFileSystems` / `DescribeMountTargets` / `DescribeAccessPoints` and
//! persists through the `elasticfilesystem` snapshot hook (survives a restart --
//! the #1766 phantom-resource lesson).
//!
//! Physical id + `Ref` (verified against the AWS resource specs):
//!   FileSystem  -> Ref = FileSystemId (`fs-...`);   physical id = FileSystemId
//!   MountTarget -> Ref = MountTargetId (`fsmt-...`); physical id = MountTargetId
//!   AccessPoint -> Ref = AccessPointId (`fsap-...`); physical id = AccessPointId
//!
//! `Fn::GetAtt` (verified against the AWS resource specs):
//!   FileSystem  -> Arn (FileSystemArn), FileSystemId
//!   MountTarget -> Id (MountTargetId), IpAddress
//!   AccessPoint -> Arn (AccessPointArn), AccessPointId
//!
//! Because CloudFormation provisions a stack in dependency order, a file system
//! in a stack is created before any mount target / access point that `Ref`s it.
//! The direct `CreateFileSystem` returns the transient `creating` state and
//! settles to `available` on the next describe; a completed CloudFormation
//! resource is already settled, so the provisioner stores `available` directly
//! -- which also lets a same-stack mount target pass the
//! `IncorrectFileSystemLifeCycleState` guard the direct `CreateMountTarget`
//! enforces.

use serde_json::{json, Map, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

impl ResourceProvisioner {
    // ---------------------------------------------------------- FileSystem

    pub(super) fn create_efs_file_system(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let fsid = format!("fs-{}", efs_hex17());
        let arn = self.efs_fs_arn(&fsid);

        let performance_mode = efs_str(props, "PerformanceMode").unwrap_or("generalPurpose");
        let throughput_mode = efs_str(props, "ThroughputMode").unwrap_or("bursting");
        // ThroughputMode=provisioned requires ProvisionedThroughputInMibps (min
        // 1 MiBps); any other mode must not carry one -- mirrors the direct
        // CreateFileSystem validation.
        if throughput_mode == "provisioned" {
            match props
                .get("ProvisionedThroughputInMibps")
                .and_then(Value::as_f64)
            {
                None => {
                    return Err(
                        "ProvisionedThroughputInMibps is required when ThroughputMode is set to provisioned."
                            .to_string(),
                    );
                }
                Some(v) if v < 1.0 => {
                    return Err(
                        "Value at 'ProvisionedThroughputInMibps' failed to satisfy constraint: Member must have value greater than or equal to 1"
                            .to_string(),
                    );
                }
                Some(_) => {}
            }
        } else if props.get("ProvisionedThroughputInMibps").is_some() {
            return Err(
                "ProvisionedThroughputInMibps is only applicable when ThroughputMode is set to provisioned."
                    .to_string(),
            );
        }
        let encrypted = props
            .get("Encrypted")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let mut fs = Map::new();
        fs.insert("OwnerId".into(), json!(self.account_id));
        // CloudFormation does not expose a CreationToken; synthesize one so the
        // stored description carries the same field the direct API always sets.
        fs.insert(
            "CreationToken".into(),
            json!(efs_str(props, "CreationToken").unwrap_or(&resource.logical_id)),
        );
        fs.insert("FileSystemId".into(), json!(fsid));
        fs.insert("FileSystemArn".into(), json!(arn));
        fs.insert("CreationTime".into(), json!(efs_now_ts()));
        // A completed CloudFormation resource is already settled to `available`.
        fs.insert("LifeCycleState".into(), json!("available"));
        fs.insert("NumberOfMountTargets".into(), json!(0));
        fs.insert(
            "SizeInBytes".into(),
            json!({
                "Value": 6144,
                "Timestamp": efs_now_ts(),
                "ValueInIA": 0,
                "ValueInStandard": 6144,
                "ValueInArchive": 0
            }),
        );
        fs.insert("PerformanceMode".into(), json!(performance_mode));
        fs.insert("ThroughputMode".into(), json!(throughput_mode));
        fs.insert("Encrypted".into(), json!(encrypted));
        if encrypted {
            let kms = efs_str(props, "KmsKeyId")
                .map(str::to_string)
                .unwrap_or_else(|| {
                    format!(
                        "arn:aws:kms:{}:{}:key/{}",
                        self.region,
                        self.account_id,
                        uuid::Uuid::new_v4()
                    )
                });
            fs.insert("KmsKeyId".into(), json!(kms));
        }
        if throughput_mode == "provisioned" {
            if let Some(p) = props.get("ProvisionedThroughputInMibps") {
                fs.insert("ProvisionedThroughputInMibps".into(), p.clone());
            }
        }
        if let Some(az) = efs_str(props, "AvailabilityZoneName") {
            fs.insert("AvailabilityZoneName".into(), json!(az));
            fs.insert(
                "AvailabilityZoneId".into(),
                json!(format!("{}-az1", self.region)),
            );
        }

        let tag_map = efs_fs_tags(props);
        if let Some(name) = tag_map.get("Name") {
            fs.insert("Name".into(), json!(name));
        }
        fs.insert("Tags".into(), efs_tags_list(&tag_map));
        fs.insert(
            "FileSystemProtection".into(),
            json!({ "ReplicationOverwriteProtection": "ENABLED" }),
        );

        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        if !tag_map.is_empty() {
            let entry = data.tags.entry(fsid.clone()).or_default();
            for (k, v) in &tag_map {
                entry.insert(k.clone(), v.clone());
            }
        }
        // Honor the optional configuration properties through the same state
        // maps their dedicated Put* handlers write.
        if let Some(policies) = props.get("LifecyclePolicies").cloned() {
            data.lifecycle_configs.insert(fsid.clone(), policies);
        }
        if let Some(status) = efs_backup_status(props) {
            data.backup_policies.insert(fsid.clone(), status);
        }
        if let Some(policy) = efs_policy_string(props, "FileSystemPolicy") {
            data.file_system_policies.insert(fsid.clone(), policy);
        }
        data.file_systems.insert(fsid.clone(), Value::Object(fs));

        Ok(ProvisionResult::new(fsid.clone())
            .with("Arn", arn)
            .with("FileSystemId", fsid))
    }

    pub(super) fn update_efs_file_system(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let fsid = existing.physical_id.clone();
        // Encrypted / KmsKeyId / PerformanceMode / AvailabilityZoneName are
        // replacement-required on the CFN resource; a change to any recreates.
        let (old_encrypted, old_pm, old_kms, old_az) = {
            let guard = self.efs_state.read();
            let fs = guard
                .get(&self.account_id)
                .and_then(|d| d.file_systems.get(&fsid));
            (
                fs.and_then(|f| f.get("Encrypted").and_then(Value::as_bool))
                    .unwrap_or(false),
                fs.and_then(|f| f.get("PerformanceMode").and_then(Value::as_str))
                    .unwrap_or("generalPurpose")
                    .to_string(),
                fs.and_then(|f| f.get("KmsKeyId").and_then(Value::as_str))
                    .map(str::to_string),
                fs.and_then(|f| f.get("AvailabilityZoneName").and_then(Value::as_str))
                    .map(str::to_string),
            )
        };
        let new_encrypted = props
            .get("Encrypted")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let new_pm = efs_str(props, "PerformanceMode").unwrap_or("generalPurpose");
        let new_kms = efs_str(props, "KmsKeyId").map(str::to_string);
        let new_az = efs_str(props, "AvailabilityZoneName").map(str::to_string);
        if new_encrypted != old_encrypted
            || new_pm != old_pm
            || (new_kms.is_some() && new_kms != old_kms)
            || (new_az.is_some() && new_az != old_az)
        {
            self.delete_efs_file_system(&fsid);
            return self.create_efs_file_system(resource);
        }

        // In-place: throughput mode / provisioned throughput, lifecycle, backup,
        // policy and tags are the no-interruption properties.
        let new_tm = efs_str(props, "ThroughputMode").map(str::to_string);
        let tag_map = efs_fs_tags(props);
        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        {
            let fs = data
                .file_systems
                .get_mut(&fsid)
                .ok_or_else(|| format!("EFS file system {fsid} not yet provisioned"))?;
            let obj = fs.as_object_mut().unwrap();
            if let Some(tm) = &new_tm {
                obj.insert("ThroughputMode".into(), json!(tm));
            }
            if let Some(p) = props.get("ProvisionedThroughputInMibps") {
                obj.insert("ProvisionedThroughputInMibps".into(), p.clone());
            }
            if obj.get("ThroughputMode").and_then(Value::as_str) != Some("provisioned") {
                obj.remove("ProvisionedThroughputInMibps");
            }
            obj.insert("Tags".into(), efs_tags_list(&tag_map));
            match tag_map.get("Name") {
                Some(name) => {
                    obj.insert("Name".into(), json!(name));
                }
                None => {
                    obj.remove("Name");
                }
            }
        }
        if tag_map.is_empty() {
            data.tags.remove(&fsid);
        } else {
            let entry = data.tags.entry(fsid.clone()).or_default();
            entry.clear();
            for (k, v) in &tag_map {
                entry.insert(k.clone(), v.clone());
            }
        }
        match props.get("LifecyclePolicies").cloned() {
            Some(policies) => {
                data.lifecycle_configs.insert(fsid.clone(), policies);
            }
            None => {
                data.lifecycle_configs.remove(&fsid);
            }
        }
        match efs_backup_status(props) {
            Some(status) => {
                data.backup_policies.insert(fsid.clone(), status);
            }
            None => {
                data.backup_policies.remove(&fsid);
            }
        }
        match efs_policy_string(props, "FileSystemPolicy") {
            Some(policy) => {
                data.file_system_policies.insert(fsid.clone(), policy);
            }
            None => {
                data.file_system_policies.remove(&fsid);
            }
        }
        Ok(ProvisionResult::new(fsid.clone())
            .with("Arn", self.efs_fs_arn(&fsid))
            .with("FileSystemId", fsid))
    }

    pub(super) fn get_att_efs_file_system(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.efs_state.read();
        let fs = guard.get(&self.account_id)?.file_systems.get(physical_id)?;
        let key = match attribute {
            "Arn" => "FileSystemArn",
            "FileSystemId" => "FileSystemId",
            _ => return None,
        };
        fs.get(key).and_then(Value::as_str).map(str::to_string)
    }

    pub(super) fn delete_efs_file_system(&self, physical_id: &str) {
        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.file_systems.remove(physical_id);
        data.tags.remove(physical_id);
        data.lifecycle_configs.remove(physical_id);
        data.backup_policies.remove(physical_id);
        data.file_system_policies.remove(physical_id);
        data.replications.remove(physical_id);
    }

    // --------------------------------------------------------- MountTarget

    pub(super) fn create_efs_mount_target(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let fsid = efs_normalize_fs_id(efs_str(props, "FileSystemId").unwrap_or(""));
        let subnet_id = efs_str(props, "SubnetId")
            .ok_or("AWS::EFS::MountTarget requires SubnetId")?
            .to_string();

        // Resolve the subnet's real Availability Zone / VPC from EC2 state,
        // exactly as the direct CreateMountTarget does. A subnet that does not
        // resolve is rejected the same way (`SubnetNotFound`).
        let (az_name, az_id, vpc_id) = self
            .efs_resolve_subnet(&subnet_id)
            .ok_or_else(|| format!("The subnet ID '{subnet_id}' is invalid or does not exist."))?;
        let h = efs_hash(&subnet_id);

        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        // The file system must exist and be `available`. A CFN file system in the
        // same stack is provisioned first (dependency order) and stored already
        // settled, so it is available here.
        let fs = data
            .file_systems
            .get(&fsid)
            .ok_or_else(|| format!("File system '{fsid}' does not exist."))?;
        let fs_state = fs
            .get("LifeCycleState")
            .and_then(Value::as_str)
            .unwrap_or("");
        if fs_state != "available" {
            return Err(format!(
                "File system '{fsid}' is in life cycle state '{fs_state}' but must be 'available' for this operation."
            ));
        }
        // One mount target per Availability Zone per file system.
        for mt in data.mount_targets.values() {
            if mt.get("FileSystemId").and_then(Value::as_str) == Some(fsid.as_str())
                && mt.get("AvailabilityZoneName").and_then(Value::as_str) == Some(az_name.as_str())
            {
                return Err(
                    "A mount target already exists in this Availability Zone for the file system."
                        .to_string(),
                );
            }
        }

        let mtid = format!("fsmt-{}", efs_hex17());
        let ip = efs_str(props, "IpAddress")
            .map(str::to_string)
            .unwrap_or_else(|| format!("10.0.{}.{}", (h >> 8) % 256, h % 254 + 1));
        let eni_id = format!("eni-{}", efs_hex17());
        let security_groups: Vec<String> = props
            .get("SecurityGroups")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .filter(|v: &Vec<String>| !v.is_empty())
            .unwrap_or_else(|| vec![format!("sg-{:017x}", h & 0x000f_ffff_ffff_ffff)]);

        let mut mt = Map::new();
        mt.insert("OwnerId".into(), json!(self.account_id));
        mt.insert("MountTargetId".into(), json!(mtid));
        mt.insert("FileSystemId".into(), json!(fsid));
        mt.insert("SubnetId".into(), json!(subnet_id));
        mt.insert("LifeCycleState".into(), json!("available"));
        mt.insert("IpAddress".into(), json!(ip));
        mt.insert("NetworkInterfaceId".into(), json!(eni_id));
        mt.insert("AvailabilityZoneId".into(), json!(az_id));
        mt.insert("AvailabilityZoneName".into(), json!(az_name));
        mt.insert("VpcId".into(), json!(vpc_id));

        data.mount_target_security_groups
            .insert(mtid.clone(), security_groups);
        data.mount_targets.insert(mtid.clone(), Value::Object(mt));

        Ok(ProvisionResult::new(mtid.clone())
            .with("Id", mtid)
            .with("IpAddress", ip))
    }

    pub(super) fn update_efs_mount_target(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mtid = existing.physical_id.clone();
        // FileSystemId / SubnetId / IpAddress are replacement-required; only
        // SecurityGroups can change in place (ModifyMountTargetSecurityGroups).
        let (old_fs, old_subnet, old_ip) = {
            let guard = self.efs_state.read();
            let mt = guard
                .get(&self.account_id)
                .and_then(|d| d.mount_targets.get(&mtid));
            (
                mt.and_then(|m| m.get("FileSystemId").and_then(Value::as_str))
                    .map(str::to_string),
                mt.and_then(|m| m.get("SubnetId").and_then(Value::as_str))
                    .map(str::to_string),
                mt.and_then(|m| m.get("IpAddress").and_then(Value::as_str))
                    .map(str::to_string),
            )
        };
        let new_fs = efs_str(props, "FileSystemId").map(efs_normalize_fs_id);
        let new_subnet = efs_str(props, "SubnetId").map(str::to_string);
        let new_ip = efs_str(props, "IpAddress").map(str::to_string);
        if new_fs != old_fs || new_subnet != old_subnet || (new_ip.is_some() && new_ip != old_ip) {
            self.delete_efs_mount_target(&mtid);
            return self.create_efs_mount_target(resource);
        }

        let ip = old_ip.unwrap_or_default();
        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(sgs) = props.get("SecurityGroups").and_then(Value::as_array) {
            let sgs: Vec<String> = sgs
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
            data.mount_target_security_groups.insert(mtid.clone(), sgs);
        }
        Ok(ProvisionResult::new(mtid.clone())
            .with("Id", mtid)
            .with("IpAddress", ip))
    }

    pub(super) fn get_att_efs_mount_target(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.efs_state.read();
        let mt = guard
            .get(&self.account_id)?
            .mount_targets
            .get(physical_id)?;
        let key = match attribute {
            "Id" => "MountTargetId",
            "IpAddress" => "IpAddress",
            _ => return None,
        };
        mt.get(key).and_then(Value::as_str).map(str::to_string)
    }

    pub(super) fn delete_efs_mount_target(&self, physical_id: &str) {
        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.mount_targets.remove(physical_id);
        data.mount_target_security_groups.remove(physical_id);
    }

    // --------------------------------------------------------- AccessPoint

    pub(super) fn create_efs_access_point(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let fsid = efs_normalize_fs_id(efs_str(props, "FileSystemId").unwrap_or(""));
        let apid = format!("fsap-{}", efs_hex17());
        let arn = self.efs_ap_arn(&apid);
        let tag_map = efs_ap_tags(props);

        let mut ap = Map::new();
        ap.insert("ClientToken".into(), json!(resource.logical_id));
        if let Some(name) = tag_map.get("Name") {
            ap.insert("Name".into(), json!(name));
        }
        ap.insert("Tags".into(), efs_tags_list(&tag_map));
        ap.insert("AccessPointId".into(), json!(apid));
        ap.insert("AccessPointArn".into(), json!(arn));
        ap.insert("FileSystemId".into(), json!(fsid));
        if let Some(pu) = props.get("PosixUser") {
            ap.insert("PosixUser".into(), efs_coerce_posix_user(pu));
        }
        if let Some(rd) = props.get("RootDirectory") {
            ap.insert("RootDirectory".into(), efs_coerce_root_directory(rd));
        } else {
            ap.insert("RootDirectory".into(), json!({ "Path": "/" }));
        }
        ap.insert("OwnerId".into(), json!(self.account_id));
        ap.insert("LifeCycleState".into(), json!("available"));

        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        if !data.file_systems.contains_key(&fsid) {
            return Err(format!("File system '{fsid}' does not exist."));
        }
        if !tag_map.is_empty() {
            let entry = data.tags.entry(apid.clone()).or_default();
            for (k, v) in &tag_map {
                entry.insert(k.clone(), v.clone());
            }
        }
        data.access_points.insert(apid.clone(), Value::Object(ap));

        Ok(ProvisionResult::new(apid.clone())
            .with("Arn", arn)
            .with("AccessPointId", apid))
    }

    pub(super) fn update_efs_access_point(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let apid = existing.physical_id.clone();
        // FileSystemId / PosixUser / RootDirectory are replacement-required;
        // only tags change in place.
        let (old_fs, old_pu, old_rd) = {
            let guard = self.efs_state.read();
            let ap = guard
                .get(&self.account_id)
                .and_then(|d| d.access_points.get(&apid));
            (
                ap.and_then(|a| a.get("FileSystemId").and_then(Value::as_str))
                    .map(efs_normalize_fs_id),
                ap.and_then(|a| a.get("PosixUser").cloned()),
                ap.and_then(|a| a.get("RootDirectory").cloned()),
            )
        };
        let new_fs = efs_str(props, "FileSystemId").map(efs_normalize_fs_id);
        let new_pu = props.get("PosixUser").map(efs_coerce_posix_user);
        let new_rd = props.get("RootDirectory").map(efs_coerce_root_directory);
        let rd_default = json!({ "Path": "/" });
        if new_fs != old_fs
            || new_pu != old_pu
            || new_rd.as_ref().unwrap_or(&rd_default) != old_rd.as_ref().unwrap_or(&rd_default)
        {
            self.delete_efs_access_point(&apid);
            return self.create_efs_access_point(resource);
        }

        let tag_map = efs_ap_tags(props);
        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(ap) = data.access_points.get_mut(&apid) {
            let obj = ap.as_object_mut().unwrap();
            obj.insert("Tags".into(), efs_tags_list(&tag_map));
            match tag_map.get("Name") {
                Some(name) => {
                    obj.insert("Name".into(), json!(name));
                }
                None => {
                    obj.remove("Name");
                }
            }
        }
        if tag_map.is_empty() {
            data.tags.remove(&apid);
        } else {
            let entry = data.tags.entry(apid.clone()).or_default();
            entry.clear();
            for (k, v) in &tag_map {
                entry.insert(k.clone(), v.clone());
            }
        }
        Ok(ProvisionResult::new(apid.clone())
            .with("Arn", self.efs_ap_arn(&apid))
            .with("AccessPointId", apid))
    }

    pub(super) fn get_att_efs_access_point(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.efs_state.read();
        let ap = guard
            .get(&self.account_id)?
            .access_points
            .get(physical_id)?;
        let key = match attribute {
            "Arn" => "AccessPointArn",
            "AccessPointId" => "AccessPointId",
            _ => return None,
        };
        ap.get(key).and_then(Value::as_str).map(str::to_string)
    }

    pub(super) fn delete_efs_access_point(&self, physical_id: &str) {
        let mut guard = self.efs_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.access_points.remove(physical_id);
        data.tags.remove(physical_id);
    }

    // ------------------------------------------------------------- helpers

    fn efs_fs_arn(&self, fsid: &str) -> String {
        format!(
            "arn:aws:elasticfilesystem:{}:{}:file-system/{}",
            self.region, self.account_id, fsid
        )
    }

    fn efs_ap_arn(&self, apid: &str) -> String {
        format!(
            "arn:aws:elasticfilesystem:{}:{}:access-point/{}",
            self.region, self.account_id, apid
        )
    }

    /// Resolve a subnet's `(availability_zone, availability_zone_id, vpc_id)`
    /// from EC2 state, mirroring the direct `CreateMountTarget` resolution so a
    /// mount target on a (CFN- or API-created) subnet reports the true AZ/VPC.
    fn efs_resolve_subnet(&self, subnet_id: &str) -> Option<(String, String, String)> {
        let guard = self.ec2_state.read();
        let subnet = guard.get(&self.account_id)?.subnets.get(subnet_id)?;
        Some((
            subnet.availability_zone.clone(),
            subnet.availability_zone_id.clone(),
            subnet.vpc_id.clone(),
        ))
    }
}

/// Read a non-empty string property.
fn efs_str<'a>(props: &'a Value, key: &str) -> Option<&'a str> {
    props
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// Normalize a `FileSystemId` that may arrive as a bare `fs-...` id or as a full
/// ARN (`.../file-system/fs-...`) into the bare id.
fn efs_normalize_fs_id(raw: &str) -> String {
    raw.rsplit('/').next().unwrap_or(raw).to_string()
}

/// 17 lowercase hex characters, matching EFS's resource-id suffix form.
fn efs_hex17() -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .filter(char::is_ascii_hexdigit)
        .take(17)
        .collect()
}

fn efs_now_ts() -> f64 {
    chrono::Utc::now().timestamp() as f64
}

/// FNV-1a hash for deterministic synthesis of an IP / security group from a
/// subnet id, matching the direct handler.
fn efs_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Convert a CFN tag list (`[{Key,Value}]`) at the given property into a flat
/// key/value map. `AWS::EFS::FileSystem` uses `FileSystemTags` (falling back to
/// `Tags`); `AWS::EFS::AccessPoint` uses `AccessPointTags`.
fn efs_tags_at(props: &Value, keys: &[&str]) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    for key in keys {
        if let Some(arr) = props.get(*key).and_then(Value::as_array) {
            for t in arr {
                if let (Some(k), Some(v)) = (
                    t.get("Key").and_then(Value::as_str),
                    t.get("Value").and_then(Value::as_str),
                ) {
                    out.insert(k.to_string(), v.to_string());
                }
            }
        }
    }
    out
}

fn efs_fs_tags(props: &Value) -> std::collections::BTreeMap<String, String> {
    efs_tags_at(props, &["FileSystemTags", "Tags"])
}

fn efs_ap_tags(props: &Value) -> std::collections::BTreeMap<String, String> {
    efs_tags_at(props, &["AccessPointTags", "Tags"])
}

/// Render a tag map into the EFS `[{Key,Value}]` description list shape.
fn efs_tags_list(m: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Array(
        m.iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect(),
    )
}

/// The `BackupPolicy` property's `Status` string, if present.
fn efs_backup_status(props: &Value) -> Option<String> {
    props
        .get("BackupPolicy")
        .and_then(|p| p.get("Status"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// CloudFormation passes EFS POSIX ids as strings (`AWS::EFS::AccessPoint`
/// PosixUser.Uid / .Gid / .SecondaryGids are `String` in the resource spec),
/// but the EFS API models them as `long`. Coerce the numeric-string fields to
/// JSON numbers so the stored `AccessPointDescription` round-trips through the
/// typed `DescribeAccessPoints` response.
fn efs_coerce_posix_user(pu: &Value) -> Value {
    let mut obj = pu.as_object().cloned().unwrap_or_default();
    for key in ["Uid", "Gid"] {
        if let Some(n) = obj.get(key).and_then(efs_as_i64) {
            obj.insert(key.into(), json!(n));
        }
    }
    if let Some(arr) = obj.get("SecondaryGids").and_then(Value::as_array) {
        let nums: Vec<Value> = arr
            .iter()
            .filter_map(|v| efs_as_i64(v).map(|n| json!(n)))
            .collect();
        obj.insert("SecondaryGids".into(), Value::Array(nums));
    }
    Value::Object(obj)
}

/// Coerce the numeric-string `CreationInfo.OwnerUid` / `.OwnerGid` fields of a
/// `RootDirectory` to JSON numbers, for the same reason as
/// `efs_coerce_posix_user`. `Path` and `CreationInfo.Permissions` stay strings.
fn efs_coerce_root_directory(rd: &Value) -> Value {
    let mut obj = rd.as_object().cloned().unwrap_or_default();
    if let Some(ci) = obj.get("CreationInfo").and_then(Value::as_object) {
        let mut ci = ci.clone();
        for key in ["OwnerUid", "OwnerGid"] {
            if let Some(n) = ci.get(key).and_then(efs_as_i64) {
                ci.insert(key.into(), json!(n));
            }
        }
        obj.insert("CreationInfo".into(), Value::Object(ci));
    }
    Value::Object(obj)
}

/// Read an integer that may be encoded as a JSON number or a numeric string.
fn efs_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// A resource-policy property (`FileSystemPolicy`) as a JSON string, accepting
/// either an inline object or an already-serialized string.
fn efs_policy_string(props: &Value, key: &str) -> Option<String> {
    match props.get(key) {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}
