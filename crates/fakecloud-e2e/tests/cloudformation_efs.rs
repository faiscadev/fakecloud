//! CloudFormation provisions AWS::EFS::FileSystem, AWS::EFS::MountTarget and
//! AWS::EFS::AccessPoint as real records in the `elasticfilesystem` service
//! control plane: they read back through DescribeFileSystems /
//! DescribeMountTargets / DescribeAccessPoints, expose their id via Ref and
//! every documented attribute via Fn::GetAtt, and honor dependency order (the
//! mount target and access point resolve their FileSystemId from the file
//! system's Ref; the mount target resolves its SubnetId from a CFN-created
//! subnet). Deleting the stack removes every resource.

mod helpers;

use helpers::TestServer;

// A VPC + subnet (so the mount target has a real subnet to resolve its AZ/VPC
// from, exactly as the direct CreateMountTarget does), a file system, a mount
// target in that subnet, and an access point. The mount target / access point
// FileSystemId resolve from the file system's Ref, and the mount target's
// SubnetId from the subnet's Ref, so all three EFS resources are provisioned
// after their dependencies -- exercising dependency ordering. Outputs surface
// Ref (the resource id, per the AWS resource specs) and each GetAtt attribute.
const TEMPLATE: &str = r#"{
  "Resources": {
    "MyVpc": {
      "Type": "AWS::EC2::VPC",
      "Properties": { "CidrBlock": "10.0.0.0/16" }
    },
    "MySubnet": {
      "Type": "AWS::EC2::Subnet",
      "Properties": {
        "VpcId": { "Ref": "MyVpc" },
        "CidrBlock": "10.0.1.0/24",
        "AvailabilityZone": "us-east-1a"
      }
    },
    "MyFs": {
      "Type": "AWS::EFS::FileSystem",
      "Properties": {
        "Encrypted": true,
        "PerformanceMode": "generalPurpose",
        "FileSystemTags": [ { "Key": "Name", "Value": "cfn-efs" } ]
      }
    },
    "MyMt": {
      "Type": "AWS::EFS::MountTarget",
      "Properties": {
        "FileSystemId": { "Ref": "MyFs" },
        "SubnetId": { "Ref": "MySubnet" }
      }
    },
    "MyAp": {
      "Type": "AWS::EFS::AccessPoint",
      "Properties": {
        "FileSystemId": { "Ref": "MyFs" },
        "PosixUser": { "Uid": "1000", "Gid": "1000" },
        "RootDirectory": { "Path": "/data" },
        "AccessPointTags": [ { "Key": "env", "Value": "test" } ]
      }
    }
  },
  "Outputs": {
    "FsRef":  { "Value": { "Ref": "MyFs" } },
    "FsArn":  { "Value": { "Fn::GetAtt": ["MyFs", "Arn"] } },
    "FsId":   { "Value": { "Fn::GetAtt": ["MyFs", "FileSystemId"] } },
    "MtRef":  { "Value": { "Ref": "MyMt" } },
    "MtId":   { "Value": { "Fn::GetAtt": ["MyMt", "Id"] } },
    "MtIp":   { "Value": { "Fn::GetAtt": ["MyMt", "IpAddress"] } },
    "ApRef":  { "Value": { "Ref": "MyAp" } },
    "ApArn":  { "Value": { "Fn::GetAtt": ["MyAp", "Arn"] } },
    "ApId":   { "Value": { "Fn::GetAtt": ["MyAp", "AccessPointId"] } }
  }
}"#;

fn output<'a>(stack: &'a aws_sdk_cloudformation::types::Stack, key: &str) -> &'a str {
    stack
        .outputs()
        .iter()
        .find(|o| o.output_key() == Some(key))
        .and_then(|o| o.output_value())
        .unwrap_or_else(|| panic!("missing output {key}"))
}

#[tokio::test]
async fn cfn_provisions_efs_file_system_mount_target_and_access_point() {
    let s = TestServer::start().await;
    let cfg = s.aws_config().await;
    let cfn = s.cloudformation_client().await;
    let efs = aws_sdk_efs::Client::new(&cfg);

    cfn.create_stack()
        .stack_name("efs-stack")
        .template_body(TEMPLATE)
        .send()
        .await
        .expect("create_stack");

    let described = cfn
        .describe_stacks()
        .stack_name("efs-stack")
        .send()
        .await
        .unwrap();
    let stack = &described.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "CREATE_COMPLETE");

    // --- Intrinsic-function resolution (Ref + GetAtt) ---
    let fs_ref = output(stack, "FsRef");
    let fs_arn = output(stack, "FsArn");
    let fs_id = output(stack, "FsId");
    let mt_ref = output(stack, "MtRef");
    let mt_id = output(stack, "MtId");
    let mt_ip = output(stack, "MtIp");
    let ap_ref = output(stack, "ApRef");
    let ap_arn = output(stack, "ApArn");
    let ap_id = output(stack, "ApId");

    // FileSystem: Ref = FileSystemId; Arn is the FileSystemArn.
    assert!(fs_ref.starts_with("fs-"), "fs ref {fs_ref}");
    assert_eq!(fs_ref, fs_id);
    assert!(
        fs_arn.ends_with(&format!(":file-system/{fs_id}")),
        "fs arn {fs_arn}"
    );

    // MountTarget: Ref = MountTargetId; Id GetAtt is the same id; IpAddress set.
    assert!(mt_ref.starts_with("fsmt-"), "mt ref {mt_ref}");
    assert_eq!(mt_ref, mt_id);
    assert!(!mt_ip.is_empty(), "mount target has an IP address");

    // AccessPoint: Ref = AccessPointId; Arn is the AccessPointArn.
    assert!(ap_ref.starts_with("fsap-"), "ap ref {ap_ref}");
    assert_eq!(ap_ref, ap_id);
    assert!(
        ap_arn.ends_with(&format!(":access-point/{ap_id}")),
        "ap arn {ap_arn}"
    );

    // --- The resources exist in the EFS service ---
    let fs_out = efs
        .describe_file_systems()
        .file_system_id(fs_id)
        .send()
        .await
        .expect("DescribeFileSystems");
    let fs = &fs_out.file_systems()[0];
    assert_eq!(fs.file_system_id(), fs_id);
    assert_eq!(fs.encrypted(), Some(true));
    assert_eq!(fs.name(), Some("cfn-efs"));
    assert_eq!(fs.life_cycle_state().as_str(), "available");

    let mt_out = efs
        .describe_mount_targets()
        .file_system_id(fs_id)
        .send()
        .await
        .expect("DescribeMountTargets");
    assert!(
        mt_out
            .mount_targets()
            .iter()
            .any(|mt| mt.mount_target_id() == mt_id),
        "expected mount target {mt_id} on the file system"
    );

    let ap_out = efs
        .describe_access_points()
        .access_point_id(ap_id)
        .send()
        .await
        .expect("DescribeAccessPoints");
    let ap = &ap_out.access_points()[0];
    assert_eq!(ap.access_point_id(), Some(ap_id));
    assert_eq!(ap.file_system_id(), Some(fs_id));
    let posix = ap.posix_user().expect("posix user");
    assert_eq!(posix.uid(), 1000);
    assert_eq!(posix.gid(), 1000);
    assert_eq!(ap.root_directory().and_then(|r| r.path()), Some("/data"));

    // --- Deleting the stack removes every resource ---
    cfn.delete_stack()
        .stack_name("efs-stack")
        .send()
        .await
        .unwrap();

    let ap_gone = efs
        .describe_access_points()
        .access_point_id(ap_id)
        .send()
        .await;
    assert!(
        ap_gone.is_err(),
        "stack delete should remove the access point"
    );

    let mt_gone = efs
        .describe_mount_targets()
        .mount_target_id(mt_id)
        .send()
        .await;
    assert!(
        mt_gone.is_err(),
        "stack delete should remove the mount target"
    );

    let fs_gone = efs
        .describe_file_systems()
        .file_system_id(fs_id)
        .send()
        .await;
    assert!(
        fs_gone.is_err(),
        "stack delete should remove the file system"
    );
}
