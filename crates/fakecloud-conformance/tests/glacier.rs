mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("glacier", "AbortMultipartUpload", checksum = "b05b6bf7")]
#[test_action("glacier", "AbortVaultLock", checksum = "6f2373f9")]
#[test_action("glacier", "AddTagsToVault", checksum = "3196cdef")]
#[test_action("glacier", "CompleteMultipartUpload", checksum = "1653e7e7")]
#[test_action("glacier", "CompleteVaultLock", checksum = "bba31e04")]
#[test_action("glacier", "CreateVault", checksum = "4a456704")]
#[test_action("glacier", "DeleteArchive", checksum = "01020081")]
#[test_action("glacier", "DeleteVault", checksum = "cfd1e56b")]
#[test_action("glacier", "DeleteVaultAccessPolicy", checksum = "953f0900")]
#[test_action("glacier", "DeleteVaultNotifications", checksum = "435cade4")]
#[test_action("glacier", "DescribeJob", checksum = "86c26b21")]
#[test_action("glacier", "DescribeVault", checksum = "95cd822f")]
#[test_action("glacier", "GetDataRetrievalPolicy", checksum = "93a35941")]
#[test_action("glacier", "GetJobOutput", checksum = "0bcbb2f2")]
#[test_action("glacier", "GetVaultAccessPolicy", checksum = "bb89ba86")]
#[test_action("glacier", "GetVaultLock", checksum = "7633459b")]
#[test_action("glacier", "GetVaultNotifications", checksum = "33d1df1e")]
#[test_action("glacier", "InitiateJob", checksum = "3497d2f4")]
#[test_action("glacier", "InitiateMultipartUpload", checksum = "ed8c68e2")]
#[test_action("glacier", "InitiateVaultLock", checksum = "2782ac1b")]
#[test_action("glacier", "ListJobs", checksum = "bad731f6")]
#[test_action("glacier", "ListMultipartUploads", checksum = "fe2578cb")]
#[test_action("glacier", "ListParts", checksum = "e44b586c")]
#[test_action("glacier", "ListProvisionedCapacity", checksum = "c4860a10")]
#[test_action("glacier", "ListTagsForVault", checksum = "3df4b309")]
#[test_action("glacier", "ListVaults", checksum = "16ae23f3")]
#[test_action("glacier", "PurchaseProvisionedCapacity", checksum = "12e694cf")]
#[test_action("glacier", "RemoveTagsFromVault", checksum = "00b2ce78")]
#[test_action("glacier", "SetDataRetrievalPolicy", checksum = "17fc1134")]
#[test_action("glacier", "SetVaultAccessPolicy", checksum = "bf95ac10")]
#[test_action("glacier", "SetVaultNotifications", checksum = "22823d86")]
#[test_action("glacier", "UploadArchive", checksum = "edb45cf6")]
#[test_action("glacier", "UploadMultipartPart", checksum = "3918fdaf")]
#[tokio::test]
async fn glacier_probe() {
    let _server = TestServer::start().await;
}
