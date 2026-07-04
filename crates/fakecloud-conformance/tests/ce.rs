//! Cost Explorer (`ce`) awsJson1.1 conformance probe assertions.
//!
//! One `#[test_action]` per operation in the Cost Explorer Smithy model.
//! Checksums are harvested from `aws-models/ce.json`; the probe generates
//! request variants per operation and asserts fakecloud responds AWS-faithfully.

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("ce", "CreateAnomalyMonitor", checksum = "e875d3f7")]
#[test_action("ce", "CreateAnomalySubscription", checksum = "9b2f8be2")]
#[test_action("ce", "CreateCostCategoryDefinition", checksum = "b5545cd5")]
#[test_action("ce", "DeleteAnomalyMonitor", checksum = "a3fa53d7")]
#[test_action("ce", "DeleteAnomalySubscription", checksum = "c5032099")]
#[test_action("ce", "DeleteCostCategoryDefinition", checksum = "29e4da68")]
#[test_action("ce", "DescribeCostCategoryDefinition", checksum = "d5928bb7")]
#[test_action("ce", "GetAnomalies", checksum = "ed1489c4")]
#[test_action("ce", "GetAnomalyMonitors", checksum = "5910d781")]
#[test_action("ce", "GetAnomalySubscriptions", checksum = "0794d3d6")]
#[test_action("ce", "GetApproximateUsageRecords", checksum = "1403f771")]
#[test_action("ce", "GetCommitmentPurchaseAnalysis", checksum = "1de674c1")]
#[test_action("ce", "GetCostAndUsage", checksum = "b64f262f")]
#[test_action("ce", "GetCostAndUsageComparisons", checksum = "a03839aa")]
#[test_action("ce", "GetCostAndUsageWithResources", checksum = "67e6f83f")]
#[test_action("ce", "GetCostCategories", checksum = "31cf265d")]
#[test_action("ce", "GetCostComparisonDrivers", checksum = "3e769537")]
#[test_action("ce", "GetCostForecast", checksum = "557eadd6")]
#[test_action("ce", "GetDimensionValues", checksum = "c15d1b97")]
#[test_action("ce", "GetReservationCoverage", checksum = "cfbb1895")]
#[test_action("ce", "GetReservationPurchaseRecommendation", checksum = "b0a50e20")]
#[test_action("ce", "GetReservationUtilization", checksum = "2ec1772e")]
#[test_action("ce", "GetRightsizingRecommendation", checksum = "955dc66c")]
#[test_action(
    "ce",
    "GetSavingsPlanPurchaseRecommendationDetails",
    checksum = "6a75ee52"
)]
#[test_action("ce", "GetSavingsPlansCoverage", checksum = "40bc2884")]
#[test_action("ce", "GetSavingsPlansPurchaseRecommendation", checksum = "51edbf6b")]
#[test_action("ce", "GetSavingsPlansUtilization", checksum = "cef19887")]
#[test_action("ce", "GetSavingsPlansUtilizationDetails", checksum = "d6ab7c03")]
#[test_action("ce", "GetTags", checksum = "651da33b")]
#[test_action("ce", "GetUsageForecast", checksum = "1351038e")]
#[test_action("ce", "ListCommitmentPurchaseAnalyses", checksum = "157e17d4")]
#[test_action("ce", "ListCostAllocationTagBackfillHistory", checksum = "ad18fc5d")]
#[test_action("ce", "ListCostAllocationTags", checksum = "e8d57b62")]
#[test_action("ce", "ListCostCategoryDefinitions", checksum = "37c0a9bd")]
#[test_action("ce", "ListCostCategoryResourceAssociations", checksum = "3aa65c91")]
#[test_action(
    "ce",
    "ListSavingsPlansPurchaseRecommendationGeneration",
    checksum = "1a282f7b"
)]
#[test_action("ce", "ListTagsForResource", checksum = "063af854")]
#[test_action("ce", "ProvideAnomalyFeedback", checksum = "c88c4556")]
#[test_action("ce", "StartCommitmentPurchaseAnalysis", checksum = "87ff29d7")]
#[test_action("ce", "StartCostAllocationTagBackfill", checksum = "f45259ee")]
#[test_action(
    "ce",
    "StartSavingsPlansPurchaseRecommendationGeneration",
    checksum = "6b6a60bf"
)]
#[test_action("ce", "TagResource", checksum = "75b89fbc")]
#[test_action("ce", "UntagResource", checksum = "ffe1ddfc")]
#[test_action("ce", "UpdateAnomalyMonitor", checksum = "7bbf5ff9")]
#[test_action("ce", "UpdateAnomalySubscription", checksum = "004212b7")]
#[test_action("ce", "UpdateCostAllocationTagsStatus", checksum = "ecb6b357")]
#[test_action("ce", "UpdateCostCategoryDefinition", checksum = "09058d11")]
#[tokio::test]
async fn ce_probe() {
    let _server = TestServer::start().await;
}
