//! IAM authorization for DynamoDB and DynamoDB Streams requests: which
//! `dynamodb:*` actions each operation needs, on which resources.
//!
//! Follows the AWS Service Authorization Reference for DynamoDB. Most
//! operations need their namesake action on one table. The exceptions:
//!
//! - A batch needs `BatchGetItem` / `BatchWriteItem` on every table it names.
//! - A transaction needs the per-item action (`GetItem`, `PutItem`,
//!   `UpdateItem`, `DeleteItem`, `ConditionCheckItem`) on each item's table;
//!   there is no `dynamodb:TransactWriteItems` action.
//! - PartiQL statements need `PartiQLSelect` / `PartiQLInsert` /
//!   `PartiQLUpdate` / `PartiQLDelete` on the table (or, for a SELECT, the
//!   index) each statement names.
//! - `Query`, `Scan` and the contributor-insights operations target the
//!   index when `IndexName` is given.
//! - `CreateTable` also needs `TagResource` when it carries `Tags` and
//!   `PutResourcePolicy` when it carries `ResourcePolicy`.
//! - A restore needs its own action on the source plus the data-plane
//!   actions DynamoDB uses to write the target table.
//!
//! A `TableName` given as an ARN authorizes against that ARN, so the
//! resource carries the table's own account and region.

use std::collections::HashMap;

use fakecloud_aws::arn::arn_resource;
use fakecloud_core::auth::IamAction;
use fakecloud_core::service::AwsRequest;
use serde_json::Value;

const SERVICE: &str = "dynamodb";

/// The data-plane actions DynamoDB performs on a restore's target table.
const RESTORE_TARGET_ACTIONS: [&str; 7] = [
    "BatchWriteItem",
    "DeleteItem",
    "GetItem",
    "PutItem",
    "Query",
    "Scan",
    "UpdateItem",
];

/// Resolves the tables a request names to the ARNs to authorize.
struct Scope<'a> {
    account: &'a str,
    region: &'a str,
    accounts: &'a fakecloud_core::multi_account::MultiRegionState<crate::state::DynamoDbState>,
    /// Whether the operation may act on another account's table.
    cross_account: bool,
}

impl Scope<'_> {
    /// The ARN to authorize for a `TableName` value (a name, or a table ARN).
    ///
    /// When the table exists this is its own stored ARN: the handler serves
    /// that table, looked up by name in the account, so authorizing an ARN
    /// built from the request's region -- or taken from a caller-written ARN
    /// -- would check a resource the request does not actually touch, and a
    /// policy scoped to the real table's region could be sidestepped. A table
    /// that does not exist yet (CreateTable) is authorized at the ARN it will
    /// get: the caller's account and the request's region.
    ///
    /// The handler serves an operation with cross-account support in the
    /// account a table ARN names, and any other operation in the caller's
    /// account -- where it does not find another account's table at all -- so
    /// the table looked up is the one in that account.
    fn table(&self, name_or_arn: &str) -> String {
        let (name, owner) = match table_arn_of(name_or_arn) {
            Some(arn) => {
                // A table in another region is never served by this request
                // (the handler answers not-found), so its ARN is authorized
                // as written -- never redirected to the same-named table of
                // the request's region.
                let region = arn.split(':').nth(3).filter(|r| !r.is_empty());
                if region.is_some_and(|r| r != self.region) {
                    return arn;
                }
                let owner = arn.split(':').nth(4).filter(|a| !a.is_empty());
                let name = arn
                    .rsplit("table/")
                    .next()
                    .unwrap_or(name_or_arn)
                    .to_string();
                match owner {
                    Some(owner) if owner != self.account && !self.cross_account => return arn,
                    Some(owner) => (name, owner.to_string()),
                    None => (name, self.account.to_string()),
                }
            }
            None => (name_or_arn.to_string(), self.account.to_string()),
        };
        if let Some(table) = self
            .accounts
            .regional(&owner, self.region)
            .and_then(|state| state.tables.get(&name))
        {
            return table.arn.clone();
        }
        match table_arn_of(name_or_arn) {
            Some(arn) => arn,
            None => crate::state::table_arn(self.region, self.account, name_or_arn),
        }
    }

    fn index(&self, table: &str, index: &str) -> String {
        format!("{}/index/{index}", self.table(table))
    }

    fn global_table(&self, name: &str) -> String {
        crate::state::global_table_arn(self.region, self.account, name)
    }
}

/// `arn:aws:dynamodb:REGION:ACCOUNT:table/NAME` for an ARN naming a table or
/// one of its sub-resources, or `None` for anything else.
fn table_arn_of(arn: &str) -> Option<String> {
    let rest = arn_resource(arn, "dynamodb")?;
    let (_, resource) = rest.split_once(":table/")?;
    let name = resource.split('/').next().filter(|n| !n.is_empty())?;
    let prefix_len = arn.len() - resource.len();
    Some(format!("{}{name}", &arn[..prefix_len]))
}

fn action(name: &'static str, resource: String) -> IamAction {
    IamAction {
        service: SERVICE,
        action: name,
        resource,
    }
}

/// A body string field, or `None` when absent or empty.
fn field<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body[name].as_str().filter(|s| !s.is_empty())
}

/// The `dynamodb:*` authorizations a DynamoDB request needs. Empty only for
/// an operation this service does not implement.
pub(crate) fn actions_for(
    state: &crate::state::SharedDynamoDbState,
    request: &AwsRequest,
) -> Vec<IamAction> {
    let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
    let account = request
        .principal
        .as_ref()
        .map(|p| p.account_id.as_str())
        .unwrap_or(request.account_id.as_str());
    let accounts = state.read();
    let scope = Scope {
        account,
        region: request.region.as_str(),
        accounts: &accounts,
        cross_account: super::cross_account::CROSS_ACCOUNT_OPERATIONS
            .contains(&request.action.as_str()),
    };
    let op: &'static str = match DYNAMODB_ACTIONS
        .iter()
        .find(|a| **a == request.action.as_str())
    {
        Some(op) => op,
        None => return Vec::new(),
    };
    // A required name that is missing still maps, to `*`: the request is
    // malformed, and the handler rejects it with its own validation error
    // once the caller is authorized for the operation at all.
    let table = |key: &str| field(&body, key).map_or_else(|| "*".to_string(), |t| scope.table(t));
    let table_or_index = || match (field(&body, "TableName"), field(&body, "IndexName")) {
        (Some(t), Some(i)) => scope.index(t, i),
        (Some(t), None) => scope.table(t),
        _ => "*".to_string(),
    };
    let arn_field = |key: &str| field(&body, key).unwrap_or("*").to_string();

    match op {
        "GetItem"
        | "PutItem"
        | "UpdateItem"
        | "DeleteItem"
        | "CreateBackup"
        | "DeleteTable"
        | "DescribeTable"
        | "UpdateTable"
        | "DescribeTimeToLive"
        | "UpdateTimeToLive"
        | "DescribeContinuousBackups"
        | "UpdateContinuousBackups"
        | "DescribeKinesisStreamingDestination"
        | "EnableKinesisStreamingDestination"
        | "DisableKinesisStreamingDestination"
        | "UpdateKinesisStreamingDestination"
        | "DescribeTableReplicaAutoScaling"
        | "UpdateTableReplicaAutoScaling" => {
            vec![action(op, table("TableName"))]
        }
        "Query"
        | "Scan"
        | "DescribeContributorInsights"
        | "UpdateContributorInsights"
        | "SearchVectors" => vec![action(op, table_or_index())],
        "ListTables"
        | "DescribeLimits"
        | "DescribeEndpoints"
        | "ListBackups"
        | "ListContributorInsights"
        | "ListGlobalTables" => vec![action(op, "*".to_string())],
        "ListExports" | "ListImports" => vec![action(op, table("TableArn"))],
        "ExportTableToPointInTime" => vec![action(op, table("TableArn"))],
        "DescribeBackup" | "DeleteBackup" => vec![action(op, arn_field("BackupArn"))],
        "DescribeExport" => vec![action(op, arn_field("ExportArn"))],
        "DescribeImport" => vec![action(op, arn_field("ImportArn"))],
        "StartBulkPutItems" => vec![action(op, table("TableName"))],
        "DescribeBulkOperation" | "CancelBulkOperation" => {
            vec![action(op, arn_field("BulkOperationArn"))]
        }
        "ListBulkOperations" => vec![action(op, "*".to_string())],
        "TagResource"
        | "UntagResource"
        | "ListTagsOfResource"
        | "GetResourcePolicy"
        | "PutResourcePolicy"
        | "DeleteResourcePolicy" => {
            vec![action(op, arn_field("ResourceArn"))]
        }
        "CreateTable" => {
            let resource = table("TableName");
            let mut out = vec![action("CreateTable", resource.clone())];
            if body["Tags"].as_array().is_some_and(|t| !t.is_empty()) {
                out.push(action("TagResource", resource.clone()));
            }
            if field(&body, "ResourcePolicy").is_some() {
                out.push(action("PutResourcePolicy", resource));
            }
            out
        }
        "ImportTable" => {
            let name = body["TableCreationParameters"]["TableName"]
                .as_str()
                .filter(|s| !s.is_empty());
            vec![action(
                "ImportTable",
                name.map_or_else(|| "*".to_string(), |n| scope.table(n)),
            )]
        }
        "RestoreTableFromBackup" => {
            let target = table("TargetTableName");
            let mut out = vec![
                action("RestoreTableFromBackup", arn_field("BackupArn")),
                action("RestoreTableFromBackup", target.clone()),
            ];
            out.extend(
                RESTORE_TARGET_ACTIONS
                    .iter()
                    .map(|a| action(a, target.clone())),
            );
            out
        }
        "RestoreTableToPointInTime" => {
            // The handler takes `SourceTableName` over `SourceTableArn`, so the
            // table authorized has to be chosen the same way.
            let source = field(&body, "SourceTableName")
                .or_else(|| field(&body, "SourceTableArn"))
                .map_or_else(|| "*".to_string(), |t| scope.table(t));
            let target = table("TargetTableName");
            let mut out = vec![action("RestoreTableToPointInTime", source)];
            out.extend(
                RESTORE_TARGET_ACTIONS
                    .iter()
                    .map(|a| action(a, target.clone())),
            );
            out
        }
        "CreateGlobalTable" | "UpdateGlobalTable" | "UpdateGlobalTableSettings" => {
            let name = field(&body, "GlobalTableName").unwrap_or("*");
            vec![
                action(op, scope.global_table(name)),
                action(op, scope.table(name)),
            ]
        }
        "DescribeGlobalTable" | "DescribeGlobalTableSettings" => {
            let name = field(&body, "GlobalTableName").unwrap_or("*");
            vec![action(op, scope.global_table(name))]
        }
        "BatchGetItem" | "BatchWriteItem" => {
            let tables = batch_table_names(&body["RequestItems"]);
            if tables.is_empty() {
                return vec![action(op, "*".to_string())];
            }
            tables
                .into_iter()
                .map(|t| action(op, scope.table(t)))
                .collect()
        }
        "TransactGetItems" | "TransactWriteItems" => {
            let mut out = Vec::new();
            for item in body["TransactItems"].as_array().into_iter().flatten() {
                for (member, item_action) in [
                    ("Get", "GetItem"),
                    ("Put", "PutItem"),
                    ("Update", "UpdateItem"),
                    ("Delete", "DeleteItem"),
                    ("ConditionCheck", "ConditionCheckItem"),
                ] {
                    if let Some(name) = item[member]["TableName"].as_str() {
                        push_unique(&mut out, action(item_action, scope.table(name)));
                    }
                }
            }
            if out.is_empty() {
                let fallback = if op == "TransactGetItems" {
                    "GetItem"
                } else {
                    "PutItem"
                };
                out.push(action(fallback, "*".to_string()));
            }
            out
        }
        "ExecuteStatement" => {
            let statement = field(&body, "Statement").unwrap_or("");
            vec![partiql_action(&scope, statement)]
        }
        "BatchExecuteStatement" | "ExecuteTransaction" => {
            let list = if op == "BatchExecuteStatement" {
                &body["Statements"]
            } else {
                &body["TransactStatements"]
            };
            let mut out = Vec::new();
            for statement in list.as_array().into_iter().flatten() {
                let text = statement["Statement"].as_str().unwrap_or("");
                push_unique(&mut out, partiql_action(&scope, text));
            }
            if out.is_empty() {
                out.push(action("PartiQLSelect", "*".to_string()));
            }
            out
        }
        _ => Vec::new(),
    }
}

/// The DynamoDB Streams operations' authorizations.
pub(crate) fn streams_actions_for(request: &AwsRequest) -> Vec<IamAction> {
    let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
    match request.action.as_str() {
        "ListStreams" => vec![action("ListStreams", "*".to_string())],
        "DescribeStream" => vec![action(
            "DescribeStream",
            field(&body, "StreamArn").unwrap_or("*").to_string(),
        )],
        "GetShardIterator" => vec![action(
            "GetShardIterator",
            field(&body, "StreamArn").unwrap_or("*").to_string(),
        )],
        // An iterator is `STREAM_ARN|SHARD|SEQUENCE`; the stream it reads is
        // the resource.
        "GetRecords" => {
            let stream = field(&body, "ShardIterator")
                .and_then(|it| it.split('|').next())
                .filter(|arn| arn.starts_with("arn:"))
                .unwrap_or("*");
            vec![action("GetRecords", stream.to_string())]
        }
        _ => Vec::new(),
    }
}

/// Every DynamoDB control- and data-plane operation this service serves.
const DYNAMODB_ACTIONS: &[&str] = &[
    "BatchExecuteStatement",
    "BatchGetItem",
    "BatchWriteItem",
    "CancelBulkOperation",
    "CreateBackup",
    "CreateGlobalTable",
    "CreateTable",
    "DeleteBackup",
    "DeleteItem",
    "DeleteResourcePolicy",
    "DeleteTable",
    "DescribeBackup",
    "DescribeBulkOperation",
    "DescribeContinuousBackups",
    "DescribeContributorInsights",
    "DescribeEndpoints",
    "DescribeExport",
    "DescribeGlobalTable",
    "DescribeGlobalTableSettings",
    "DescribeImport",
    "DescribeKinesisStreamingDestination",
    "DescribeLimits",
    "DescribeTable",
    "DescribeTableReplicaAutoScaling",
    "DescribeTimeToLive",
    "DisableKinesisStreamingDestination",
    "EnableKinesisStreamingDestination",
    "ExecuteStatement",
    "ExecuteTransaction",
    "ExportTableToPointInTime",
    "GetItem",
    "GetResourcePolicy",
    "ImportTable",
    "ListBackups",
    "ListBulkOperations",
    "ListContributorInsights",
    "ListExports",
    "ListGlobalTables",
    "ListImports",
    "ListTables",
    "ListTagsOfResource",
    "PutItem",
    "PutResourcePolicy",
    "Query",
    "RestoreTableFromBackup",
    "RestoreTableToPointInTime",
    "Scan",
    "SearchVectors",
    "StartBulkPutItems",
    "TagResource",
    "TransactGetItems",
    "TransactWriteItems",
    "UntagResource",
    "UpdateContinuousBackups",
    "UpdateContributorInsights",
    "UpdateGlobalTable",
    "UpdateGlobalTableSettings",
    "UpdateItem",
    "UpdateKinesisStreamingDestination",
    "UpdateTable",
    "UpdateTableReplicaAutoScaling",
    "UpdateTimeToLive",
];

fn batch_table_names(request_items: &Value) -> Vec<&str> {
    request_items
        .as_object()
        .map(|m| m.keys().map(String::as_str).collect())
        .unwrap_or_default()
}

fn push_unique(out: &mut Vec<IamAction>, a: IamAction) {
    if !out.contains(&a) {
        out.push(a);
    }
}

/// The PartiQL action a statement's verb needs, and the table it names.
/// `None` for a statement too malformed to name a table.
pub(crate) fn partiql_verb_and_table(statement: &str) -> Option<(&'static str, String)> {
    use super::helpers::partiql_parse::{statement_shape, Statement};
    let stmt = statement_shape(statement)?;
    let verb = match &stmt {
        Statement::Select { .. } | Statement::Exists(_) => "PartiQLSelect",
        Statement::Insert { .. } => "PartiQLInsert",
        Statement::Update { .. } => "PartiQLUpdate",
        Statement::Delete { .. } => "PartiQLDelete",
    };
    Some((verb, stmt.table().to_string()))
}

/// The PartiQL action a statement needs, on the table (or, for a SELECT
/// from `"table"."index"`, the index) it names. A statement too malformed to
/// name a table maps to `PartiQLSelect` on `*`, leaving the syntax error to
/// the handler.
fn partiql_action(scope: &Scope<'_>, statement: &str) -> IamAction {
    let Some((verb, table)) = partiql_verb_and_table(statement) else {
        return action("PartiQLSelect", "*".to_string());
    };
    if verb == "PartiQLSelect" {
        if let Some(index) = partiql_select_index(statement) {
            return action(verb, scope.index(&table, &index));
        }
    }
    action(verb, scope.table(&table))
}

/// The index a `SELECT ... FROM "table"."index"` reads, if any.
pub(crate) fn partiql_select_index(statement: &str) -> Option<String> {
    let stmt = super::helpers::partiql_parse::statement_shape(statement)?;
    stmt.is_read().then(|| stmt.index().map(str::to_string))?
}

/// Tags on the table a resource ARN names (a table, or its index or
/// stream), for `aws:ResourceTag/*`. `Some(empty)` for `*`; `None` when the
/// ARN names no table this state holds.
pub(crate) fn resource_tags(
    state: &crate::state::SharedDynamoDbState,
    resource_arn: &str,
) -> Option<HashMap<String, String>> {
    if resource_arn == "*" {
        return Some(HashMap::new());
    }
    let table_arn = table_arn_of(resource_arn)?;
    let region = table_arn.split(':').nth(3)?;
    let account = table_arn.split(':').nth(4)?;
    let name = table_arn.rsplit("table/").next()?;
    let accounts = state.read();
    let table = accounts.regional(account, region)?.tables.get(name)?;
    Some(
        table
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    )
}

/// Tags a request writes, for `aws:RequestTag/*` and `aws:TagKeys`:
/// `CreateTable` / `TagResource` carry `Tags: [{Key, Value}]`, and
/// `UntagResource` names keys only.
pub(crate) fn request_tags(request: &AwsRequest, action: &str) -> Option<HashMap<String, String>> {
    let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
    match action {
        "CreateTable" | "TagResource" => Some(
            body["Tags"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|t| {
                    Some((
                        t["Key"].as_str()?.to_string(),
                        t["Value"].as_str().unwrap_or_default().to_string(),
                    ))
                })
                .collect(),
        ),
        "UntagResource" => Some(
            body["TagKeys"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|k| Some((k.as_str()?.to_string(), String::new())))
                .collect(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::service::AwsService;
    use serde_json::json;

    const ACCOUNT: &str = "111122223333";
    const TABLE: &str = "arn:aws:dynamodb:eu-west-1:111122223333:table/Orders";

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodb".to_string(),
            action: action.to_string(),
            region: "eu-west-1".to_string(),
            account_id: ACCOUNT.to_string(),
            request_id: "test-id".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn test_state() -> crate::state::SharedDynamoDbState {
        std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "eu-west-1", ""),
        ))
    }

    fn pairs(actions: Vec<IamAction>) -> Vec<(String, String)> {
        actions
            .into_iter()
            .map(|a| (a.action_string(), a.resource))
            .collect()
    }

    fn one(action: &str, resource: &str) -> Vec<(String, String)> {
        vec![(format!("dynamodb:{action}"), resource.to_string())]
    }

    /// Strict enforcement denies an operation with no mapping, so every
    /// operation either service serves must map to at least one action.
    #[test]
    fn every_served_operation_maps_to_an_action() {
        let state: crate::state::SharedDynamoDbState =
            std::sync::Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(ACCOUNT, "eu-west-1", ""),
            ));
        let service = crate::DynamoDbService::new(state.clone());
        for op in service.supported_actions() {
            assert!(
                !actions_for(&test_state(), &req(op, json!({}))).is_empty(),
                "DynamoDB {op} has no IAM mapping"
            );
        }
        let streams = crate::DynamoDbStreamsService::new(state);
        for op in streams.supported_actions() {
            assert!(
                !streams_actions_for(&req(op, json!({}))).is_empty(),
                "DynamoDB Streams {op} has no IAM mapping"
            );
        }
    }

    #[test]
    fn item_and_table_operations_target_the_table() {
        for op in [
            "GetItem",
            "PutItem",
            "UpdateItem",
            "DeleteItem",
            "DescribeTable",
        ] {
            assert_eq!(
                pairs(actions_for(
                    &test_state(),
                    &req(op, json!({"TableName": "Orders"}))
                )),
                one(op, TABLE)
            );
        }
        // A table ARN is authorized as given, in its own account and region.
        let foreign = "arn:aws:dynamodb:us-east-2:444455556666:table/Shared";
        assert_eq!(
            pairs(actions_for(
                &test_state(),
                &req("GetItem", json!({"TableName": foreign}))
            )),
            one("GetItem", foreign)
        );
        assert_eq!(
            pairs(actions_for(&test_state(), &req("ListTables", json!({})))),
            one("ListTables", "*")
        );
    }

    #[test]
    fn query_and_scan_target_the_index_when_named() {
        let body = json!({"TableName": "Orders", "IndexName": "by-customer"});
        let index = format!("{TABLE}/index/by-customer");
        assert_eq!(
            pairs(actions_for(&test_state(), &req("Query", body.clone()))),
            one("Query", &index)
        );
        assert_eq!(
            pairs(actions_for(&test_state(), &req("Scan", body))),
            one("Scan", &index)
        );
        assert_eq!(
            pairs(actions_for(
                &test_state(),
                &req("Scan", json!({"TableName": "Orders"}))
            )),
            one("Scan", TABLE)
        );
    }

    /// Batches need the batch action on every table; transactions need the
    /// per-item action on each item's table, once per distinct pair.
    #[test]
    fn batches_and_transactions_authorize_every_table() {
        let batch = json!({"RequestItems": {"Orders": [], "Customers": []}});
        let mut got = pairs(actions_for(&test_state(), &req("BatchWriteItem", batch)));
        got.sort();
        assert_eq!(
            got,
            vec![
                (
                    "dynamodb:BatchWriteItem".to_string(),
                    "arn:aws:dynamodb:eu-west-1:111122223333:table/Customers".to_string()
                ),
                ("dynamodb:BatchWriteItem".to_string(), TABLE.to_string()),
            ]
        );

        let transact = json!({"TransactItems": [
            {"Put": {"TableName": "Orders"}},
            {"Put": {"TableName": "Orders"}},
            {"ConditionCheck": {"TableName": "Customers"}},
            {"Delete": {"TableName": "Orders"}},
            {"Update": {"TableName": "Orders"}}
        ]});
        assert_eq!(
            pairs(actions_for(
                &test_state(),
                &req("TransactWriteItems", transact)
            )),
            vec![
                ("dynamodb:PutItem".to_string(), TABLE.to_string()),
                (
                    "dynamodb:ConditionCheckItem".to_string(),
                    "arn:aws:dynamodb:eu-west-1:111122223333:table/Customers".to_string()
                ),
                ("dynamodb:DeleteItem".to_string(), TABLE.to_string()),
                ("dynamodb:UpdateItem".to_string(), TABLE.to_string()),
            ]
        );
        assert_eq!(
            pairs(actions_for(
                &test_state(),
                &req(
                    "TransactGetItems",
                    json!({"TransactItems": [{"Get": {"TableName": "Orders"}}]})
                )
            )),
            one("GetItem", TABLE)
        );
    }

    #[test]
    fn partiql_statements_map_to_partiql_actions() {
        let cases = [
            (
                "SELECT * FROM \"Orders\" WHERE pk = 'a'",
                "PartiQLSelect",
                TABLE.to_string(),
            ),
            ("select * from Orders", "PartiQLSelect", TABLE.to_string()),
            (
                "SELECT * FROM \"Orders\".\"by-customer\"",
                "PartiQLSelect",
                format!("{TABLE}/index/by-customer"),
            ),
            (
                "INSERT INTO \"Orders\" VALUE {'pk': 'a'}",
                "PartiQLInsert",
                TABLE.to_string(),
            ),
            (
                "UPDATE \"Orders\" SET x = 1 WHERE pk = 'a'",
                "PartiQLUpdate",
                TABLE.to_string(),
            ),
            (
                "DELETE FROM \"Orders\" WHERE pk = 'a'",
                "PartiQLDelete",
                TABLE.to_string(),
            ),
            ("EXPLAIN nonsense", "PartiQLSelect", "*".to_string()),
        ];
        for (statement, action_name, resource) in cases {
            assert_eq!(
                pairs(actions_for(
                    &test_state(),
                    &req("ExecuteStatement", json!({"Statement": statement}))
                )),
                one(action_name, &resource),
                "{statement}"
            );
        }
        let batch = json!({"Statements": [
            {"Statement": "INSERT INTO \"Orders\" VALUE {'pk': 'a'}"},
            {"Statement": "INSERT INTO \"Orders\" VALUE {'pk': 'b'}"},
            {"Statement": "DELETE FROM \"Orders\" WHERE pk = 'c'"}
        ]});
        assert_eq!(
            pairs(actions_for(
                &test_state(),
                &req("BatchExecuteStatement", batch)
            )),
            vec![
                ("dynamodb:PartiQLInsert".to_string(), TABLE.to_string()),
                ("dynamodb:PartiQLDelete".to_string(), TABLE.to_string()),
            ]
        );
    }

    #[test]
    fn create_table_and_restores_need_their_companion_actions() {
        let create = json!({
            "TableName": "Orders",
            "Tags": [{"Key": "team", "Value": "x"}],
            "ResourcePolicy": "{}"
        });
        assert_eq!(
            pairs(actions_for(&test_state(), &req("CreateTable", create))),
            vec![
                ("dynamodb:CreateTable".to_string(), TABLE.to_string()),
                ("dynamodb:TagResource".to_string(), TABLE.to_string()),
                ("dynamodb:PutResourcePolicy".to_string(), TABLE.to_string()),
            ]
        );
        assert_eq!(
            pairs(actions_for(
                &test_state(),
                &req("CreateTable", json!({"TableName": "Orders"}))
            )),
            one("CreateTable", TABLE)
        );

        let backup = format!("{TABLE}/backup/01700000000000-abcd");
        let restored = pairs(actions_for(
            &test_state(),
            &req(
                "RestoreTableFromBackup",
                json!({"BackupArn": backup, "TargetTableName": "Copy"}),
            ),
        ));
        let copy = "arn:aws:dynamodb:eu-west-1:111122223333:table/Copy";
        assert_eq!(
            restored[0],
            (
                "dynamodb:RestoreTableFromBackup".to_string(),
                backup.clone()
            )
        );
        assert!(restored.contains(&("dynamodb:PutItem".to_string(), copy.to_string())));
        assert!(restored.contains(&("dynamodb:BatchWriteItem".to_string(), copy.to_string())));

        let pitr = pairs(actions_for(
            &test_state(),
            &req(
                "RestoreTableToPointInTime",
                json!({"SourceTableName": "Orders", "TargetTableName": "Copy"}),
            ),
        ));
        assert_eq!(
            pitr[0],
            (
                "dynamodb:RestoreTableToPointInTime".to_string(),
                TABLE.to_string()
            )
        );
        assert!(pitr.contains(&("dynamodb:UpdateItem".to_string(), copy.to_string())));
    }

    #[test]
    fn streams_operations_target_the_stream() {
        let stream = format!("{TABLE}/stream/2026-01-01T00:00:00.000");
        assert_eq!(
            pairs(streams_actions_for(&req(
                "DescribeStream",
                json!({"StreamArn": stream})
            ))),
            one("DescribeStream", &stream)
        );
        assert_eq!(
            pairs(streams_actions_for(&req(
                "GetRecords",
                json!({"ShardIterator": format!("{stream}|shardId-1|0")})
            ))),
            one("GetRecords", &stream)
        );
        assert_eq!(
            pairs(streams_actions_for(&req("ListStreams", json!({})))),
            one("ListStreams", "*")
        );
    }

    #[test]
    fn request_tags_cover_create_tag_and_untag() {
        let create = req(
            "CreateTable",
            json!({"Tags": [{"Key": "team", "Value": "payments"}]}),
        );
        assert_eq!(
            request_tags(&create, "CreateTable"),
            Some(HashMap::from([(
                "team".to_string(),
                "payments".to_string()
            )]))
        );
        let untag = req("UntagResource", json!({"TagKeys": ["team"]}));
        assert_eq!(
            request_tags(&untag, "UntagResource").map(|t| t.into_keys().collect::<Vec<_>>()),
            Some(vec!["team".to_string()])
        );
        assert_eq!(request_tags(&req("GetItem", json!({})), "GetItem"), None);
    }
}
