+++
title = "Resource Groups"
description = "AWS Resource Groups (resource-groups) on fakecloud: groups, tag/CloudFormation-stack resource queries, explicit membership, group configuration, tagging, account settings, and tag-sync tasks, with real account-partitioned, persisted state."
weight = 43
+++

fakecloud implements the **Resource Groups** (`resource-groups`) control plane,
the service that lets you organize AWS resources into groups defined by a tag or
CloudFormation-stack query. All **23 operations** from the AWS Smithy model ship
now, backed by real, account-partitioned state that persists across restarts in
persistent mode. Groups get `arn:aws:resource-groups:<region>:<acct>:group/<name>/<id>`
ARNs.

## Supported features

- **Group lifecycle** (`CreateGroup`, `GetGroup`, `UpdateGroup`, `DeleteGroup`,
  `ListGroups`). A group is created with either a `ResourceQuery` (a tag or
  CloudFormation-stack membership query) or a `Configuration` (a
  service-configuration group that takes explicit membership). `ListGroups`
  returns both `GroupIdentifiers` and full `Groups`, with pagination.
- **Resource queries** (`GetGroupQuery`, `UpdateGroupQuery`) store and return
  the `TAG_FILTERS_1_0` / `CLOUDFORMATION_STACK_1_0` query. The `Query` payload
  is validated as JSON.
- **Explicit membership** (`GroupResources`, `UngroupResources`,
  `ListGroupResources`) associates resource ARNs with a configuration group and
  lists them with their derived resource types. Query-based groups reject
  explicit membership, matching AWS.
- **Search** (`SearchResources`) evaluates a free-standing resource query.
- **Group configuration** (`GetGroupConfiguration`, `PutGroupConfiguration`)
  stores the `GroupConfigurationItem` list.
- **Tagging** (`Tag`, `Untag`, `GetTags`) keyed by group ARN.
- **Account settings** (`GetAccountSettings`, `UpdateAccountSettings`) for the
  group lifecycle events desired status.
- **Grouping statuses** (`ListGroupingStatuses`) and **tag-sync tasks**
  (`StartTagSyncTask`, `GetTagSyncTask`, `ListTagSyncTasks`,
  `CancelTagSyncTask`).

100% conformance: all 786 generated Smithy probe variants pass.

Query-based membership **resolution** (evaluating a tag or CloudFormation-stack
query against live resources across every service) depends on the cross-service
tag index that ships with the Resource Groups Tagging API. Until that lands, a
query-based group has **no** computed members and `ListGroupResources` returns
an empty set for it — query-based groups also reject explicit `GroupResources` /
`UngroupResources` (matching AWS, since their membership is meant to be derived).
Explicit membership on **configuration** groups is fully real today: create a
group with a `Configuration` and `GroupResources` associates ARNs you can list
back immediately.

## Example

```python
import boto3, json
rg = boto3.client("resource-groups", endpoint_url="http://localhost:4566")

# A tag-query group.
rg.create_group(
    Name="prod-web",
    ResourceQuery={
        "Type": "TAG_FILTERS_1_0",
        "Query": json.dumps({
            "ResourceTypeFilters": ["AWS::AllSupported"],
            "TagFilters": [{"Key": "stage", "Values": ["prod"]}],
        }),
    },
    Tags={"team": "web"},
)
print(rg.get_group_query(Group="prod-web")["GroupQuery"]["ResourceQuery"]["Type"])

# A configuration group with explicit membership.
rg.create_group(
    Name="hosts",
    Configuration=[{
        "Type": "AWS::ResourceGroups::Generic",
        "Parameters": [{"Name": "allowed-resource-types", "Values": ["AWS::EC2::Host"]}],
    }],
)
rg.group_resources(Group="hosts",
    ResourceArns=["arn:aws:ec2:us-east-1:123456789012:host/h-abc"])
print(rg.list_group_resources(Group="hosts")["Resources"])
```
