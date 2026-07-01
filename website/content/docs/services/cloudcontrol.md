+++
title = "Cloud Control API"
description = "AWS Cloud Control API (cloudcontrolapi) on fakecloud: a uniform CRUD-L interface over CloudFormation resource types, driving the same real provisioners as CloudFormation, with JSON Patch updates and request tracking."
weight = 42
+++

fakecloud implements the **Cloud Control API** (`cloudcontrolapi`), the uniform
create/read/update/delete/list interface AWS exposes over every CloudFormation
resource type. All **8 operations** ship now, and they are not a parallel
implementation: `CreateResource`, `UpdateResource`, and `DeleteResource` drive
the exact same resource provisioners CloudFormation uses, so a
`AWS::SQS::Queue` created through Cloud Control is a real queue, backed the same
way it would be inside a stack. State is account-partitioned and persists across
restarts in persistent mode.

## Supported features

- **Resource CRUD-L** (`CreateResource`, `GetResource`, `UpdateResource`,
  `DeleteResource`, `ListResources`) over any CloudFormation resource type that
  fakecloud's provisioners support. `DesiredState` is the resource's properties
  as a JSON string; the response `ProgressEvent` carries the assigned
  `Identifier` and the resolved `ResourceModel`.
- **Real provisioning**: create/update/delete delegate to the CloudFormation
  resource provisioners, including container-backed resources — the same
  machinery that spawns real infrastructure for stacks. There is no separate
  fake path.
- **JSON Patch updates**: `UpdateResource` takes a `PatchDocument` (an RFC 6902
  JSON Patch array). fakecloud applies the full operation set (`add`, `remove`,
  `replace`, `move`, `copy`, `test`) over RFC 6901 JSON Pointer paths to the
  stored desired state, then hands the result to the resource's update handler.
- **Idempotency**: `ClientToken` on a mutating request replays the original
  request's terminal `ProgressEvent` instead of provisioning again.
- **Request tracking** (`GetResourceRequestStatus`, `ListResourceRequests`,
  `CancelResourceRequest`) records every mutating request as a
  `ProgressEvent` with its `RequestToken`, `Operation`, `OperationStatus`, and
  timing, so callers can poll status and enumerate history.

100% conformance: all 346 generated Smithy probe variants pass.

## Example

```python
import boto3, json
cc = boto3.client("cloudcontrol", endpoint_url="http://localhost:4566")

# Create a real SQS queue through Cloud Control.
resp = cc.create_resource(
    TypeName="AWS::SQS::Queue",
    DesiredState=json.dumps({"QueueName": "cc-demo"}),
)
ev = resp["ProgressEvent"]
identifier = ev["Identifier"]        # the queue URL/name
assert ev["OperationStatus"] == "SUCCESS"

# Read it back.
got = cc.get_resource(TypeName="AWS::SQS::Queue", Identifier=identifier)
props = json.loads(got["ResourceDescription"]["Properties"])

# Update it with a JSON Patch document.
cc.update_resource(
    TypeName="AWS::SQS::Queue",
    Identifier=identifier,
    PatchDocument=json.dumps([
        {"op": "add", "path": "/Tags", "value": [{"Key": "env", "Value": "test"}]}
    ]),
)

# Track the request history.
cc.list_resource_requests()

# Delete it.
cc.delete_resource(TypeName="AWS::SQS::Queue", Identifier=identifier)
```
