package fakecloud

import (
	"context"
	"net/url"
)

// StepFunctionsClient provides access to Step Functions introspection endpoints.
type StepFunctionsClient struct {
	fc *FakeCloud
}

// GetExecutions lists all state machine executions that have been recorded.
func (c *StepFunctionsClient) GetExecutions(ctx context.Context) (*StepFunctionsExecutionsResponse, error) {
	var out StepFunctionsExecutionsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/stepfunctions/executions", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetSyncExecutions lists all recorded EXPRESS sync executions, with billing detail.
func (c *StepFunctionsClient) GetSyncExecutions(ctx context.Context) (*StepFunctionsSyncExecutionsResponse, error) {
	var out StepFunctionsSyncExecutionsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/stepfunctions/sync-executions", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetExecutionTree returns the parent/child execution tree rooted at the given execution ARN.
func (c *StepFunctionsClient) GetExecutionTree(ctx context.Context, arn string) (*StepFunctionsExecutionTreeResponse, error) {
	var out StepFunctionsExecutionTreeResponse
	path := "/_fakecloud/stepfunctions/execution-tree/" + url.PathEscape(arn)
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// EnqueueActivityTask inserts a pending task into an activity's worker queue
// without running an ASL execution. Useful for testing activity worker clients
// (GetActivityTask / SendTaskSuccess / SendTaskFailure) in isolation.
func (c *StepFunctionsClient) EnqueueActivityTask(ctx context.Context, req SfnEnqueueActivityTaskRequest) (*SfnEnqueueActivityTaskResponse, error) {
	var out SfnEnqueueActivityTaskResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/stepfunctions/enqueue-activity-task", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
