package fakecloud

import (
	"context"
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
