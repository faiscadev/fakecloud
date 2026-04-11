package fakecloud

import (
	"context"
	"fmt"
)

// BedrockClient provides access to Bedrock introspection endpoints.
type BedrockClient struct {
	fc *FakeCloud
}

// GetInvocations lists all recorded Bedrock model invocations.
func (c *BedrockClient) GetInvocations(ctx context.Context) (*BedrockInvocationsResponse, error) {
	var out BedrockInvocationsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/bedrock/invocations", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetModelResponse configures the canned response for a Bedrock model.
func (c *BedrockClient) SetModelResponse(ctx context.Context, modelID string, response string) (*BedrockModelResponseConfig, error) {
	var out BedrockModelResponseConfig
	if err := c.fc.doPostText(ctx, fmt.Sprintf("/_fakecloud/bedrock/models/%s/response", modelID), response, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
