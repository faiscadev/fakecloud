package fakecloud

import (
	"context"
	"fmt"
)

// LambdaClient provides access to Lambda introspection endpoints.
type LambdaClient struct {
	fc *FakeCloud
}

// GetInvocations lists recorded Lambda invocations.
func (c *LambdaClient) GetInvocations(ctx context.Context) (*LambdaInvocationsResponse, error) {
	var out LambdaInvocationsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/lambda/invocations", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetWarmContainers lists warm (cached) Lambda containers.
func (c *LambdaClient) GetWarmContainers(ctx context.Context) (*WarmContainersResponse, error) {
	var out WarmContainersResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/lambda/warm-containers", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// EvictContainer evicts the warm container for a specific function.
func (c *LambdaClient) EvictContainer(ctx context.Context, functionName string) (*EvictContainerResponse, error) {
	var out EvictContainerResponse
	if err := c.fc.doPost(ctx, fmt.Sprintf("/_fakecloud/lambda/%s/evict-container", functionName), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
