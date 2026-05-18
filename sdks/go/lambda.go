package fakecloud

import (
	"context"
	"fmt"
	"net/url"
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

// DownloadFunctionCode returns the raw zip bytes of a function's stored
// deployment package. Pass "latest" as qualifierOrLatest to fetch the
// most recent code; otherwise pass a numeric version string (e.g. "3").
func (c *LambdaClient) DownloadFunctionCode(
	ctx context.Context,
	accountID, functionName, qualifierOrLatest string,
) ([]byte, error) {
	file := qualifierOrLatest
	if file == "" || file == "latest" {
		file = "latest.zip"
	} else {
		file = file + ".zip"
	}
	path := fmt.Sprintf(
		"/_fakecloud/lambda/function-code/%s/%s/%s",
		url.PathEscape(accountID),
		url.PathEscape(functionName),
		url.PathEscape(file),
	)
	return c.fc.doGetBytes(ctx, path)
}

// DownloadLayerContent returns the raw zip bytes of a specific layer
// version's stored content.
func (c *LambdaClient) DownloadLayerContent(
	ctx context.Context,
	accountID, layerName string,
	version int64,
) ([]byte, error) {
	file := fmt.Sprintf("%d.zip", version)
	path := fmt.Sprintf(
		"/_fakecloud/lambda/layer-content/%s/%s/%s",
		url.PathEscape(accountID),
		url.PathEscape(layerName),
		url.PathEscape(file),
	)
	return c.fc.doGetBytes(ctx, path)
}

// EvictContainer evicts the warm container for a specific function.
func (c *LambdaClient) EvictContainer(ctx context.Context, functionName string) (*EvictContainerResponse, error) {
	var out EvictContainerResponse
	if err := c.fc.doPost(ctx, fmt.Sprintf("/_fakecloud/lambda/%s/evict-container", functionName), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
