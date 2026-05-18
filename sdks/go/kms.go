package fakecloud

import "context"

// KMSClient provides access to KMS introspection endpoints.
type KMSClient struct {
	fc *FakeCloud
}

// GetUsage returns the recorded KMS usage records (one per cryptographic
// operation seen by the server).
func (c *KMSClient) GetUsage(ctx context.Context) (*KMSUsageResponse, error) {
	var out KMSUsageResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/kms/usage", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
