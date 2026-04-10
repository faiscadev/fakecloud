package fakecloud

import "context"

// RDSClient provides access to RDS introspection endpoints.
type RDSClient struct {
	fc *FakeCloud
}

// GetInstances lists fakecloud-managed RDS DB instances and runtime metadata.
func (c *RDSClient) GetInstances(ctx context.Context) (*RDSInstancesResponse, error) {
	var out RDSInstancesResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/rds/instances", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
