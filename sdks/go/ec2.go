package fakecloud

import "context"

// EC2Client provides access to EC2 introspection endpoints
// (the fakecloud `/_fakecloud/ec2/*` surface).
type EC2Client struct {
	fc *FakeCloud
}

// GetInstances lists fakecloud-managed EC2 instances and runtime metadata.
func (c *EC2Client) GetInstances(ctx context.Context) (*EC2InstancesResponse, error) {
	var out EC2InstancesResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/ec2/instances", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
