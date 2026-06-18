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

// GetInstanceNetworks inspects the real backing network of each EC2 instance —
// which Docker/Podman network or k8s NetworkPolicy backs it, its container IP,
// and whether security-group enforcement is active or degraded. A debugging
// aid for "why can't X reach Y" (issue #1745).
func (c *EC2Client) GetInstanceNetworks(ctx context.Context) (*EC2InstanceNetworksResponse, error) {
	var out EC2InstanceNetworksResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/ec2/instance-networks", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
