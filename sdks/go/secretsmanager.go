package fakecloud

import "context"

// SecretsManagerClient provides access to SecretsManager introspection endpoints.
type SecretsManagerClient struct {
	fc *FakeCloud
}

// TickRotation ticks the SecretsManager rotation scheduler.
func (c *SecretsManagerClient) TickRotation(ctx context.Context) (*RotationTickResponse, error) {
	var out RotationTickResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/secretsmanager/rotation-scheduler/tick", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
