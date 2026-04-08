package fakecloud

import "context"

// DynamoDBClient provides access to DynamoDB introspection endpoints.
type DynamoDBClient struct {
	fc *FakeCloud
}

// TickTTL ticks the DynamoDB TTL processor.
func (c *DynamoDBClient) TickTTL(ctx context.Context) (*TTLTickResponse, error) {
	var out TTLTickResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/dynamodb/ttl-processor/tick", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
