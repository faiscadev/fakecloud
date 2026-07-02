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

// SaveSnapshot writes the current DynamoDB state as a canonical snapshot on
// demand. When dataPath is non-empty the snapshot is written to
// <dataPath>/dynamodb/snapshot.json; when empty it is written to the server's
// configured persistent store (an error if none is configured).
func (c *DynamoDBClient) SaveSnapshot(ctx context.Context, dataPath string) (*DynamoDBSnapshotSaveResponse, error) {
	var body *DynamoDBSnapshotSaveRequest
	if dataPath != "" {
		body = &DynamoDBSnapshotSaveRequest{DataPath: dataPath}
	}
	var out DynamoDBSnapshotSaveResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/dynamodb/snapshot/save", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
