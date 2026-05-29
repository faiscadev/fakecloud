package fakecloud

import "context"

// FirehoseClient provides access to Firehose introspection endpoints.
// Lets tests assert delivery stream state without round-tripping through
// DescribeDeliveryStream (and without credentials).
type FirehoseClient struct {
	fc *FakeCloud
}

// GetDeliveryStreams returns every Firehose delivery stream across all
// accounts and regions, with stream type, lifecycle status, encryption
// summary, and destination count. Order is stable: by account, then name.
func (c *FirehoseClient) GetDeliveryStreams(ctx context.Context) (*FirehoseDeliveryStreamsResponse, error) {
	var out FirehoseDeliveryStreamsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/firehose/delivery-streams", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
