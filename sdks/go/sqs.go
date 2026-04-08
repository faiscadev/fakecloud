package fakecloud

import (
	"context"
	"fmt"
)

// SQSClient provides access to SQS introspection endpoints.
type SQSClient struct {
	fc *FakeCloud
}

// GetMessages lists all messages across all SQS queues.
func (c *SQSClient) GetMessages(ctx context.Context) (*SQSMessagesResponse, error) {
	var out SQSMessagesResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/sqs/messages", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// TickExpiration ticks the SQS message expiration processor.
func (c *SQSClient) TickExpiration(ctx context.Context) (*ExpirationTickResponse, error) {
	var out ExpirationTickResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/sqs/expiration-processor/tick", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ForceDLQ forces all messages in a queue to its dead-letter queue.
func (c *SQSClient) ForceDLQ(ctx context.Context, queueName string) (*ForceDLQResponse, error) {
	var out ForceDLQResponse
	if err := c.fc.doPost(ctx, fmt.Sprintf("/_fakecloud/sqs/%s/force-dlq", queueName), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
