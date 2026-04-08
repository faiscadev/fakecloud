package fakecloud

import "context"

// SNSClient provides access to SNS introspection endpoints.
type SNSClient struct {
	fc *FakeCloud
}

// GetMessages lists all published SNS messages.
func (c *SNSClient) GetMessages(ctx context.Context) (*SNSMessagesResponse, error) {
	var out SNSMessagesResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/sns/messages", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetPendingConfirmations lists subscriptions pending confirmation.
func (c *SNSClient) GetPendingConfirmations(ctx context.Context) (*PendingConfirmationsResponse, error) {
	var out PendingConfirmationsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/sns/pending-confirmations", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ConfirmSubscription confirms a pending SNS subscription.
func (c *SNSClient) ConfirmSubscription(ctx context.Context, req *ConfirmSubscriptionRequest) (*ConfirmSubscriptionResponse, error) {
	var out ConfirmSubscriptionResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/sns/confirm-subscription", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
