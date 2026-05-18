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

// GetCertPEM returns the SNS signing certificate as a PEM string. This
// is the certificate AWS uses to sign SNS HTTP notifications; tests can
// pin against it to verify message signatures locally.
func (c *SNSClient) GetCertPEM(ctx context.Context) (string, error) {
	return c.fc.doGetText(ctx, "/_fakecloud/sns/cert.pem")
}

// GetSMSMessages lists every SMS message published through the fake SNS
// SMS publisher.
func (c *SNSClient) GetSMSMessages(ctx context.Context) (*SnsSmsResponse, error) {
	var out SnsSmsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/sns/sms", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
