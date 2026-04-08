package fakecloud

import "context"

// SESClient provides access to SES introspection endpoints.
type SESClient struct {
	fc *FakeCloud
}

// GetEmails lists all emails sent through the SES emulator.
func (c *SESClient) GetEmails(ctx context.Context) (*SESEmailsResponse, error) {
	var out SESEmailsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/ses/emails", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SimulateInbound simulates an inbound email (SES receipt rules).
func (c *SESClient) SimulateInbound(ctx context.Context, req *InboundEmailRequest) (*InboundEmailResponse, error) {
	var out InboundEmailResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/ses/inbound", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
