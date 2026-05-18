package fakecloud

import (
	"context"
	"fmt"
	"net/url"
)

// SSMClient provides access to Systems Manager admin/introspection endpoints.
type SSMClient struct {
	fc *FakeCloud
}

// SetCommandStatus flips a stored Run Command's status (and the status of all
// its invocations) without going through the agent-side reporting path.
func (c *SSMClient) SetCommandStatus(
	ctx context.Context,
	commandID string,
	req *SetSsmCommandStatusRequest,
) (*SetSsmCommandStatusResponse, error) {
	var out SetSsmCommandStatusResponse
	path := fmt.Sprintf("/_fakecloud/ssm/commands/%s/status", url.PathEscape(commandID))
	if err := c.fc.doPost(ctx, path, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FailCommand flips a Run Command (or a single invocation, when InstanceID is
// set) to Failed. The request body is optional; pass nil to flip every
// invocation with default status details.
func (c *SSMClient) FailCommand(
	ctx context.Context,
	commandID string,
	req *FailSsmCommandRequest,
) (*FailSsmCommandResponse, error) {
	var out FailSsmCommandResponse
	path := fmt.Sprintf("/_fakecloud/ssm/commands/%s/fail", url.PathEscape(commandID))
	if err := c.fc.doPost(ctx, path, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetParameterPolicyEvents returns every parameter-policy event recorded for
// the given account.
func (c *SSMClient) GetParameterPolicyEvents(
	ctx context.Context,
	accountID string,
) (*SsmParameterPolicyEventsResponse, error) {
	var out SsmParameterPolicyEventsResponse
	path := "/_fakecloud/ssm/parameter-policy-events"
	if accountID != "" {
		path += "?accountId=" + url.QueryEscape(accountID)
	}
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// InjectSession drops a fake Session Manager session record into state
// without going through StartSession.
func (c *SSMClient) InjectSession(
	ctx context.Context,
	req *InjectSsmSessionRequest,
) (*InjectSsmSessionResponse, error) {
	var out InjectSsmSessionResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/ssm/sessions/inject", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
