package fakecloud

import (
	"context"
	"encoding/json"
)

// WAFv2Client provides access to WAFv2 admin/introspection endpoints.
type WAFv2Client struct {
	fc *FakeCloud
}

// Evaluate runs the WAFv2 evaluation engine for a synthesized request and
// returns the engine's verdict. Request and response are passed through as
// raw JSON so callers can drive whichever evaluation shape the server
// currently exposes without an SDK release coupling.
func (c *WAFv2Client) Evaluate(
	ctx context.Context,
	req json.RawMessage,
) (json.RawMessage, error) {
	var out json.RawMessage
	if err := c.fc.doPost(ctx, "/_fakecloud/wafv2/evaluate", req, &out); err != nil {
		return nil, err
	}
	return out, nil
}
