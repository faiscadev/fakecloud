package fakecloud

import "context"

// EventsClient provides access to EventBridge introspection endpoints.
type EventsClient struct {
	fc *FakeCloud
}

// GetHistory returns event history and delivery records.
func (c *EventsClient) GetHistory(ctx context.Context) (*EventHistoryResponse, error) {
	var out EventHistoryResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/events/history", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FireRule manually fires an EventBridge rule.
func (c *EventsClient) FireRule(ctx context.Context, req *FireRuleRequest) (*FireRuleResponse, error) {
	var out FireRuleResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/events/fire-rule", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
