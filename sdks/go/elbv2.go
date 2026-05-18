package fakecloud

import "context"

// ELBv2Client provides access to ELBv2 introspection endpoints —
// the persisted control-plane state of every Application/Network/Gateway
// load balancer fakecloud has seen, plus their target groups, listeners,
// and rules.
type ELBv2Client struct {
	fc *FakeCloud
}

// GetLoadBalancers lists every load balancer across every account.
func (c *ELBv2Client) GetLoadBalancers(ctx context.Context) (*Elbv2LoadBalancersResponse, error) {
	var out Elbv2LoadBalancersResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/elbv2/load-balancers", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTargetGroups lists every target group across every account.
func (c *ELBv2Client) GetTargetGroups(ctx context.Context) (*Elbv2TargetGroupsResponse, error) {
	var out Elbv2TargetGroupsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/elbv2/target-groups", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetListeners lists every listener across every account.
func (c *ELBv2Client) GetListeners(ctx context.Context) (*Elbv2ListenersResponse, error) {
	var out Elbv2ListenersResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/elbv2/listeners", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetRules lists every listener rule across every account, including
// the default rules that AWS auto-creates for each listener.
func (c *ELBv2Client) GetRules(ctx context.Context) (*Elbv2RulesResponse, error) {
	var out Elbv2RulesResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/elbv2/rules", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetWafCounts returns the WAF rule-match counters accumulated per
// load balancer / listener. Payload is passed through as raw JSON so
// the SDK doesn't need a release every time a new counter is added.
func (c *ELBv2Client) GetWafCounts(ctx context.Context) (*Elbv2WafCountsResponse, error) {
	var out Elbv2WafCountsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/elbv2/waf-counts", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FlushAccessLogsResponse is the response from a `POST` to the
// /_fakecloud/elbv2/access-logs/flush admin endpoint.
type FlushAccessLogsResponse struct {
	Flushed bool `json:"flushed"`
}

// FlushAccessLogs forces every buffered access-log + connection-log
// line to flush to S3 right now, bypassing the periodic 60-second
// timer. Returns Flushed=true when a logger is wired and the flush ran.
func (c *ELBv2Client) FlushAccessLogs(ctx context.Context) (*FlushAccessLogsResponse, error) {
	var out FlushAccessLogsResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/elbv2/access-logs/flush", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
