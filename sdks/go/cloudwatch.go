package fakecloud

import "context"

// CloudWatchClient provides access to CloudWatch introspection endpoints.
// Lets tests assert what PutMetricAlarm / PutCompositeAlarm recorded and
// inspect published metric data without re-listing through the AWS surface.
type CloudWatchClient struct {
	fc *FakeCloud
}

// GetAlarms returns every metric and composite alarm across all accounts
// and regions. Order is stable: by account, then region, then name.
func (c *CloudWatchClient) GetAlarms(ctx context.Context) (*CloudWatchAlarmsResponse, error) {
	var out CloudWatchAlarmsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cloudwatch/alarms", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetMetrics returns every unique metric series (account, region,
// namespace, metric, dimensions) with its datapoint count and latest
// value. Order is stable: by account, region, namespace, then metric.
func (c *CloudWatchClient) GetMetrics(ctx context.Context) (*CloudWatchMetricsResponse, error) {
	var out CloudWatchMetricsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cloudwatch/metrics", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
