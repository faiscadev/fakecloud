package fakecloud

import (
	"context"
	"fmt"
	"net/url"
)

// LogsClient provides access to CloudWatch Logs admin/introspection endpoints.
type LogsClient struct {
	fc *FakeCloud
}

// InjectAnomaly seeds a synthetic anomaly so tests can exercise
// ListAnomalies/UpdateAnomaly deterministically.
func (c *LogsClient) InjectAnomaly(ctx context.Context, req *LogsAnomalyInjectRequest) (*LogsAnomalyInjectResponse, error) {
	var out LogsAnomalyInjectResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/logs/anomalies/inject", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LogsAnomalyInjectRequest is the payload for `/_fakecloud/logs/anomalies/inject`.
type LogsAnomalyInjectRequest struct {
	AnomalyDetectorARN string   `json:"anomalyDetectorArn"`
	LogGroupARNs       []string `json:"logGroupArns,omitempty"`
	PatternString      string   `json:"patternString"`
	Priority           *string  `json:"priority,omitempty"`
}

// LogsAnomalyInjectResponse is returned by the inject endpoint.
type LogsAnomalyInjectResponse struct {
	AnomalyID string `json:"anomalyId"`
}

// GetDeliveryConfig returns persisted CloudWatch Logs delivery
// configurations (PutDeliverySource + PutDeliveryDestination +
// CreateDelivery) so tests can assert what fakecloud has wired up
// without joining state by hand.
func (c *LogsClient) GetDeliveryConfig(ctx context.Context) (*LogsDeliveryConfigResponse, error) {
	var out LogsDeliveryConfigResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/logs/delivery-config", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetFieldIndexes returns the index policies registered on a log
// group, with the `Fields` array parsed from each policy document.
// Returns 404 when the log group does not exist.
func (c *LogsClient) GetFieldIndexes(ctx context.Context, logGroupName string) (*LogsFieldIndexesResponse, error) {
	var out LogsFieldIndexesResponse
	path := fmt.Sprintf("/_fakecloud/logs/field-indexes/%s", url.PathEscape(logGroupName))
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LogsDeliveryConfiguration mirrors one entry of the delivery-config
// introspection response.
type LogsDeliveryConfiguration struct {
	ID                      string                 `json:"id"`
	Name                    string                 `json:"name"`
	DeliveryDestinationARN  string                 `json:"deliveryDestinationArn"`
	DeliverySourceName      string                 `json:"deliverySourceName"`
	LogType                 string                 `json:"logType"`
	RecordFields            []string               `json:"recordFields,omitempty"`
	FieldDelimiter          *string                `json:"fieldDelimiter,omitempty"`
	S3DeliveryConfiguration map[string]interface{} `json:"s3DeliveryConfiguration,omitempty"`
	CreatedAt               int64                  `json:"createdAt"`
}

// LogsDeliveryConfigResponse is returned by GetDeliveryConfig.
type LogsDeliveryConfigResponse struct {
	Configurations []LogsDeliveryConfiguration `json:"configurations"`
}

// LogsFieldIndex is one parsed IndexPolicy.
type LogsFieldIndex struct {
	Fields     []string `json:"fields"`
	CreatedAt  int64    `json:"createdAt"`
	LastUsedAt int64    `json:"lastUsedAt"`
}

// LogsFieldIndexesResponse is returned by GetFieldIndexes.
type LogsFieldIndexesResponse struct {
	LogGroupName string           `json:"logGroupName"`
	Indexes      []LogsFieldIndex `json:"indexes"`
}
