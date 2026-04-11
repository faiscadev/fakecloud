// Package fakecloud provides a Go client for the fakecloud introspection
// and simulation API (/_fakecloud/*).
package fakecloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// FakeCloud is the top-level client for the fakecloud introspection API.
type FakeCloud struct {
	BaseURL string
	client  *http.Client
}

// New creates a new FakeCloud client pointing at the given base URL
// (e.g. "http://localhost:4566").
func New(baseURL string) *FakeCloud {
	return &FakeCloud{
		BaseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{},
	}
}

// Health checks server health.
func (fc *FakeCloud) Health(ctx context.Context) (*HealthResponse, error) {
	var out HealthResponse
	if err := fc.doGet(ctx, "/_fakecloud/health", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Reset resets all service state using the legacy /_reset endpoint.
func (fc *FakeCloud) Reset(ctx context.Context) error {
	var out ResetResponse
	return fc.doPost(ctx, "/_reset", nil, &out)
}

// ResetService resets a single service's state.
func (fc *FakeCloud) ResetService(ctx context.Context, service string) (*ResetServiceResponse, error) {
	var out ResetServiceResponse
	if err := fc.doPost(ctx, fmt.Sprintf("/_fakecloud/reset/%s", service), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Sub-client accessors

// SES returns the SES sub-client.
func (fc *FakeCloud) SES() *SESClient { return &SESClient{fc: fc} }

// SNS returns the SNS sub-client.
func (fc *FakeCloud) SNS() *SNSClient { return &SNSClient{fc: fc} }

// SQS returns the SQS sub-client.
func (fc *FakeCloud) SQS() *SQSClient { return &SQSClient{fc: fc} }

// Events returns the EventBridge sub-client.
func (fc *FakeCloud) Events() *EventsClient { return &EventsClient{fc: fc} }

// S3 returns the S3 sub-client.
func (fc *FakeCloud) S3() *S3Client { return &S3Client{fc: fc} }

// Lambda returns the Lambda sub-client.
func (fc *FakeCloud) Lambda() *LambdaClient { return &LambdaClient{fc: fc} }

// RDS returns the RDS sub-client.
func (fc *FakeCloud) RDS() *RDSClient { return &RDSClient{fc: fc} }

// ElastiCache returns the ElastiCache sub-client.
func (fc *FakeCloud) ElastiCache() *ElastiCacheClient { return &ElastiCacheClient{fc: fc} }

// DynamoDB returns the DynamoDB sub-client.
func (fc *FakeCloud) DynamoDB() *DynamoDBClient { return &DynamoDBClient{fc: fc} }

// SecretsManager returns the SecretsManager sub-client.
func (fc *FakeCloud) SecretsManager() *SecretsManagerClient { return &SecretsManagerClient{fc: fc} }

// Cognito returns the Cognito sub-client.
func (fc *FakeCloud) Cognito() *CognitoClient { return &CognitoClient{fc: fc} }

// ApiGatewayV2 returns the API Gateway v2 sub-client.
func (fc *FakeCloud) ApiGatewayV2() *ApiGatewayV2Client { return &ApiGatewayV2Client{fc: fc} }

// StepFunctions returns the Step Functions sub-client.
func (fc *FakeCloud) StepFunctions() *StepFunctionsClient { return &StepFunctionsClient{fc: fc} }

// ── Error type ─────────────────────────────────────────────────────

// APIError is returned when the server responds with a non-2xx status.
type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("fakecloud: HTTP %d: %s", e.StatusCode, e.Body)
}

// ── Internal helpers ───────────────────────────────────────────────

func (fc *FakeCloud) doGet(ctx context.Context, path string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fc.BaseURL+path, nil)
	if err != nil {
		return err
	}
	return fc.do(req, out)
}

func (fc *FakeCloud) doPost(ctx context.Context, path string, body interface{}, out interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fc.BaseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return fc.do(req, out)
}

func (fc *FakeCloud) do(req *http.Request, out interface{}) error {
	resp, err := fc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}

	if out != nil {
		return json.Unmarshal(respBody, out)
	}
	return nil
}
