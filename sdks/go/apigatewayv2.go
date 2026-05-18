package fakecloud

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ApiGatewayV2Client provides access to API Gateway v2 introspection endpoints.
type ApiGatewayV2Client struct {
	fc *FakeCloud
}

// GetRequests lists all HTTP API requests that were received and processed.
func (c *ApiGatewayV2Client) GetRequests(ctx context.Context) (*ApiGatewayV2RequestsResponse, error) {
	var out ApiGatewayV2RequestsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/apigatewayv2/requests", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetConnections lists live WebSocket connections tracked by the fake
// API Gateway v2 runtime.
func (c *ApiGatewayV2Client) GetConnections(ctx context.Context) (*ApiGatewayV2ConnectionsResponse, error) {
	var out ApiGatewayV2ConnectionsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/apigatewayv2/connections", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetDomainNameMtlsInfo returns the mTLS truststore introspection blob
// for a custom domain. The response shape is intentionally open-ended;
// callers can decode further into a typed structure if needed.
func (c *ApiGatewayV2Client) GetDomainNameMtlsInfo(ctx context.Context, name string) (json.RawMessage, error) {
	var out json.RawMessage
	path := fmt.Sprintf("/_fakecloud/apigatewayv2/domain-names/%s/mtls-info", name)
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// WsURL returns the ws:// (or wss://) URL for the given API Gateway v2
// WebSocket API and stage. The stage defaults to "$default" when not
// provided. The scheme is derived from the client's BaseURL: an https://
// base maps to wss://, anything else to ws://.
func (c *ApiGatewayV2Client) WsURL(apiID string, stage ...string) string {
	stg := "$default"
	if len(stage) > 0 && stage[0] != "" {
		stg = stage[0]
	}
	base := c.fc.BaseURL
	scheme := "ws"
	rest := base
	switch {
	case strings.HasPrefix(base, "https://"):
		scheme = "wss"
		rest = strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		rest = strings.TrimPrefix(base, "http://")
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, rest, apiID, stg)
}
