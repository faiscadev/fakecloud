package fakecloud

import (
	"context"
	"fmt"
)

// Route53Client provides access to Route 53 admin endpoints.
//
// Wraps the per-health-check status admin endpoint that lets tests flip a
// stored health check between healthy and unhealthy without a live prober,
// so failover and multi-value routing can be exercised end-to-end.
type Route53Client struct {
	fc *FakeCloud
}

// SetHealthCheckStatusRequest is the JSON body sent to the admin endpoint.
//
// Status is one of "Success", "Failure", "Timeout", "DnsError",
// "InsufficientDataPoints", "Unknown". Reason is appended to the <Status>
// element returned by GetHealthCheckStatus for failure-flavoured statuses
// (Failure, Timeout, DnsError); ignored otherwise.
type SetHealthCheckStatusRequest struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// SetHealthCheckStatus flips a Route 53 health check's reported status.
// Pass an empty Reason to omit it (the prior reason is preserved).
func (c *Route53Client) SetHealthCheckStatus(
	ctx context.Context,
	healthCheckID string,
	req *SetHealthCheckStatusRequest,
) error {
	path := fmt.Sprintf("/_fakecloud/route53/health-checks/%s/status", healthCheckID)
	return c.fc.doPost(ctx, path, req, nil)
}

// GetDnssecMaterial returns the active KSK material for a hosted zone
// so tests can verify DNSSEC signatures locally. Returns an error
// (typically *APIError with StatusCode 404) when the zone has no
// active KSK.
func (c *Route53Client) GetDnssecMaterial(ctx context.Context, hostedZoneID string) (*Route53DnssecMaterialResponse, error) {
	var out Route53DnssecMaterialResponse
	path := fmt.Sprintf("/_fakecloud/route53/zones/%s/dnssec", hostedZoneID)
	if err := c.fc.doGet(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SignRRset signs an RRset under the zone's first ACTIVE KSK and
// returns the raw RRSIG fields so tests can verify the signature
// against the zone's published DNSKEY material.
func (c *Route53Client) SignRRset(ctx context.Context, hostedZoneID string, req *Route53DnssecSignRequest) (*Route53DnssecSignResponse, error) {
	var out Route53DnssecSignResponse
	path := fmt.Sprintf("/_fakecloud/route53/zones/%s/dnssec/sign", hostedZoneID)
	if err := c.fc.doPost(ctx, path, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
