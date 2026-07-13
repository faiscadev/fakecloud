package fakecloud

import (
	"context"
	"fmt"
	"net/url"
)

// CloudFrontClient provides access to CloudFront admin endpoints.
type CloudFrontClient struct {
	fc *FakeCloud
}

// GetDistributions lists every CloudFront distribution across every account.
func (c *CloudFrontClient) GetDistributions(ctx context.Context) (*CloudFrontDistributionsResponse, error) {
	var out CloudFrontDistributionsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cloudfront/distributions", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetDistributionStatus flips a stored CloudFront Distribution's status
// (typically "Deployed" or "InProgress") so tests can synchronously force
// propagation without waiting on the periodic tick. Returns an APIError
// (StatusCode 404) when the distribution is not found.
func (c *CloudFrontClient) SetDistributionStatus(
	ctx context.Context,
	distributionID string,
	req *CloudFrontDistributionStatusRequest,
) error {
	path := fmt.Sprintf("/_fakecloud/cloudfront/distributions/%s/status", url.PathEscape(distributionID))
	return c.fc.doPost(ctx, path, req, nil)
}
