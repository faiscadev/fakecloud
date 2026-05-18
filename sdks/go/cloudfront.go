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
