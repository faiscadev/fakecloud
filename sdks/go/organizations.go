package fakecloud

import "context"

// OrganizationsClient provides access to AWS Organizations admin/introspection endpoints.
type OrganizationsClient struct {
	fc *FakeCloud
}

// GetAccounts lists every member account in the org with lifecycle
// state, parent OU, tags, and directly-attached SCPs. Returns an empty
// account list (and nil management/master ids) when no organization
// has been created yet.
func (c *OrganizationsClient) GetAccounts(ctx context.Context) (*OrganizationsAccountsResponse, error) {
	var out OrganizationsAccountsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/organizations/accounts", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetResponsibilityTransfers lists every billing responsibility transfer
// in the org, with direction (INBOUND/OUTBOUND), lifecycle status, and
// the active handshake. Returns an empty list when no organization has
// been created. Order is stable: by id.
func (c *OrganizationsClient) GetResponsibilityTransfers(ctx context.Context) (*OrganizationsResponsibilityTransfersResponse, error) {
	var out OrganizationsResponsibilityTransfersResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/organizations/responsibility-transfers", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
