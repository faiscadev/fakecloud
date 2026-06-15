package fakecloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// CognitoClient provides access to Cognito introspection endpoints.
type CognitoClient struct {
	fc *FakeCloud
}

// GetUserCodes returns confirmation codes for a specific user.
func (c *CognitoClient) GetUserCodes(ctx context.Context, poolID, username string) (*UserConfirmationCodes, error) {
	var out UserConfirmationCodes
	if err := c.fc.doGet(ctx, fmt.Sprintf("/_fakecloud/cognito/confirmation-codes/%s/%s", poolID, username), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetConfirmationCodes lists all confirmation codes across all pools.
func (c *CognitoClient) GetConfirmationCodes(ctx context.Context) (*ConfirmationCodesResponse, error) {
	var out ConfirmationCodesResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cognito/confirmation-codes", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ConfirmUser confirms a Cognito user, bypassing email/phone verification.
// Returns an APIError with status 404 if the user is not found.
func (c *CognitoClient) ConfirmUser(ctx context.Context, req *ConfirmUserRequest) (*ConfirmUserResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.fc.BaseURL+"/_fakecloud/cognito/confirm-user",
		strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.fc.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var out ConfirmUserResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}

	if resp.StatusCode == 404 {
		errMsg := ""
		if out.Error != nil {
			errMsg = *out.Error
		}
		return nil, &APIError{StatusCode: 404, Body: errMsg}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &APIError{StatusCode: resp.StatusCode, Body: string(body)}
	}

	return &out, nil
}

// GetTokens lists all active Cognito tokens.
func (c *CognitoClient) GetTokens(ctx context.Context) (*TokensResponse, error) {
	var out TokensResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cognito/tokens", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ExpireTokens expires Cognito tokens, optionally filtered by pool/user.
func (c *CognitoClient) ExpireTokens(ctx context.Context, req *ExpireTokensRequest) (*ExpireTokensResponse, error) {
	var out ExpireTokensResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/cognito/expire-tokens", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetAuthEvents lists Cognito authentication events.
func (c *CognitoClient) GetAuthEvents(ctx context.Context) (*AuthEventsResponse, error) {
	var out AuthEventsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cognito/auth-events", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// MintAuthorizationCode mints a single-use OAuth2 authorization code
// that the /oauth2/token authorization_code grant can later redeem.
// Programmatic alternative to driving /oauth2/authorize.
func (c *CognitoClient) MintAuthorizationCode(ctx context.Context, req *MintAuthorizationCodeRequest) (*MintAuthorizationCodeResponse, error) {
	var out MintAuthorizationCodeResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/cognito/authorization-codes", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetCompromisedPasswords registers plaintext passwords as compromised.
// Each is SHA-256 hashed server-side and added to the per-account
// compromised-password set, after which `SignUp` / `AdminInitiateAuth`
// will fail with `InvalidPasswordException` on any pool whose
// `CompromisedCredentialsRiskConfiguration.Actions.EventAction` is
// `BLOCK`.
func (c *CognitoClient) SetCompromisedPasswords(ctx context.Context, req *CompromisedPasswordsRequest) (*CompromisedPasswordsResponse, error) {
	var out CompromisedPasswordsResponse
	if err := c.fc.doPost(ctx, "/_fakecloud/cognito/compromised-passwords", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetPreTokenGenInvocations returns the PreTokenGeneration Lambda
// trigger invocation log recorded by InitiateAuth. Each entry has the
// full request/response payloads plus pre-parsed ClaimsAdded,
// ClaimsOverridden, and GroupOverrides so tests can assert claim
// mutation flows without inspecting the issued JWT.
func (c *CognitoClient) GetPreTokenGenInvocations(ctx context.Context) (*PreTokenGenInvocationsResponse, error) {
	var out PreTokenGenInvocationsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cognito/pretokengen/invocations", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetWebAuthnCredentials lists every registered WebAuthn credential
// across pools and users, including the parsed packed-attestation
// info captured at registration time.
func (c *CognitoClient) GetWebAuthnCredentials(ctx context.Context) (*WebAuthnCredentialsResponse, error) {
	var out WebAuthnCredentialsResponse
	if err := c.fc.doGet(ctx, "/_fakecloud/cognito/webauthn-credentials", &out); err != nil {
		return nil, err
	}
	return &out, nil
}
