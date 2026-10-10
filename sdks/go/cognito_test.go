package fakecloud

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestCognitoSetSoftwareToken(t *testing.T) {
	fc, seen := stubServer(t, 200, `{"enrolled":true}`)
	out, err := fc.Cognito().SetSoftwareToken(context.Background(), &SetSoftwareTokenRequest{
		UserPoolID: "us-east-1_Local",
		Username:   "alice",
		SecretCode: "JBSWY3DPEHPK3PXP",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := (*seen)[0]
	if got.method != http.MethodPost || got.uri != "/_fakecloud/cognito/software-token" {
		t.Fatalf("got %s %s", got.method, got.uri)
	}
	want := `{"userPoolId":"us-east-1_Local","username":"alice","secretCode":"JBSWY3DPEHPK3PXP"}`
	if got.body != want {
		t.Fatalf("body = %s, want %s", got.body, want)
	}
	if !out.Enrolled {
		t.Fatal("expected enrolled")
	}
}

func TestCognitoSetSoftwareTokenError(t *testing.T) {
	fc, _ := stubServer(t, 404, `{"error":"user not found in pool"}`)
	_, err := fc.Cognito().SetSoftwareToken(context.Background(), &SetSoftwareTokenRequest{
		UserPoolID: "us-east-1_Local",
		Username:   "nobody",
		SecretCode: "JBSWY3DPEHPK3PXP",
	})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 {
		t.Fatalf("expected APIError 404, got %v", err)
	}
}
