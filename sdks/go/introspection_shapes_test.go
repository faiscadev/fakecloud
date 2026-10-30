package fakecloud

import (
	"context"
	"testing"
)

func TestELBv2GetWafCountsDecodesEntries(t *testing.T) {
	fc, seen := stubServer(t, 200, `{"counts":[{"webAclArn":"arn:aws:wafv2:us-east-1:123456789012:regional/webacl/a/1","ruleName":"count-admin","count":3}]}`)
	out, err := fc.ELBv2().GetWafCounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if (*seen)[0].uri != "/_fakecloud/elbv2/waf-counts" {
		t.Fatalf("unexpected uri %s", (*seen)[0].uri)
	}
	c := out.Counts[0]
	if c.WebACLArn != "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/a/1" || c.RuleName != "count-admin" || c.Count != 3 {
		t.Fatalf("unexpected decode: %+v", c)
	}
}

func TestCognitoGetWebAuthnCredentialsDecodesCamelCase(t *testing.T) {
	fc, _ := stubServer(t, 200, `{"credentials":[{"accountId":"123456789012","poolId":"us-east-1_abc","username":"a:b","credentialId":"cred","friendlyCredentialName":null,"relyingPartyId":"rp","authenticatorAttachment":"platform","authenticatorTransports":["internal"],"createdAt":"2026-01-01T00:00:00+00:00","attestationInfo":{"fmt":"packed","alg":-7,"signatureLen":70,"x5cChainLen":0,"cosePublicKeyPresent":true,"selfAttestVerified":true}}]}`)
	out, err := fc.Cognito().GetWebAuthnCredentials(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := out.Credentials[0]
	if c.PoolID != "us-east-1_abc" || c.Username != "a:b" || c.CredentialID != "cred" ||
		c.AuthenticatorTransports[0] != "internal" || c.AttestationInfo == nil ||
		c.AttestationInfo.Alg != -7 || !c.AttestationInfo.SelfAttestVerified {
		t.Fatalf("unexpected decode: %+v", c)
	}
}

func TestApiGatewayV2GetRequestsDecodesBase64Flag(t *testing.T) {
	fc, _ := stubServer(t, 200, `{"requests":[{"requestId":"r","apiId":"a","stage":"prod","method":"POST","path":"/u","headers":{},"queryParams":{},"body":"/wD+","isBase64Encoded":true,"timestamp":"t","statusCode":200}]}`)
	out, err := fc.ApiGatewayV2().GetRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := out.Requests[0]
	if !r.IsBase64Encoded || r.Body == nil || *r.Body != "/wD+" {
		t.Fatalf("unexpected decode: %+v", r)
	}
}
