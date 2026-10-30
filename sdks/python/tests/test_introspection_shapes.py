"""Decoding of the waf-counts, webauthn-credentials and apigatewayv2 requests
introspection payloads."""

from __future__ import annotations

from fakecloud.types import (
    ApiGatewayV2Request,
    Elbv2WafCountsResponse,
    WebAuthnCredentialsResponse,
)

ARN = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/a/1"


def test_waf_counts_decodes_entries() -> None:
    out = Elbv2WafCountsResponse.from_dict(
        {
            "counts": [
                {
                    "webAclArn": ARN,
                    "ruleName": "count-admin",
                    "count": 3,
                }
            ]
        }
    )
    assert out.counts[0].web_acl_arn == ARN
    assert out.counts[0].rule_name == "count-admin"
    assert out.counts[0].count == 3


def test_webauthn_credentials_decode_camel_case() -> None:
    out = WebAuthnCredentialsResponse.from_dict(
        {
            "credentials": [
                {
                    "accountId": "123456789012",
                    "poolId": "us-east-1_abc",
                    "username": "a:b",
                    "credentialId": "cred",
                    "friendlyCredentialName": None,
                    "relyingPartyId": "rp",
                    "authenticatorAttachment": "platform",
                    "authenticatorTransports": ["internal"],
                    "createdAt": "2026-01-01T00:00:00+00:00",
                    "attestationInfo": {
                        "fmt": "packed",
                        "alg": -7,
                        "signatureLen": 70,
                        "x5cChainLen": 0,
                        "cosePublicKeyPresent": True,
                        "selfAttestVerified": True,
                    },
                },
                {
                    "accountId": "123456789012",
                    "poolId": "us-east-1_abc",
                    "username": "c",
                    "credentialId": "cred2",
                    "friendlyCredentialName": None,
                    "relyingPartyId": "rp",
                    "authenticatorAttachment": None,
                    "authenticatorTransports": [],
                    "createdAt": "2026-01-01T00:00:00+00:00",
                    "attestationInfo": None,
                },
            ]
        }
    )
    first, second = out.credentials
    assert (first.pool_id, first.username) == ("us-east-1_abc", "a:b")
    assert first.authenticator_transports == ["internal"]
    assert first.attestation_info is not None
    assert first.attestation_info.alg == -7
    assert first.attestation_info.self_attest_verified
    assert second.attestation_info is None


def test_apigatewayv2_request_decodes_base64_flag() -> None:
    req = ApiGatewayV2Request.from_dict(
        {
            "requestId": "r",
            "apiId": "a",
            "stage": "prod",
            "method": "POST",
            "path": "/u",
            "headers": {"X-Trace-Id": "abc"},
            "queryParams": {"pageSize": "10"},
            "body": "/wD+",
            "isBase64Encoded": True,
            "timestamp": "t",
            "statusCode": 200,
        }
    )
    assert req.is_base64_encoded
    assert req.body == "/wD+"
    # User-supplied keys keep their spelling.
    assert req.headers == {"X-Trace-Id": "abc"}
    assert req.query_params == {"pageSize": "10"}
