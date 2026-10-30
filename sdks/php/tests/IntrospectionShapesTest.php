<?php

declare(strict_types=1);

namespace FakeCloud\Tests;

use FakeCloud\ApiGatewayV2Request;
use FakeCloud\Elbv2WafCountsResponse;
use FakeCloud\WebAuthnCredentialsResponse;
use PHPUnit\Framework\TestCase;

final class IntrospectionShapesTest extends TestCase
{
    public function testWafCountsDecodeEntries(): void
    {
        $resp = Elbv2WafCountsResponse::fromArray([
            'counts' => [['webAclArn' => 'arn:acl', 'ruleName' => 'count-admin', 'count' => 3]],
        ]);
        $this->assertSame('arn:acl', $resp->counts[0]->webAclArn);
        $this->assertSame('count-admin', $resp->counts[0]->ruleName);
        $this->assertSame(3, $resp->counts[0]->count);
    }

    public function testWebAuthnCredentialsDecodeCamelCase(): void
    {
        $resp = WebAuthnCredentialsResponse::fromArray([
            'credentials' => [[
                'accountId' => '123456789012',
                'poolId' => 'us-east-1_abc',
                'username' => 'a:b',
                'credentialId' => 'cred',
                'friendlyCredentialName' => null,
                'relyingPartyId' => 'rp',
                'authenticatorAttachment' => 'platform',
                'authenticatorTransports' => ['internal'],
                'createdAt' => '2026-01-01T00:00:00+00:00',
                'attestationInfo' => [
                    'fmt' => 'packed',
                    'alg' => -7,
                    'signatureLen' => 70,
                    'x5cChainLen' => 0,
                    'cosePublicKeyPresent' => true,
                    'selfAttestVerified' => true,
                ],
            ]],
        ]);
        $cred = $resp->credentials[0];
        $this->assertSame('us-east-1_abc', $cred->poolId);
        $this->assertSame('a:b', $cred->username);
        $this->assertSame(['internal'], $cred->authenticatorTransports);
        $this->assertSame(-7, $cred->attestationInfo?->alg);
        $this->assertTrue($cred->attestationInfo?->selfAttestVerified);
    }

    public function testApiGatewayV2RequestDecodesBase64Flag(): void
    {
        $req = ApiGatewayV2Request::fromArray([
            'requestId' => 'r',
            'apiId' => 'a',
            'stage' => 'prod',
            'method' => 'POST',
            'path' => '/u',
            'headers' => [],
            'queryParams' => [],
            'body' => '/wD+',
            'isBase64Encoded' => true,
            'timestamp' => 't',
            'statusCode' => 200,
        ]);
        $this->assertTrue($req->isBase64Encoded);
        $this->assertSame('/wD+', $req->body);
    }
}
