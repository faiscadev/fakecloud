package dev.fakecloud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.fakecloud.Types.ApiGatewayV2RequestsResponse;
import dev.fakecloud.Types.Elbv2WafCountsResponse;
import dev.fakecloud.Types.WebAuthnCredential;
import dev.fakecloud.Types.WebAuthnCredentialsResponse;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Decoding of the waf-counts, webauthn-credentials and apigatewayv2 requests payloads. */
class IntrospectionShapesTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void wafCountsDecodeEntries() throws Exception {
        Elbv2WafCountsResponse out = mapper.readValue(
                "{\"counts\":[{\"webAclArn\":\"arn:acl\",\"ruleName\":\"count-admin\",\"count\":3}]}",
                Elbv2WafCountsResponse.class);
        assertEquals("arn:acl", out.counts().get(0).webAclArn());
        assertEquals("count-admin", out.counts().get(0).ruleName());
        assertEquals(3, out.counts().get(0).count());
    }

    @Test
    void webAuthnCredentialsDecodeCamelCase() throws Exception {
        WebAuthnCredentialsResponse out = mapper.readValue(
                "{\"credentials\":[{\"accountId\":\"123456789012\",\"poolId\":\"us-east-1_abc\","
                        + "\"username\":\"a:b\",\"credentialId\":\"cred\",\"friendlyCredentialName\":null,"
                        + "\"relyingPartyId\":\"rp\",\"authenticatorAttachment\":\"platform\","
                        + "\"authenticatorTransports\":[\"internal\"],\"createdAt\":\"t\","
                        + "\"attestationInfo\":{\"fmt\":\"packed\",\"alg\":-7,\"signatureLen\":70,"
                        + "\"x5cChainLen\":0,\"cosePublicKeyPresent\":true,\"selfAttestVerified\":true}},"
                        + "{\"accountId\":\"123456789012\",\"poolId\":\"us-east-1_abc\",\"username\":\"c\","
                        + "\"credentialId\":\"cred2\",\"relyingPartyId\":\"rp\",\"authenticatorTransports\":[],"
                        + "\"createdAt\":\"t\",\"attestationInfo\":null}]}",
                WebAuthnCredentialsResponse.class);
        WebAuthnCredential first = out.credentials().get(0);
        assertEquals("us-east-1_abc", first.poolId());
        assertEquals("a:b", first.username());
        assertEquals(List.of("internal"), first.authenticatorTransports());
        assertEquals(-7, first.attestationInfo().alg());
        assertTrue(first.attestationInfo().selfAttestVerified());
        assertNull(out.credentials().get(1).attestationInfo());
    }

    @Test
    void apiGatewayV2RequestDecodesBase64Flag() throws Exception {
        ApiGatewayV2RequestsResponse out = mapper.readValue(
                "{\"requests\":[{\"requestId\":\"r\",\"apiId\":\"a\",\"stage\":\"prod\",\"method\":\"POST\","
                        + "\"path\":\"/u\",\"headers\":{},\"queryParams\":{},\"body\":\"/wD+\","
                        + "\"isBase64Encoded\":true,\"timestamp\":\"t\",\"statusCode\":200}]}",
                ApiGatewayV2RequestsResponse.class);
        assertTrue(out.requests().get(0).isBase64Encoded());
        assertEquals("/wD+", out.requests().get(0).body());
    }
}
