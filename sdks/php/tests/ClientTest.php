<?php

declare(strict_types=1);

namespace FakeCloud\Tests;

use FakeCloud\ApiGatewayV2Request;
use FakeCloud\ApiGatewayV2RequestsResponse;
use FakeCloud\AuthEvent;
use FakeCloud\AuthEventsResponse;
use FakeCloud\BedrockFaultRule;
use FakeCloud\BedrockFaultRuleState;
use FakeCloud\BedrockFaultsResponse;
use FakeCloud\BedrockInvocation;
use FakeCloud\BedrockInvocationsResponse;
use FakeCloud\BedrockModelResponseConfig;
use FakeCloud\BedrockResponseRule;
use FakeCloud\BedrockStatusResponse;
use FakeCloud\ConfirmationCode;
use FakeCloud\ConfirmationCodesResponse;
use FakeCloud\ConfirmSubscriptionRequest;
use FakeCloud\ConfirmSubscriptionResponse;
use FakeCloud\CloudFrontDistribution;
use FakeCloud\CloudFrontDistributionsResponse;
use FakeCloud\ConfirmUserRequest;
use FakeCloud\ConfirmUserResponse;
use FakeCloud\ElastiCacheCluster;
use FakeCloud\ElastiCacheClustersResponse;
use FakeCloud\ElastiCacheReplicationGroup;
use FakeCloud\ElastiCacheReplicationGroupsResponse;
use FakeCloud\ElastiCacheServerlessCache;
use FakeCloud\ElastiCacheServerlessCachesResponse;
use FakeCloud\EventBridgeDeliveries;
use FakeCloud\EventBridgeEvent;
use FakeCloud\EventHistoryResponse;
use FakeCloud\EvictContainerResponse;
use FakeCloud\ExpirationTickResponse;
use FakeCloud\ExpireTokensRequest;
use FakeCloud\ExpireTokensResponse;
use FakeCloud\FakeCloud;
use FakeCloud\FakeCloudError;
use FakeCloud\FireRuleRequest;
use FakeCloud\FireRuleResponse;
use FakeCloud\FireRuleTarget;
use FakeCloud\ForceDlqResponse;
use FakeCloud\HealthResponse;
use FakeCloud\HttpTransport;
use FakeCloud\InboundEmailRequest;
use FakeCloud\InboundEmailResponse;
use FakeCloud\LambdaInvocation;
use FakeCloud\LambdaInvocationsResponse;
use FakeCloud\LifecycleTickResponse;
use FakeCloud\PendingConfirmation;
use FakeCloud\PendingConfirmationsResponse;
use FakeCloud\RdsInstance;
use FakeCloud\RdsInstancesResponse;
use FakeCloud\RdsTag;
use FakeCloud\ResetResponse;
use FakeCloud\ResetServiceResponse;
use FakeCloud\RotationTickResponse;
use FakeCloud\S3Notification;
use FakeCloud\S3NotificationsResponse;
use FakeCloud\SentEmail;
use FakeCloud\SesEmailsResponse;
use FakeCloud\SnsMessage;
use FakeCloud\SnsMessagesResponse;
use FakeCloud\SqsMessageInfo;
use FakeCloud\SqsMessagesResponse;
use FakeCloud\SqsQueueMessages;
use FakeCloud\StepFunctionsExecution;
use FakeCloud\StepFunctionsExecutionsResponse;
use FakeCloud\TokenInfo;
use FakeCloud\TokensResponse;
use FakeCloud\TtlTickResponse;
use FakeCloud\UserConfirmationCodes;
use FakeCloud\WarmContainer;
use FakeCloud\WarmContainersResponse;
use PHPUnit\Framework\TestCase;

final class ClientTest extends TestCase
{
    public function testDefaultBaseUrl(): void
    {
        $fc = new FakeCloud();
        $this->assertSame('http://localhost:4566', $fc->baseUrl());
    }

    public function testCustomBaseUrl(): void
    {
        $fc = new FakeCloud('http://custom:1234');
        $this->assertSame('http://custom:1234', $fc->baseUrl());
    }

    public function testTrailingSlashStripped(): void
    {
        $fc = new FakeCloud('http://localhost:4566///');
        $this->assertSame('http://localhost:4566', $fc->baseUrl());
    }

    public function testEncodePath(): void
    {
        $this->assertSame('hello%20world', HttpTransport::encodePath('hello world'));
        $this->assertSame('a%2Fb', HttpTransport::encodePath('a/b'));
        $this->assertSame('simple', HttpTransport::encodePath('simple'));
    }

    // ── Type deserialization ───────────────────────────────────────

    public function testHealthResponseFromArray(): void
    {
        $resp = HealthResponse::fromArray([
            'status' => 'ok',
            'version' => '0.9.2',
            'services' => ['s3', 'sqs'],
        ]);
        $this->assertSame('ok', $resp->status);
        $this->assertSame('0.9.2', $resp->version);
        $this->assertSame(['s3', 'sqs'], $resp->services);
    }

    public function testResetResponseFromArray(): void
    {
        $resp = ResetResponse::fromArray(['status' => 'ok']);
        $this->assertSame('ok', $resp->status);
    }

    public function testResetServiceResponseFromArray(): void
    {
        $resp = ResetServiceResponse::fromArray(['reset' => 'sqs']);
        $this->assertSame('sqs', $resp->reset);
    }

    public function testLambdaInvocationsResponseFromArray(): void
    {
        $resp = LambdaInvocationsResponse::fromArray([
            'invocations' => [
                ['functionArn' => 'arn:aws:lambda:us-east-1:123:function:my-fn', 'payload' => '{}', 'source' => 'sdk', 'timestamp' => '2026-01-01T00:00:00Z'],
            ],
        ]);
        $this->assertCount(1, $resp->invocations);
        $this->assertStringEndsWith(':my-fn', $resp->invocations[0]->functionArn);
    }

    public function testWarmContainersResponseFromArray(): void
    {
        $resp = WarmContainersResponse::fromArray([
            'containers' => [
                ['functionName' => 'fn1', 'runtime' => 'nodejs20.x', 'containerId' => 'abc123', 'lastUsedSecsAgo' => 42],
            ],
        ]);
        $this->assertCount(1, $resp->containers);
        $this->assertSame(42, $resp->containers[0]->lastUsedSecsAgo);
    }

    public function testEvictContainerResponseFromArray(): void
    {
        $resp = EvictContainerResponse::fromArray(['evicted' => true]);
        $this->assertTrue($resp->evicted);
    }

    public function testSesEmailsResponseFromArray(): void
    {
        $resp = SesEmailsResponse::fromArray([
            'emails' => [
                [
                    'messageId' => 'msg-1',
                    'from' => 'a@b.com',
                    'to' => ['c@d.com'],
                    'cc' => [],
                    'bcc' => [],
                    'subject' => 'Hello',
                    'htmlBody' => '<b>hi</b>',
                    'textBody' => 'hi',
                    'rawData' => null,
                    'templateName' => null,
                    'templateData' => null,
                    'timestamp' => '2026-01-01T00:00:00Z',
                ],
            ],
        ]);
        $this->assertCount(1, $resp->emails);
        $this->assertSame('Hello', $resp->emails[0]->subject);
    }

    public function testInboundEmailRequestToArray(): void
    {
        $req = new InboundEmailRequest('a@b.com', ['c@d.com'], 'Test', 'Body');
        $this->assertSame([
            'from' => 'a@b.com',
            'to' => ['c@d.com'],
            'subject' => 'Test',
            'body' => 'Body',
        ], $req->toArray());
    }

    public function testSnsMessagesResponseFromArray(): void
    {
        $resp = SnsMessagesResponse::fromArray([
            'messages' => [
                ['messageId' => 'm1', 'topicArn' => 'arn:aws:sns:us-east-1:123:topic', 'message' => 'hello', 'subject' => null, 'timestamp' => '2026-01-01T00:00:00Z'],
            ],
        ]);
        $this->assertCount(1, $resp->messages);
        $this->assertNull($resp->messages[0]->subject);
    }

    public function testSqsMessagesResponseFromArray(): void
    {
        $resp = SqsMessagesResponse::fromArray([
            'queues' => [
                [
                    'queueUrl' => 'http://localhost:4566/queue/test-queue',
                    'queueName' => 'test-queue',
                    'messages' => [
                        ['messageId' => 'm1', 'body' => 'hi', 'receiveCount' => 0, 'inFlight' => false, 'createdAt' => '2026-01-01T00:00:00Z'],
                    ],
                ],
            ],
        ]);
        $this->assertCount(1, $resp->queues);
        $this->assertSame('test-queue', $resp->queues[0]->queueName);
        $this->assertCount(1, $resp->queues[0]->messages);
    }

    public function testEventHistoryResponseFromArray(): void
    {
        $resp = EventHistoryResponse::fromArray([
            'events' => [
                ['eventId' => 'e1', 'source' => 'my.app', 'detailType' => 'OrderPlaced', 'detail' => '{}', 'busName' => 'default', 'timestamp' => '2026-01-01T00:00:00Z'],
            ],
            'deliveries' => ['lambda' => [], 'logs' => []],
        ]);
        $this->assertCount(1, $resp->events);
        $this->assertEmpty($resp->deliveries->lambda);
    }

    public function testFireRuleRequestToArray(): void
    {
        $req = new FireRuleRequest('my-rule');
        $this->assertSame(['ruleName' => 'my-rule'], $req->toArray());

        $req2 = new FireRuleRequest('my-rule', 'custom-bus');
        $this->assertSame(['ruleName' => 'my-rule', 'busName' => 'custom-bus'], $req2->toArray());
    }

    public function testRdsInstancesResponseFromArray(): void
    {
        $resp = RdsInstancesResponse::fromArray([
            'instances' => [
                [
                    'dbInstanceIdentifier' => 'test-db',
                    'dbInstanceArn' => 'arn:aws:rds:us-east-1:123:db:test-db',
                    'dbInstanceClass' => 'db.t3.micro',
                    'engine' => 'postgres',
                    'engineVersion' => '15.4',
                    'dbInstanceStatus' => 'available',
                    'masterUsername' => 'admin',
                    'dbName' => 'testdb',
                    'endpointAddress' => 'localhost',
                    'port' => 5432,
                    'allocatedStorage' => 20,
                    'publiclyAccessible' => false,
                    'deletionProtection' => false,
                    'createdAt' => '2026-01-01T00:00:00Z',
                    'dbiResourceId' => 'db-ABC123',
                    'containerId' => 'ctr-123',
                    'hostPort' => 55432,
                    'tags' => [['key' => 'env', 'value' => 'test']],
                ],
            ],
        ]);
        $this->assertCount(1, $resp->instances);
        $this->assertSame('test-db', $resp->instances[0]->dbInstanceIdentifier);
        $this->assertCount(1, $resp->instances[0]->tags);
        $this->assertSame('env', $resp->instances[0]->tags[0]->key);
    }

    public function testElastiCacheResponsesFromArray(): void
    {
        $clusters = ElastiCacheClustersResponse::fromArray([
            'clusters' => [
                ['cacheClusterId' => 'c1', 'cacheClusterStatus' => 'available', 'engine' => 'redis', 'engineVersion' => '7.0', 'cacheNodeType' => 'cache.t3.micro', 'numCacheNodes' => 1, 'replicationGroupId' => null, 'port' => 6379, 'hostPort' => 56379, 'containerId' => 'ctr'],
            ],
        ]);
        $this->assertCount(1, $clusters->clusters);

        $replGroups = ElastiCacheReplicationGroupsResponse::fromArray([
            'replicationGroups' => [
                ['replicationGroupId' => 'rg1', 'status' => 'available', 'description' => 'test', 'memberClusters' => ['c1'], 'automaticFailover' => true, 'multiAz' => false, 'engine' => 'redis', 'engineVersion' => '7.0', 'cacheNodeType' => 'cache.t3.micro', 'numCacheClusters' => 1],
            ],
        ]);
        $this->assertCount(1, $replGroups->replicationGroups);

        $serverless = ElastiCacheServerlessCachesResponse::fromArray([
            'serverlessCaches' => [
                ['serverlessCacheName' => 'sc1', 'status' => 'available', 'engine' => 'redis', 'engineVersion' => '7.0', 'cacheNodeType' => null],
            ],
        ]);
        $this->assertCount(1, $serverless->serverlessCaches);
    }

    public function testCognitoTypesFromArray(): void
    {
        $codes = UserConfirmationCodes::fromArray(['confirmationCode' => '123456', 'attributeVerificationCodes' => ['email' => '654321']]);
        $this->assertSame('123456', $codes->confirmationCode);

        $allCodes = ConfirmationCodesResponse::fromArray([
            'codes' => [
                ['poolId' => 'pool-1', 'username' => 'user1', 'code' => '123', 'type' => 'signup', 'attribute' => null],
            ],
        ]);
        $this->assertCount(1, $allCodes->codes);

        $confirmResp = ConfirmUserResponse::fromArray(['confirmed' => true, 'error' => null]);
        $this->assertTrue($confirmResp->confirmed);

        $tokens = TokensResponse::fromArray([
            'tokens' => [
                ['type' => 'access', 'username' => 'user1', 'poolId' => 'pool-1', 'clientId' => 'client-1', 'issuedAt' => 1700000000],
            ],
        ]);
        $this->assertCount(1, $tokens->tokens);

        $expireResp = ExpireTokensResponse::fromArray(['expiredTokens' => 5]);
        $this->assertSame(5, $expireResp->expiredTokens);

        $events = AuthEventsResponse::fromArray([
            'events' => [
                ['eventType' => 'SignIn', 'username' => 'user1', 'userPoolId' => 'pool-1', 'clientId' => 'client-1', 'timestamp' => 1700000000, 'success' => true],
            ],
        ]);
        $this->assertCount(1, $events->events);
    }

    public function testStepFunctionsResponseFromArray(): void
    {
        $resp = StepFunctionsExecutionsResponse::fromArray([
            'executions' => [
                ['executionArn' => 'arn:exec', 'stateMachineArn' => 'arn:sm', 'name' => 'exec-1', 'status' => 'SUCCEEDED', 'startDate' => '2026-01-01T00:00:00Z', 'input' => '{}', 'output' => '{"result":1}', 'stopDate' => '2026-01-01T00:00:01Z'],
            ],
        ]);
        $this->assertCount(1, $resp->executions);
        $this->assertSame('SUCCEEDED', $resp->executions[0]->status);
    }

    public function testApiGatewayV2ResponseFromArray(): void
    {
        $resp = ApiGatewayV2RequestsResponse::fromArray([
            'requests' => [
                ['requestId' => 'r1', 'apiId' => 'api1', 'stage' => '$default', 'method' => 'GET', 'path' => '/hello', 'headers' => ['host' => 'localhost'], 'queryParams' => ['q' => 'test'], 'body' => null, 'timestamp' => '2026-01-01T00:00:00Z', 'statusCode' => 200],
            ],
        ]);
        $this->assertCount(1, $resp->requests);
        $this->assertSame(200, $resp->requests[0]->statusCode);
    }

    public function testBedrockTypesFromArray(): void
    {
        $invocations = BedrockInvocationsResponse::fromArray([
            'invocations' => [
                ['modelId' => 'anthropic.claude-3', 'input' => 'hi', 'output' => 'hello', 'timestamp' => '2026-01-01T00:00:00Z', 'error' => null],
            ],
        ]);
        $this->assertCount(1, $invocations->invocations);

        $config = BedrockModelResponseConfig::fromArray(['status' => 'ok', 'modelId' => 'test-model']);
        $this->assertSame('ok', $config->status);

        $faults = BedrockFaultsResponse::fromArray([
            'faults' => [
                ['errorType' => 'ThrottlingException', 'message' => 'too fast', 'httpStatus' => 429, 'remaining' => 3, 'modelId' => null, 'operation' => null],
            ],
        ]);
        $this->assertCount(1, $faults->faults);
        $this->assertSame(3, $faults->faults[0]->remaining);
    }

    public function testBedrockFaultRuleToArray(): void
    {
        $rule = new BedrockFaultRule('ThrottlingException');
        $this->assertSame(['errorType' => 'ThrottlingException'], $rule->toArray());

        $rule2 = new BedrockFaultRule('ThrottlingException', 'too fast', 429, 3, 'model-1', 'InvokeModel');
        $expected = [
            'errorType' => 'ThrottlingException',
            'message' => 'too fast',
            'httpStatus' => 429,
            'count' => 3,
            'modelId' => 'model-1',
            'operation' => 'InvokeModel',
        ];
        $this->assertSame($expected, $rule2->toArray());
    }

    public function testBedrockResponseRuleToArray(): void
    {
        $rule = new BedrockResponseRule('buy now', '{"label":"spam"}');
        $this->assertSame(['response' => '{"label":"spam"}', 'promptContains' => 'buy now'], $rule->toArray());

        $catchAll = new BedrockResponseRule(null, '{"label":"ham"}');
        $this->assertSame(['response' => '{"label":"ham"}'], $catchAll->toArray());
    }

    public function testExpireTokensRequestToArray(): void
    {
        $req = new ExpireTokensRequest();
        $this->assertSame([], $req->toArray());

        $req2 = new ExpireTokensRequest('pool-1', 'user1');
        $this->assertSame(['userPoolId' => 'pool-1', 'username' => 'user1'], $req2->toArray());
    }

    public function testConfirmSubscriptionRequestToArray(): void
    {
        $req = new ConfirmSubscriptionRequest('arn:sub');
        $this->assertSame(['subscriptionArn' => 'arn:sub'], $req->toArray());
    }

    public function testConfirmUserRequestToArray(): void
    {
        $req = new ConfirmUserRequest('pool-1', 'user1');
        $this->assertSame(['userPoolId' => 'pool-1', 'username' => 'user1'], $req->toArray());
    }

    public function testFakeCloudErrorMessage(): void
    {
        $err = new FakeCloudError(404, 'not found');
        $this->assertSame(404, $err->status);
        $this->assertSame('not found', $err->body);
        $this->assertSame('fakecloud API error (404): not found', $err->getMessage());
    }

    public function testSubClientAccessors(): void
    {
        $fc = new FakeCloud();
        $this->assertInstanceOf(\FakeCloud\LambdaClient::class, $fc->lambda());
        $this->assertInstanceOf(\FakeCloud\RdsClient::class, $fc->rds());
        $this->assertInstanceOf(\FakeCloud\ElastiCacheClient::class, $fc->elasticache());
        $this->assertInstanceOf(\FakeCloud\SesClient::class, $fc->ses());
        $this->assertInstanceOf(\FakeCloud\SnsClient::class, $fc->sns());
        $this->assertInstanceOf(\FakeCloud\SqsClient::class, $fc->sqs());
        $this->assertInstanceOf(\FakeCloud\EventsClient::class, $fc->events());
        $this->assertInstanceOf(\FakeCloud\S3Client::class, $fc->s3());
        $this->assertInstanceOf(\FakeCloud\DynamoDbClient::class, $fc->dynamodb());
        $this->assertInstanceOf(\FakeCloud\SecretsManagerClient::class, $fc->secretsmanager());
        $this->assertInstanceOf(\FakeCloud\CognitoClient::class, $fc->cognito());
        $this->assertInstanceOf(\FakeCloud\ApiGatewayV2Client::class, $fc->apigatewayv2());
        $this->assertInstanceOf(\FakeCloud\StepFunctionsClient::class, $fc->stepfunctions());
        $this->assertInstanceOf(\FakeCloud\BedrockClient::class, $fc->bedrock());
    }

    public function testTickerResponsesFromArray(): void
    {
        $ttl = TtlTickResponse::fromArray(['expiredItems' => 10]);
        $this->assertSame(10, $ttl->expiredItems);

        $rotation = RotationTickResponse::fromArray(['rotatedSecrets' => ['secret-1', 'secret-2']]);
        $this->assertSame(['secret-1', 'secret-2'], $rotation->rotatedSecrets);

        $expiration = ExpirationTickResponse::fromArray(['expiredMessages' => 3]);
        $this->assertSame(3, $expiration->expiredMessages);

        $dlq = ForceDlqResponse::fromArray(['movedMessages' => 5]);
        $this->assertSame(5, $dlq->movedMessages);

        $lifecycle = LifecycleTickResponse::fromArray(['processedBuckets' => 2, 'expiredObjects' => 4, 'transitionedObjects' => 1]);
        $this->assertSame(2, $lifecycle->processedBuckets);
        $this->assertSame(4, $lifecycle->expiredObjects);
        $this->assertSame(1, $lifecycle->transitionedObjects);
    }

    public function testS3NotificationsFromArray(): void
    {
        $resp = S3NotificationsResponse::fromArray([
            'notifications' => [
                ['bucket' => 'my-bucket', 'key' => 'file.txt', 'eventType' => 's3:ObjectCreated:Put', 'timestamp' => '2026-01-01T00:00:00Z'],
            ],
        ]);
        $this->assertCount(1, $resp->notifications);
        $this->assertSame('my-bucket', $resp->notifications[0]->bucket);
    }

    public function testPendingConfirmationsFromArray(): void
    {
        $resp = PendingConfirmationsResponse::fromArray([
            'pendingConfirmations' => [
                ['subscriptionArn' => 'arn:sub', 'topicArn' => 'arn:topic', 'protocol' => 'https', 'endpoint' => 'https://example.com', 'token' => 'tok-123'],
            ],
        ]);
        $this->assertCount(1, $resp->pendingConfirmations);
        $this->assertSame('tok-123', $resp->pendingConfirmations[0]->token);
    }

    public function testInboundEmailResponseFromArray(): void
    {
        $resp = InboundEmailResponse::fromArray([
            'messageId' => 'msg-1',
            'matchedRules' => ['rule-1'],
            'actionsExecuted' => [
                ['rule' => 'rule-1', 'actionType' => 'Lambda'],
            ],
        ]);
        $this->assertSame('msg-1', $resp->messageId);
        $this->assertCount(1, $resp->actionsExecuted);
    }

    public function testCloudFrontDistributionsResponseFromArray(): void
    {
        $resp = CloudFrontDistributionsResponse::fromArray([
            'distributions' => [
                ['id' => 'E1ABCDEF', 'domainName' => 'E1ABCDEF.cloudfront.net', 'enabled' => true, 'served' => true],
                ['id' => 'E2GHIJKL', 'domainName' => 'E2GHIJKL.cloudfront.net', 'enabled' => false, 'served' => false],
            ],
        ]);
        $this->assertCount(2, $resp->distributions);
        $this->assertInstanceOf(CloudFrontDistribution::class, $resp->distributions[0]);
        $this->assertSame('E1ABCDEF', $resp->distributions[0]->id);
        $this->assertSame('E1ABCDEF.cloudfront.net', $resp->distributions[0]->domainName);
        $this->assertTrue($resp->distributions[0]->enabled);
        $this->assertTrue($resp->distributions[0]->served);
        $this->assertFalse($resp->distributions[1]->enabled);
        $this->assertFalse($resp->distributions[1]->served);
    }

    public function testUnknownFieldsIgnored(): void
    {
        // Forward compatibility: extra fields should not cause errors
        $resp = HealthResponse::fromArray([
            'status' => 'ok',
            'version' => '1.0.0',
            'services' => [],
            'newFieldFromFuture' => 'value',
        ]);
        $this->assertSame('ok', $resp->status);
    }
}
