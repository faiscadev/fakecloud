<?php

declare(strict_types=1);

namespace FakeCloud\Tests;

use FakeCloud\BedrockFaultRule;
use FakeCloud\BedrockResponseRule;
use FakeCloud\ConfirmUserRequest;
use FakeCloud\FakeCloud;
use FakeCloud\FakeCloudError;
use PHPUnit\Framework\TestCase;

/**
 * E2E tests that require a running fakecloud server.
 *
 * The harness spawns the `fakecloud` binary itself (release build, debug
 * fallback) on an ephemeral port, waits for `/_fakecloud/health`, and tears
 * it down afterwards, mirroring the Java/Python/Go SDK harnesses. Build the
 * binary first:
 *   cargo build --release
 *
 * Then run:
 *   vendor/bin/phpunit --testsuite e2e
 *
 * Overrides:
 *   FAKECLOUD_ENDPOINT: run against an already-running server (no spawn).
 *   FAKECLOUD_BIN:      path to the fakecloud binary to spawn.
 *
 * The suite FAILS LOUD (it does not skip) when the binary is missing or the
 * server never becomes ready (a skipped E2E suite is a silent false-pass).
 */
final class E2ETest extends TestCase
{
    /** @var resource|null */
    private static $serverProcess = null;
    /** @var array<int, resource> */
    private static array $serverPipes = [];
    private static string $serverEndpoint = '';

    private FakeCloud $fc;
    private string $endpoint;

    public static function setUpBeforeClass(): void
    {
        // Run against an externally-managed server if asked.
        $external = getenv('FAKECLOUD_ENDPOINT');
        if ($external !== false && $external !== '') {
            self::$serverEndpoint = rtrim($external, '/');
            self::waitForReady(self::$serverEndpoint, 15.0);
            return;
        }

        $binary = self::locateBinary();
        $port = self::freePort();
        $endpoint = 'http://127.0.0.1:' . $port;

        $descriptors = [
            0 => ['file', '/dev/null', 'r'],
            1 => ['file', '/dev/null', 'w'],
            2 => ['file', '/dev/null', 'w'],
        ];
        $process = proc_open(
            [$binary, '--addr', '127.0.0.1:' . $port, '--log-level', 'warn'],
            $descriptors,
            self::$serverPipes
        );
        if (!is_resource($process)) {
            throw new \RuntimeException('failed to spawn fakecloud binary: ' . $binary);
        }
        self::$serverProcess = $process;

        try {
            self::waitForReady($endpoint, 30.0);
        } catch (\RuntimeException $e) {
            self::stopServer();
            throw $e;
        }
        self::$serverEndpoint = $endpoint;
    }

    public static function tearDownAfterClass(): void
    {
        self::stopServer();
        self::$serverEndpoint = '';
    }

    protected function setUp(): void
    {
        $this->endpoint = self::$serverEndpoint;
        $this->fc = new FakeCloud($this->endpoint);
        $this->fc->reset();
    }

    /** Locate the fakecloud binary: FAKECLOUD_BIN, then release, then debug. */
    private static function locateBinary(): string
    {
        $override = getenv('FAKECLOUD_BIN');
        if ($override !== false && $override !== '') {
            if (!is_file($override) || !is_executable($override)) {
                throw new \RuntimeException(
                    'FAKECLOUD_BIN is set but not an executable file: ' . $override
                );
            }
            return $override;
        }

        $repoRoot = self::locateRepoRoot();
        $candidates = [
            $repoRoot . '/target/release/fakecloud',
            $repoRoot . '/target/debug/fakecloud',
        ];
        foreach ($candidates as $bin) {
            if (is_file($bin) && is_executable($bin)) {
                return $bin;
            }
        }
        throw new \RuntimeException(
            "fakecloud binary not found. Build it first with: cargo build --release\n"
            . "  Looked for:\n    " . implode("\n    ", $candidates)
        );
    }

    /** Walk up from this file to the workspace root (Cargo.toml + crates/). */
    private static function locateRepoRoot(): string
    {
        $dir = __DIR__;
        for ($i = 0; $i < 8; $i++) {
            if (is_file($dir . '/Cargo.toml') && is_dir($dir . '/crates')) {
                return $dir;
            }
            $parent = dirname($dir);
            if ($parent === $dir) {
                break;
            }
            $dir = $parent;
        }
        throw new \RuntimeException('could not locate fakecloud repo root from ' . __DIR__);
    }

    /** Bind :0 to grab a free ephemeral port, then release it. */
    private static function freePort(): int
    {
        $sock = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($sock === false) {
            throw new \RuntimeException("could not allocate a free port: {$errstr} ({$errno})");
        }
        $name = stream_socket_get_name($sock, false);
        fclose($sock);
        $port = (int) substr((string) $name, strrpos((string) $name, ':') + 1);
        if ($port <= 0) {
            throw new \RuntimeException('could not determine a free port');
        }
        return $port;
    }

    /** Poll /_fakecloud/health until ready; fail loud on timeout. */
    private static function waitForReady(string $endpoint, float $timeout): void
    {
        $deadline = microtime(true) + $timeout;
        while (microtime(true) < $deadline) {
            $ch = curl_init($endpoint . '/_fakecloud/health');
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 2);
            curl_setopt($ch, CURLOPT_TIMEOUT, 2);
            $result = curl_exec($ch);
            $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);
            if ($result !== false && $code === 200) {
                return;
            }
            usleep(100_000);
        }
        throw new \RuntimeException(
            "fakecloud did not become ready at {$endpoint} within {$timeout}s"
        );
    }

    private static function stopServer(): void
    {
        if (is_resource(self::$serverProcess)) {
            proc_terminate(self::$serverProcess);
            proc_close(self::$serverProcess);
        }
        self::$serverProcess = null;
        foreach (self::$serverPipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        self::$serverPipes = [];
    }

    // ── Health ─────────────────────────────────────────────────────

    public function testHealthReturnsServerStatus(): void
    {
        $health = $this->fc->health();
        $this->assertSame('ok', $health->status);
        $this->assertNotEmpty($health->version);
        $this->assertNotEmpty($health->services);
    }

    // ── Reset ──────────────────────────────────────────────────────

    public function testResetClearsState(): void
    {
        // Create a queue via AWS SDK
        $this->awsSqs('CreateQueue', ['QueueName' => 'reset-test']);

        // Verify queue exists
        $queues = $this->fc->sqs()->getMessages();
        // After reset, should be empty
        $this->fc->reset();
        $queuesAfter = $this->fc->sqs()->getMessages();
        $this->assertEmpty($queuesAfter->queues);
    }

    public function testResetServiceClearsOneService(): void
    {
        $result = $this->fc->resetService('sqs');
        $this->assertSame('sqs', $result->reset);
    }

    // ── SQS ────────────────────────────────────────────────────────

    public function testSqsGetMessages(): void
    {
        // Disable SSE-SQS so the introspection endpoint surfaces the
        // plaintext body. Queues created without explicit attributes
        // get `SqsManagedSseEnabled=true` (the AWS default since
        // May 2023) and the body would be the at-rest ciphertext
        // envelope.
        $this->awsSqs('CreateQueue', [
            'QueueName' => 'php-test-queue',
            'Attribute.1.Name' => 'SqsManagedSseEnabled',
            'Attribute.1.Value' => 'false',
        ]);
        $queueUrl = $this->endpoint . '/000000000000/php-test-queue';
        $this->awsSqs('SendMessage', [
            'QueueUrl' => $queueUrl,
            'MessageBody' => 'hello from php',
        ]);

        $result = $this->fc->sqs()->getMessages();
        $this->assertNotEmpty($result->queues);
        $queue = $this->findQueue($result->queues, 'php-test-queue');
        $this->assertNotNull($queue);
        $this->assertCount(1, $queue->messages);
        $this->assertSame('hello from php', $queue->messages[0]->body);
    }

    // ── SNS ────────────────────────────────────────────────────────

    public function testSnsGetMessages(): void
    {
        $xml = $this->awsSns('CreateTopic', ['Name' => 'php-test-topic']);
        $topicArn = $this->extractXmlValue($xml, 'TopicArn');
        $this->awsSns('Publish', [
            'TopicArn' => $topicArn,
            'Message' => 'hello sns from php',
            'Subject' => 'test subject',
        ]);

        $result = $this->fc->sns()->getMessages();
        $this->assertNotEmpty($result->messages);
        $this->assertSame('hello sns from php', $result->messages[0]->message);
    }

    public function testSnsPendingConfirmations(): void
    {
        $xml = $this->awsSns('CreateTopic', ['Name' => 'php-confirm-topic']);
        $topicArn = $this->extractXmlValue($xml, 'TopicArn');
        $this->awsSns('Subscribe', [
            'TopicArn' => $topicArn,
            'Protocol' => 'https',
            'Endpoint' => 'https://example.com/php-webhook',
        ]);

        $result = $this->fc->sns()->getPendingConfirmations();
        $found = false;
        foreach ($result->pendingConfirmations as $pc) {
            if ($pc->endpoint === 'https://example.com/php-webhook') {
                $found = true;
                $this->assertSame('https', $pc->protocol);
            }
        }
        $this->assertTrue($found, 'Expected pending confirmation not found');
    }

    // ── EC2 ────────────────────────────────────────────────────────

    public function testEc2GetInstances(): void
    {
        $xml = $this->awsEc2('RunInstances', [
            'ImageId' => 'ami-12345678',
            'InstanceType' => 't3.micro',
            'MinCount' => '1',
            'MaxCount' => '1',
        ]);
        $instanceId = $this->extractXmlValue($xml, 'instanceId');
        $this->assertNotEmpty($instanceId, 'RunInstances did not return an instanceId');

        $result = $this->fc->ec2()->getInstances();
        $this->assertNotEmpty($result->instances);
        $found = null;
        foreach ($result->instances as $instance) {
            if ($instance->instanceId === $instanceId) {
                $found = $instance;
                break;
            }
        }
        $this->assertNotNull($found, 'Expected EC2 instance not found');
        $this->assertSame('ami-12345678', $found->imageId);
        $this->assertSame('t3.micro', $found->instanceType);
        $this->assertIsArray($found->securityGroupIds);
    }

    // ── SES ────────────────────────────────────────────────────────

    public function testSesGetEmails(): void
    {
        $this->awsSesV2('CreateEmailIdentity', ['EmailIdentity' => 'php@example.com']);
        $this->awsSesV2('SendEmail', [
            'FromEmailAddress' => 'php@example.com',
            'Destination' => ['ToAddresses' => ['to@example.com']],
            'Content' => [
                'Simple' => [
                    'Subject' => ['Data' => 'PHP test email'],
                    'Body' => ['Text' => ['Data' => 'Hello from PHP']],
                ],
            ],
        ]);

        $result = $this->fc->ses()->getEmails();
        $this->assertNotEmpty($result->emails);
        $found = false;
        foreach ($result->emails as $email) {
            if ($email->subject === 'PHP test email') {
                $found = true;
                $this->assertSame('php@example.com', $email->from);
            }
        }
        $this->assertTrue($found, 'Expected email not found');
    }

    // ── EventBridge ────────────────────────────────────────────────

    public function testEventsGetHistory(): void
    {
        $this->awsEventBridge('PutEvents', [
            'Entries' => [
                [
                    'Source' => 'php.test',
                    'DetailType' => 'PhpTestEvent',
                    'Detail' => '{"key":"value"}',
                ],
            ],
        ]);

        $result = $this->fc->events()->getHistory();
        $found = false;
        foreach ($result->events as $event) {
            if ($event->source === 'php.test') {
                $found = true;
                $this->assertSame('PhpTestEvent', $event->detailType);
            }
        }
        $this->assertTrue($found, 'Expected event not found');
    }

    // ── DynamoDB ───────────────────────────────────────────────────

    public function testDynamodbTickTtl(): void
    {
        $this->awsDynamoDB('CreateTable', [
            'TableName' => 'php-ttl-table',
            'KeySchema' => [['AttributeName' => 'pk', 'KeyType' => 'HASH']],
            'AttributeDefinitions' => [['AttributeName' => 'pk', 'AttributeType' => 'S']],
            'BillingMode' => 'PAY_PER_REQUEST',
        ]);
        $this->awsDynamoDB('UpdateTimeToLive', [
            'TableName' => 'php-ttl-table',
            'TimeToLiveSpecification' => ['AttributeName' => 'ttl', 'Enabled' => true],
        ]);
        $this->awsDynamoDB('PutItem', [
            'TableName' => 'php-ttl-table',
            'Item' => [
                'pk' => ['S' => 'item-1'],
                'ttl' => ['N' => '0'],
            ],
        ]);

        $result = $this->fc->dynamodb()->tickTtl();
        $this->assertGreaterThanOrEqual(1, $result->expiredItems);
    }

    // ── Bedrock ────────────────────────────────────────────────────

    public function testBedrockResponseRulesRoundTrip(): void
    {
        $modelId = 'anthropic.claude-3-haiku-20240307-v1:0';
        $set = $this->fc->bedrock()->setResponseRules($modelId, [
            new BedrockResponseRule('spam:', '{"label":"spam"}'),
            new BedrockResponseRule(null, '{"label":"ham"}'),
        ]);
        $this->assertSame('ok', $set->status);
        $this->assertSame($modelId, $set->modelId);

        $cleared = $this->fc->bedrock()->clearResponseRules($modelId);
        $this->assertSame('ok', $cleared->status);
    }

    public function testBedrockFaultsRoundTrip(): void
    {
        $queued = $this->fc->bedrock()->queueFault(
            new BedrockFaultRule('ThrottlingException', 'Rate exceeded', 429, 2, null, 'InvokeModel')
        );
        $this->assertSame('ok', $queued->status);

        $list = $this->fc->bedrock()->getFaults();
        $this->assertCount(1, $list->faults);
        $this->assertSame('ThrottlingException', $list->faults[0]->errorType);
        $this->assertSame(2, $list->faults[0]->remaining);

        $cleared = $this->fc->bedrock()->clearFaults();
        $this->assertSame('ok', $cleared->status);
        $this->assertEmpty($this->fc->bedrock()->getFaults()->faults);
    }

    public function testBedrockGetInvocations(): void
    {
        $result = $this->fc->bedrock()->getInvocations();
        $this->assertIsArray($result->invocations);
    }

    // ── S3 ─────────────────────────────────────────────────────────

    public function testS3GetNotifications(): void
    {
        $result = $this->fc->s3()->getNotifications();
        $this->assertIsArray($result->notifications);
    }

    // ── Cognito ────────────────────────────────────────────────────

    public function testCognitoConfirmUserNotFoundThrows(): void
    {
        // Create pool so we have a valid pool ID
        $poolId = $this->createCognitoPool();

        $this->expectException(FakeCloudError::class);
        $this->fc->cognito()->confirmUser(new ConfirmUserRequest($poolId, 'nobody-here'));
    }

    // ── Helpers: raw AWS API calls via curl ────────────────────────

    private function awsSqs(string $action, array $params): array
    {
        $params['Action'] = $action;
        $params['Version'] = '2012-11-05';
        $ch = curl_init($this->endpoint);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/x-www-form-urlencoded',
            'Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=dummy',
        ]);
        $result = curl_exec($ch);
        curl_close($ch);
        return json_decode($result, true) ?: [];
    }

    private function awsEc2(string $action, array $params): string
    {
        $params['Action'] = $action;
        $params['Version'] = '2016-11-15';
        $ch = curl_init($this->endpoint);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/x-www-form-urlencoded',
            'Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/ec2/aws4_request, SignedHeaders=host, Signature=dummy',
        ]);
        $result = curl_exec($ch);
        curl_close($ch);
        return $result;
    }

    private function awsSns(string $action, array $params): string
    {
        $params['Action'] = $action;
        $params['Version'] = '2010-03-31';
        $ch = curl_init($this->endpoint);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/x-www-form-urlencoded',
            'Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/sns/aws4_request, SignedHeaders=host, Signature=dummy',
        ]);
        $result = curl_exec($ch);
        curl_close($ch);
        return $result;
    }

    private function extractXmlValue(string $xml, string $tag): string
    {
        preg_match("/<{$tag}>([^<]+)<\/{$tag}>/", $xml, $matches);
        return $matches[1] ?? '';
    }

    private function awsSesV2(string $action, array $params): array
    {
        $path = match ($action) {
            'CreateEmailIdentity' => '/v2/email/identities',
            'SendEmail' => '/v2/email/outbound-emails',
            default => throw new \RuntimeException("Unknown SES v2 action: {$action}"),
        };
        $ch = curl_init($this->endpoint . $path);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($params));
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/json',
            'Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/ses/aws4_request, SignedHeaders=host, Signature=dummy',
        ]);
        $result = curl_exec($ch);
        curl_close($ch);
        return json_decode($result, true) ?: [];
    }

    private function awsEventBridge(string $action, array $params): array
    {
        $ch = curl_init($this->endpoint);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($params));
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/x-amz-json-1.1',
            'X-Amz-Target: AWSEvents.' . $action,
            'Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/events/aws4_request, SignedHeaders=host, Signature=dummy',
        ]);
        $result = curl_exec($ch);
        curl_close($ch);
        return json_decode($result, true) ?: [];
    }

    private function awsDynamoDB(string $action, array $params): array
    {
        $ch = curl_init($this->endpoint);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($params));
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/x-amz-json-1.0',
            'X-Amz-Target: DynamoDB_20120810.' . $action,
            'Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=dummy',
        ]);
        $result = curl_exec($ch);
        curl_close($ch);
        return json_decode($result, true) ?: [];
    }

    private function createCognitoPool(): string
    {
        $ch = curl_init($this->endpoint);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode(['PoolName' => 'php-test-pool']));
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            'Content-Type: application/x-amz-json-1.1',
            'X-Amz-Target: AWSCognitoIdentityProviderService.CreateUserPool',
            'Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/cognito-idp/aws4_request, SignedHeaders=host, Signature=dummy',
        ]);
        $result = curl_exec($ch);
        curl_close($ch);
        $data = json_decode($result, true);
        return $data['UserPool']['Id'];
    }

    /** @param \FakeCloud\SqsQueueMessages[] $queues */
    private function findQueue(array $queues, string $name): ?\FakeCloud\SqsQueueMessages
    {
        foreach ($queues as $q) {
            if ($q->queueName === $name) {
                return $q;
            }
        }
        return null;
    }
}
