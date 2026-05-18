<?php

declare(strict_types=1);

namespace FakeCloud;

// ── Health & Reset ─────────────────────────────────────────────

final class HealthResponse
{
    public function __construct(
        public readonly string $status,
        public readonly string $version,
        /** @var string[] */
        public readonly array $services,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['status'], $data['version'], $data['services']);
    }
}

final class ResetResponse
{
    public function __construct(
        public readonly string $status,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['status']);
    }
}

final class ResetServiceResponse
{
    public function __construct(
        public readonly string $reset,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['reset']);
    }
}

// ── RDS ────────────────────────────────────────────────────────

final class RdsTag
{
    public function __construct(
        public readonly string $key,
        public readonly string $value,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['key'], $data['value']);
    }
}

final class RdsInstance
{
    public function __construct(
        public readonly string $dbInstanceIdentifier,
        public readonly string $dbInstanceArn,
        public readonly string $dbInstanceClass,
        public readonly string $engine,
        public readonly string $engineVersion,
        public readonly string $dbInstanceStatus,
        public readonly string $masterUsername,
        public readonly ?string $dbName,
        public readonly ?string $endpointAddress,
        public readonly int $port,
        public readonly int $allocatedStorage,
        public readonly bool $publiclyAccessible,
        public readonly bool $deletionProtection,
        public readonly string $createdAt,
        public readonly string $dbiResourceId,
        public readonly ?string $containerId,
        public readonly ?int $hostPort,
        /** @var RdsTag[] */
        public readonly array $tags,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['dbInstanceIdentifier'],
            $data['dbInstanceArn'],
            $data['dbInstanceClass'],
            $data['engine'],
            $data['engineVersion'],
            $data['dbInstanceStatus'],
            $data['masterUsername'],
            $data['dbName'] ?? null,
            $data['endpointAddress'] ?? null,
            $data['port'],
            $data['allocatedStorage'],
            $data['publiclyAccessible'],
            $data['deletionProtection'],
            $data['createdAt'],
            $data['dbiResourceId'],
            $data['containerId'] ?? null,
            $data['hostPort'] ?? null,
            array_map(RdsTag::fromArray(...), $data['tags'] ?? []),
        );
    }
}

final class RdsInstancesResponse
{
    public function __construct(
        /** @var RdsInstance[] */
        public readonly array $instances,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(RdsInstance::fromArray(...), $data['instances']));
    }
}

// ── ElastiCache ────────────────────────────────────────────────

final class ElastiCacheCluster
{
    public function __construct(
        public readonly string $cacheClusterId,
        public readonly string $cacheClusterStatus,
        public readonly string $engine,
        public readonly string $engineVersion,
        public readonly string $cacheNodeType,
        public readonly int $numCacheNodes,
        public readonly ?string $replicationGroupId,
        public readonly ?int $port,
        public readonly ?int $hostPort,
        public readonly ?string $containerId,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['cacheClusterId'],
            $data['cacheClusterStatus'],
            $data['engine'],
            $data['engineVersion'],
            $data['cacheNodeType'],
            $data['numCacheNodes'],
            $data['replicationGroupId'] ?? null,
            $data['port'] ?? null,
            $data['hostPort'] ?? null,
            $data['containerId'] ?? null,
        );
    }
}

final class ElastiCacheClustersResponse
{
    public function __construct(
        /** @var ElastiCacheCluster[] */
        public readonly array $clusters,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(ElastiCacheCluster::fromArray(...), $data['clusters']));
    }
}

final class ElastiCacheReplicationGroup
{
    public function __construct(
        public readonly string $replicationGroupId,
        public readonly string $status,
        public readonly string $description,
        /** @var string[] */
        public readonly array $memberClusters,
        public readonly bool $automaticFailover,
        public readonly bool $multiAz,
        public readonly string $engine,
        public readonly string $engineVersion,
        public readonly string $cacheNodeType,
        public readonly int $numCacheClusters,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['replicationGroupId'],
            $data['status'],
            $data['description'],
            $data['memberClusters'],
            $data['automaticFailover'],
            $data['multiAz'],
            $data['engine'],
            $data['engineVersion'],
            $data['cacheNodeType'],
            $data['numCacheClusters'],
        );
    }
}

final class ElastiCacheReplicationGroupsResponse
{
    public function __construct(
        /** @var ElastiCacheReplicationGroup[] */
        public readonly array $replicationGroups,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(ElastiCacheReplicationGroup::fromArray(...), $data['replicationGroups']));
    }
}

final class ElastiCacheServerlessCache
{
    public function __construct(
        public readonly string $serverlessCacheName,
        public readonly string $status,
        public readonly string $engine,
        public readonly string $engineVersion,
        public readonly ?string $cacheNodeType,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['serverlessCacheName'],
            $data['status'],
            $data['engine'],
            $data['engineVersion'],
            $data['cacheNodeType'] ?? null,
        );
    }
}

final class ElastiCacheServerlessCachesResponse
{
    public function __construct(
        /** @var ElastiCacheServerlessCache[] */
        public readonly array $serverlessCaches,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(ElastiCacheServerlessCache::fromArray(...), $data['serverlessCaches']));
    }
}

final class ElastiCacheAclUser
{
    public function __construct(
        public readonly string $name,
        public readonly string $status,
        public readonly string $accessString,
        public readonly bool $noPasswordRequired,
        public readonly int $passwordCount,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'],
            $data['status'],
            $data['accessString'],
            $data['noPasswordRequired'],
            $data['passwordCount'],
        );
    }
}

final class ElastiCacheAclGroup
{
    public function __construct(
        public readonly string $name,
        /** @var string[] */
        public readonly array $members,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['name'], $data['members'] ?? []);
    }
}

final class ElastiCacheAclCluster
{
    public function __construct(
        public readonly string $clusterId,
        public readonly string $engine,
        /** @var ElastiCacheAclUser[] */
        public readonly array $users,
        /** @var ElastiCacheAclGroup[] */
        public readonly array $groups,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['clusterId'],
            $data['engine'],
            array_map(ElastiCacheAclUser::fromArray(...), $data['users'] ?? []),
            array_map(ElastiCacheAclGroup::fromArray(...), $data['groups'] ?? []),
        );
    }
}

final class ElastiCacheAclsResponse
{
    public function __construct(
        /** @var ElastiCacheAclCluster[] */
        public readonly array $acls,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(ElastiCacheAclCluster::fromArray(...), $data['acls'] ?? []));
    }
}

// ── Lambda ─────────────────────────────────────────────────────

final class LambdaInvocation
{
    public function __construct(
        public readonly string $functionArn,
        public readonly string $payload,
        public readonly string $source,
        public readonly string $timestamp,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['functionArn'], $data['payload'], $data['source'], $data['timestamp']);
    }
}

final class LambdaInvocationsResponse
{
    public function __construct(
        /** @var LambdaInvocation[] */
        public readonly array $invocations,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(LambdaInvocation::fromArray(...), $data['invocations']));
    }
}

final class WarmContainer
{
    public function __construct(
        public readonly string $functionName,
        public readonly string $runtime,
        public readonly string $containerId,
        public readonly int $lastUsedSecsAgo,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['functionName'], $data['runtime'], $data['containerId'], $data['lastUsedSecsAgo']);
    }
}

final class WarmContainersResponse
{
    public function __construct(
        /** @var WarmContainer[] */
        public readonly array $containers,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(WarmContainer::fromArray(...), $data['containers']));
    }
}

final class EvictContainerResponse
{
    public function __construct(
        public readonly bool $evicted,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['evicted']);
    }
}

// ── SES ────────────────────────────────────────────────────────

final class SentEmail
{
    public function __construct(
        public readonly string $messageId,
        public readonly string $from,
        /** @var string[] */
        public readonly array $to,
        /** @var string[] */
        public readonly array $cc,
        /** @var string[] */
        public readonly array $bcc,
        public readonly ?string $subject,
        public readonly ?string $htmlBody,
        public readonly ?string $textBody,
        public readonly ?string $rawData,
        public readonly ?string $templateName,
        public readonly ?string $templateData,
        public readonly ?string $dkimSignature,
        /** @var array<int, array{0: string, 1: string}> */
        public readonly array $headers,
        public readonly string $timestamp,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['messageId'],
            $data['from'],
            $data['to'] ?? [],
            $data['cc'] ?? [],
            $data['bcc'] ?? [],
            $data['subject'] ?? null,
            $data['htmlBody'] ?? null,
            $data['textBody'] ?? null,
            $data['rawData'] ?? null,
            $data['templateName'] ?? null,
            $data['templateData'] ?? null,
            $data['dkimSignature'] ?? null,
            $data['headers'] ?? [],
            $data['timestamp'],
        );
    }
}

final class SesEmailsResponse
{
    public function __construct(
        /** @var SentEmail[] */
        public readonly array $emails,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(SentEmail::fromArray(...), $data['emails']));
    }
}

final class InboundEmailRequest
{
    public function __construct(
        public readonly string $from,
        /** @var string[] */
        public readonly array $to,
        public readonly string $subject,
        public readonly string $body,
    ) {}

    public function toArray(): array
    {
        return [
            'from' => $this->from,
            'to' => $this->to,
            'subject' => $this->subject,
            'body' => $this->body,
        ];
    }
}

final class InboundActionExecuted
{
    public function __construct(
        public readonly string $rule,
        public readonly string $actionType,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['rule'], $data['actionType']);
    }
}

final class InboundEmailResponse
{
    public function __construct(
        public readonly string $messageId,
        /** @var string[] */
        public readonly array $matchedRules,
        /** @var InboundActionExecuted[] */
        public readonly array $actionsExecuted,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['messageId'],
            $data['matchedRules'],
            array_map(InboundActionExecuted::fromArray(...), $data['actionsExecuted']),
        );
    }
}

final class SesMetrics
{
    public function __construct(
        public readonly int $suppressedDropsTotal,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self((int) ($data['suppressedDropsTotal'] ?? 0));
    }
}

final class SesMailFromStatusResponse
{
    public function __construct(
        public readonly string $identity,
        public readonly string $mailFromDomainStatus,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['identity'], $data['mailFromDomainStatus']);
    }
}

final class SesDkimPublicKey
{
    public function __construct(
        public readonly string $identity,
        public readonly ?string $selector,
        public readonly ?string $publicKeyBase64,
        public readonly bool $signingEnabled,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['identity'],
            $data['selector'] ?? null,
            $data['publicKeyBase64'] ?? null,
            (bool) ($data['signingEnabled'] ?? false),
        );
    }
}

final class SesSandboxResponse
{
    public function __construct(
        public readonly bool $sandbox,
        public readonly bool $productionAccessEnabled,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (bool) $data['sandbox'],
            (bool) $data['productionAccessEnabled'],
        );
    }
}

final class SesBouncedRecipientInfo
{
    public function __construct(
        public readonly string $recipient,
        public readonly string $bounceType,
        public readonly string $action,
        public readonly string $status,
        public readonly string $diagnosticCode,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['recipient'] ?? ''),
            (string) ($data['bounceType'] ?? ''),
            (string) ($data['action'] ?? ''),
            (string) ($data['status'] ?? ''),
            (string) ($data['diagnosticCode'] ?? ''),
        );
    }
}

final class SesBounce
{
    public function __construct(
        public readonly string $messageId,
        public readonly string $bounceType,
        public readonly string $bounceSubType,
        /** @var SesBouncedRecipientInfo[] */
        public readonly array $bouncedRecipientInfo,
        public readonly ?string $explanation,
        public readonly string $timestamp,
        public readonly string $originalMessageId,
        public readonly string $bounceSender,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['messageId'] ?? ''),
            (string) ($data['bounceType'] ?? ''),
            (string) ($data['bounceSubType'] ?? ''),
            array_map(
                fn(array $i) => SesBouncedRecipientInfo::fromArray($i),
                $data['bouncedRecipientInfo'] ?? []
            ),
            $data['explanation'] ?? null,
            (string) ($data['timestamp'] ?? ''),
            (string) ($data['originalMessageId'] ?? ''),
            (string) ($data['bounceSender'] ?? ''),
        );
    }
}

final class SesBouncesResponse
{
    public function __construct(
        /** @var SesBounce[] */
        public readonly array $bounces,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            fn(array $b) => SesBounce::fromArray($b),
            $data['bounces'] ?? []
        ));
    }
}

final class SesMessageInsightEvent
{
    public function __construct(
        public readonly string $destination,
        public readonly string $timestamp,
        public readonly ?string $bounceType,
        public readonly ?string $bounceSubType,
        public readonly ?string $diagnosticCode,
        public readonly ?string $complaintFeedbackType,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['destination'] ?? ''),
            (string) ($data['timestamp'] ?? ''),
            $data['bounceType'] ?? null,
            $data['bounceSubType'] ?? null,
            $data['diagnosticCode'] ?? null,
            $data['complaintFeedbackType'] ?? null,
        );
    }
}

final class SesMessageInsightsResponse
{
    public function __construct(
        public readonly string $messageId,
        /** @var SesMessageInsightEvent[] */ public readonly array $sends,
        /** @var SesMessageInsightEvent[] */ public readonly array $deliveries,
        /** @var SesMessageInsightEvent[] */ public readonly array $opens,
        /** @var SesMessageInsightEvent[] */ public readonly array $clicks,
        /** @var SesMessageInsightEvent[] */ public readonly array $bounces,
        /** @var SesMessageInsightEvent[] */ public readonly array $complaints,
        /** @var SesMessageInsightEvent[] */ public readonly array $rejects,
    ) {}

    public static function fromArray(array $data): self
    {
        $events = fn(string $k) => array_map(
            fn(array $e) => SesMessageInsightEvent::fromArray($e),
            $data[$k] ?? []
        );
        return new self(
            (string) ($data['messageId'] ?? ''),
            $events('sends'),
            $events('deliveries'),
            $events('opens'),
            $events('clicks'),
            $events('bounces'),
            $events('complaints'),
            $events('rejects'),
        );
    }
}

final class SesSmtpSubmission
{
    public function __construct(
        public readonly string $messageId,
        public readonly string $from,
        /** @var string[] */ public readonly array $to,
        public readonly ?string $subject,
        public readonly int $rawSizeBytes,
        public readonly string $receivedAt,
        public readonly string $authUser,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['messageId'] ?? ''),
            (string) ($data['from'] ?? ''),
            $data['to'] ?? [],
            $data['subject'] ?? null,
            (int) ($data['rawSizeBytes'] ?? 0),
            (string) ($data['receivedAt'] ?? ''),
            (string) ($data['authUser'] ?? ''),
        );
    }
}

final class SesSmtpSubmissionsResponse
{
    public function __construct(
        /** @var SesSmtpSubmission[] */
        public readonly array $submissions,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            fn(array $s) => SesSmtpSubmission::fromArray($s),
            $data['submissions'] ?? []
        ));
    }
}

final class SesEventDestinationDelivery
{
    public function __construct(
        public readonly string $destinationName,
        public readonly string $destinationType,
        public readonly string $eventType,
        public readonly string $messageId,
        public readonly string $dispatchedAt,
        public readonly string $targetArn,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['destinationName'] ?? ''),
            (string) ($data['destinationType'] ?? ''),
            (string) ($data['eventType'] ?? ''),
            (string) ($data['messageId'] ?? ''),
            (string) ($data['dispatchedAt'] ?? ''),
            (string) ($data['targetArn'] ?? ''),
        );
    }
}

final class SesEventDestinationDeliveriesResponse
{
    public function __construct(
        /** @var SesEventDestinationDelivery[] */
        public readonly array $deliveries,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            fn(array $d) => SesEventDestinationDelivery::fromArray($d),
            $data['deliveries'] ?? []
        ));
    }
}

// ── SNS ────────────────────────────────────────────────────────

final class SnsMessage
{
    public function __construct(
        public readonly string $messageId,
        public readonly string $topicArn,
        public readonly string $message,
        public readonly ?string $subject,
        public readonly string $timestamp,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['messageId'], $data['topicArn'], $data['message'], $data['subject'] ?? null, $data['timestamp']);
    }
}

final class SnsMessagesResponse
{
    public function __construct(
        /** @var SnsMessage[] */
        public readonly array $messages,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(SnsMessage::fromArray(...), $data['messages']));
    }
}

final class PendingConfirmation
{
    public function __construct(
        public readonly string $subscriptionArn,
        public readonly string $topicArn,
        public readonly string $protocol,
        public readonly string $endpoint,
        public readonly string $token,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['subscriptionArn'], $data['topicArn'], $data['protocol'], $data['endpoint'], $data['token']);
    }
}

final class PendingConfirmationsResponse
{
    public function __construct(
        /** @var PendingConfirmation[] */
        public readonly array $pendingConfirmations,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(PendingConfirmation::fromArray(...), $data['pendingConfirmations']));
    }
}

final class ConfirmSubscriptionRequest
{
    public function __construct(
        public readonly string $subscriptionArn,
    ) {}

    public function toArray(): array
    {
        return ['subscriptionArn' => $this->subscriptionArn];
    }
}

final class ConfirmSubscriptionResponse
{
    public function __construct(
        public readonly bool $confirmed,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['confirmed']);
    }
}

// ── SQS ────────────────────────────────────────────────────────

final class SqsMessageInfo
{
    public function __construct(
        public readonly string $messageId,
        public readonly string $body,
        public readonly int $receiveCount,
        public readonly bool $inFlight,
        public readonly string $createdAt,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['messageId'], $data['body'], $data['receiveCount'], $data['inFlight'], $data['createdAt']);
    }
}

final class SqsQueueMessages
{
    public function __construct(
        public readonly string $queueUrl,
        public readonly string $queueName,
        /** @var SqsMessageInfo[] */
        public readonly array $messages,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['queueUrl'],
            $data['queueName'],
            array_map(SqsMessageInfo::fromArray(...), $data['messages']),
        );
    }
}

final class SqsMessagesResponse
{
    public function __construct(
        /** @var SqsQueueMessages[] */
        public readonly array $queues,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(SqsQueueMessages::fromArray(...), $data['queues']));
    }
}

final class ExpirationTickResponse
{
    public function __construct(
        public readonly int $expiredMessages,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['expiredMessages']);
    }
}

final class ForceDlqResponse
{
    public function __construct(
        public readonly int $movedMessages,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['movedMessages']);
    }
}

final class AppAsTickResponse
{
    public function __construct(
        public readonly int $applied,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['applied']);
    }
}

final class AppAsScheduledTickResponse
{
    public function __construct(
        public readonly int $fired,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['fired']);
    }
}

// ── EventBridge ────────────────────────────────────────────────

final class EventBridgeEvent
{
    public function __construct(
        public readonly string $eventId,
        public readonly string $source,
        public readonly string $detailType,
        public readonly string $detail,
        public readonly string $busName,
        public readonly string $timestamp,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['eventId'], $data['source'], $data['detailType'], $data['detail'], $data['busName'], $data['timestamp']);
    }
}

final class EventBridgeLambdaDelivery
{
    public function __construct(
        public readonly string $functionArn,
        public readonly string $payload,
        public readonly string $timestamp,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['functionArn'], $data['payload'], $data['timestamp']);
    }
}

final class EventBridgeLogDelivery
{
    public function __construct(
        public readonly string $logGroupArn,
        public readonly string $payload,
        public readonly string $timestamp,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['logGroupArn'], $data['payload'], $data['timestamp']);
    }
}

final class EventBridgeDeliveries
{
    public function __construct(
        /** @var EventBridgeLambdaDelivery[] */
        public readonly array $lambda,
        /** @var EventBridgeLogDelivery[] */
        public readonly array $logs,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            array_map(EventBridgeLambdaDelivery::fromArray(...), $data['lambda'] ?? []),
            array_map(EventBridgeLogDelivery::fromArray(...), $data['logs'] ?? []),
        );
    }
}

final class EventHistoryResponse
{
    public function __construct(
        /** @var EventBridgeEvent[] */
        public readonly array $events,
        public readonly EventBridgeDeliveries $deliveries,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            array_map(EventBridgeEvent::fromArray(...), $data['events']),
            EventBridgeDeliveries::fromArray($data['deliveries']),
        );
    }
}

final class FireRuleRequest
{
    public function __construct(
        public readonly string $ruleName,
        public readonly ?string $busName = null,
    ) {}

    public function toArray(): array
    {
        $data = ['ruleName' => $this->ruleName];
        if ($this->busName !== null) {
            $data['busName'] = $this->busName;
        }
        return $data;
    }
}

final class FireRuleTarget
{
    public function __construct(
        public readonly string $type,
        public readonly string $arn,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['type'], $data['arn']);
    }
}

final class FireRuleResponse
{
    public function __construct(
        /** @var FireRuleTarget[] */
        public readonly array $targets,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(FireRuleTarget::fromArray(...), $data['targets']));
    }
}

// ── Glue ────────────────────────────────────────────────────────

final class GlueJob
{
    public function __construct(
        public readonly string $accountId,
        public readonly string $name,
        public readonly string $role,
        public readonly mixed $command,
        /** @var array<string,string> */
        public readonly array $defaultArguments,
        public readonly ?float $maxCapacity,
        public readonly int $maxRetries,
        public readonly ?int $timeout,
        public readonly ?string $glueVersion,
        public readonly ?string $workerType,
        public readonly ?int $numberOfWorkers,
        public readonly string $createdOn,
        public readonly string $lastModifiedOn,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['accountId'] ?? '',
            $data['name'] ?? '',
            $data['role'] ?? '',
            $data['command'] ?? null,
            $data['defaultArguments'] ?? [],
            isset($data['maxCapacity']) ? (float) $data['maxCapacity'] : null,
            (int) ($data['maxRetries'] ?? 0),
            isset($data['timeout']) ? (int) $data['timeout'] : null,
            $data['glueVersion'] ?? null,
            $data['workerType'] ?? null,
            isset($data['numberOfWorkers']) ? (int) $data['numberOfWorkers'] : null,
            $data['createdOn'] ?? '',
            $data['lastModifiedOn'] ?? '',
        );
    }
}

final class GlueJobsResponse
{
    public function __construct(
        /** @var GlueJob[] */
        public readonly array $jobs,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            GlueJob::fromArray(...),
            $data['jobs'] ?? [],
        ));
    }
}

final class GlueJobRun
{
    public function __construct(
        public readonly string $accountId,
        public readonly string $id,
        public readonly string $jobName,
        public readonly int $attempt,
        public readonly string $startedOn,
        public readonly ?string $completedOn,
        public readonly string $jobRunState,
        /** @var array<string,string> */
        public readonly array $arguments,
        public readonly ?string $errorMessage,
        public readonly int $executionTime,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['accountId'] ?? '',
            $data['id'] ?? '',
            $data['jobName'] ?? '',
            (int) ($data['attempt'] ?? 0),
            $data['startedOn'] ?? '',
            $data['completedOn'] ?? null,
            $data['jobRunState'] ?? '',
            $data['arguments'] ?? [],
            $data['errorMessage'] ?? null,
            (int) ($data['executionTime'] ?? 0),
        );
    }
}

final class GlueJobRunsResponse
{
    public function __construct(
        /** @var GlueJobRun[] */
        public readonly array $runs,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            GlueJobRun::fromArray(...),
            $data['runs'] ?? [],
        ));
    }
}

// ── Scheduler (EventBridge Scheduler) ───────────────────────────

final class SchedulerSchedule
{
    public function __construct(
        public readonly string $accountId,
        public readonly string $groupName,
        public readonly string $name,
        public readonly string $arn,
        public readonly string $state,
        public readonly string $scheduleExpression,
        public readonly string $targetArn,
        public readonly ?string $lastFired,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['accountId'] ?? '',
            $data['groupName'] ?? '',
            $data['name'] ?? '',
            $data['arn'] ?? '',
            $data['state'] ?? '',
            $data['scheduleExpression'] ?? '',
            $data['targetArn'] ?? '',
            $data['lastFired'] ?? null,
        );
    }
}

final class SchedulerSchedulesResponse
{
    public function __construct(
        /** @var SchedulerSchedule[] */
        public readonly array $schedules,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            SchedulerSchedule::fromArray(...),
            $data['schedules'] ?? [],
        ));
    }
}

final class FireScheduleResponse
{
    public function __construct(
        public readonly string $scheduleArn,
        public readonly string $targetArn,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['scheduleArn'] ?? '',
            $data['targetArn'] ?? '',
        );
    }
}

// ── S3 ─────────────────────────────────────────────────────────

final class S3Notification
{
    public function __construct(
        public readonly string $bucket,
        public readonly string $key,
        public readonly string $eventType,
        public readonly string $timestamp,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['bucket'], $data['key'], $data['eventType'], $data['timestamp']);
    }
}

final class S3NotificationsResponse
{
    public function __construct(
        /** @var S3Notification[] */
        public readonly array $notifications,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(S3Notification::fromArray(...), $data['notifications']));
    }
}

final class S3AccessPointEntry
{
    public function __construct(
        public readonly string $name,
        public readonly string $alias,
        public readonly string $bucket,
        public readonly string $accountId,
        public readonly string $networkOrigin,
        public readonly string $createdAt,
        public readonly ?string $vpcConfiguration = null,
        public readonly ?string $publicAccessBlock = null,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'],
            $data['alias'],
            $data['bucket'],
            $data['accountId'],
            $data['networkOrigin'],
            $data['createdAt'],
            $data['vpcConfiguration'] ?? null,
            $data['publicAccessBlock'] ?? null,
        );
    }
}

final class S3AccessPointsResponse
{
    public function __construct(
        /** @var S3AccessPointEntry[] */
        public readonly array $accessPoints,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            S3AccessPointEntry::fromArray(...),
            $data['accessPoints'] ?? []
        ));
    }
}

final class S3ObjectLambdaResponse
{
    public function __construct(
        public readonly string $requestToken,
        public readonly string $requestRoute,
        public readonly string $bodyBase64,
        public readonly int $bodySize,
        /** @var array<string,string> */
        public readonly array $metadata,
        public readonly ?int $statusCode = null,
        public readonly ?string $contentType = null,
        public readonly ?string $errorMessage = null,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['requestToken'],
            $data['requestRoute'],
            $data['bodyBase64'],
            (int) ($data['bodySize'] ?? 0),
            $data['metadata'] ?? [],
            isset($data['statusCode']) ? (int) $data['statusCode'] : null,
            $data['contentType'] ?? null,
            $data['errorMessage'] ?? null,
        );
    }
}

final class S3ObjectLambdaResponsesResponse
{
    public function __construct(
        /** @var S3ObjectLambdaResponse[] */
        public readonly array $responses,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(
            S3ObjectLambdaResponse::fromArray(...),
            $data['responses'] ?? []
        ));
    }
}

final class LifecycleTickResponse
{
    public function __construct(
        public readonly int $processedBuckets,
        public readonly int $expiredObjects,
        public readonly int $transitionedObjects,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['processedBuckets'], $data['expiredObjects'], $data['transitionedObjects']);
    }
}

// ── DynamoDB ───────────────────────────────────────────────────

final class TtlTickResponse
{
    public function __construct(
        public readonly int $expiredItems,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['expiredItems']);
    }
}

// ── SecretsManager ─────────────────────────────────────────────

final class RotationTickResponse
{
    public function __construct(
        /** @var string[] */
        public readonly array $rotatedSecrets,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['rotatedSecrets']);
    }
}

// ── Cognito ────────────────────────────────────────────────────

final class UserConfirmationCodes
{
    public function __construct(
        public readonly ?string $confirmationCode,
        /** @var array<string, mixed> */
        public readonly array $attributeVerificationCodes,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['confirmationCode'] ?? null, $data['attributeVerificationCodes'] ?? []);
    }
}

final class ConfirmationCode
{
    public function __construct(
        public readonly string $poolId,
        public readonly string $username,
        public readonly string $code,
        public readonly string $type,
        public readonly ?string $attribute,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['poolId'], $data['username'], $data['code'], $data['type'], $data['attribute'] ?? null);
    }
}

final class ConfirmationCodesResponse
{
    public function __construct(
        /** @var ConfirmationCode[] */
        public readonly array $codes,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(ConfirmationCode::fromArray(...), $data['codes']));
    }
}

final class ConfirmUserRequest
{
    public function __construct(
        public readonly string $userPoolId,
        public readonly string $username,
    ) {}

    public function toArray(): array
    {
        return ['userPoolId' => $this->userPoolId, 'username' => $this->username];
    }
}

final class ConfirmUserResponse
{
    public function __construct(
        public readonly bool $confirmed,
        public readonly ?string $error,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['confirmed'], $data['error'] ?? null);
    }
}

final class TokenInfo
{
    public function __construct(
        public readonly string $type,
        public readonly string $username,
        public readonly string $poolId,
        public readonly string $clientId,
        public readonly int $issuedAt,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['type'], $data['username'], $data['poolId'], $data['clientId'], $data['issuedAt']);
    }
}

final class TokensResponse
{
    public function __construct(
        /** @var TokenInfo[] */
        public readonly array $tokens,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(TokenInfo::fromArray(...), $data['tokens']));
    }
}

final class ExpireTokensRequest
{
    public function __construct(
        public readonly ?string $userPoolId = null,
        public readonly ?string $username = null,
    ) {}

    public function toArray(): array
    {
        $data = [];
        if ($this->userPoolId !== null) {
            $data['userPoolId'] = $this->userPoolId;
        }
        if ($this->username !== null) {
            $data['username'] = $this->username;
        }
        return $data;
    }
}

final class ExpireTokensResponse
{
    public function __construct(
        public readonly int $expiredTokens,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['expiredTokens']);
    }
}

final class AuthEvent
{
    public function __construct(
        public readonly string $eventType,
        public readonly string $username,
        public readonly string $userPoolId,
        public readonly string $clientId,
        public readonly int $timestamp,
        public readonly bool $success,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['eventType'], $data['username'], $data['userPoolId'], $data['clientId'], $data['timestamp'], $data['success']);
    }
}

final class AuthEventsResponse
{
    public function __construct(
        /** @var AuthEvent[] */
        public readonly array $events,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(AuthEvent::fromArray(...), $data['events']));
    }
}

/**
 * One PreTokenGeneration Lambda trigger invocation captured by
 * `InitiateAuth`. `claimsAdded` / `claimsOverridden` / `groupOverrides`
 * are pre-parsed from the Lambda response.
 */
final class PreTokenGenInvocation
{
    public function __construct(
        public readonly string $poolId,
        public readonly string $userPoolArn,
        public readonly string $username,
        public readonly string $triggerSource,
        public readonly string $lambdaArn,
        /** @var array<string, mixed> */
        public readonly array $requestPayload,
        /** @var array<string, mixed>|null */
        public readonly ?array $responsePayload,
        /** @var string[] */
        public readonly array $claimsAdded,
        /** @var string[] */
        public readonly array $claimsOverridden,
        /** @var string[] */
        public readonly array $groupOverrides,
        public readonly string $invokedAt,
        public readonly int $durationMs,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['poolId'],
            $data['userPoolArn'],
            $data['username'],
            $data['triggerSource'],
            $data['lambdaArn'],
            $data['requestPayload'] ?? [],
            $data['responsePayload'] ?? null,
            $data['claimsAdded'] ?? [],
            $data['claimsOverridden'] ?? [],
            $data['groupOverrides'] ?? [],
            $data['invokedAt'],
            (int) $data['durationMs'],
        );
    }
}

final class PreTokenGenInvocationsResponse
{
    public function __construct(
        /** @var PreTokenGenInvocation[] */
        public readonly array $invocations,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            array_map(PreTokenGenInvocation::fromArray(...), $data['invocations'] ?? []),
        );
    }
}

/**
 * Payload for `POST /_fakecloud/cognito/authorization-codes`. Lets
 * test harnesses pre-allocate a single-use OAuth2 authorization code
 * that the `/oauth2/token` `authorization_code` grant later consumes.
 */
final class MintAuthorizationCodeRequest
{
    public function __construct(
        public readonly string $userPoolId,
        public readonly string $clientId,
        public readonly string $username,
        public readonly string $redirectUri,
        /** @var string[] */
        public readonly array $scopes = [],
        public readonly ?string $codeChallenge = null,
        public readonly ?string $codeChallengeMethod = null,
        public readonly ?string $nonce = null,
    ) {}

    public function toArray(): array
    {
        $out = [
            'userPoolId' => $this->userPoolId,
            'clientId' => $this->clientId,
            'username' => $this->username,
            'redirectUri' => $this->redirectUri,
            'scopes' => $this->scopes,
        ];
        if ($this->codeChallenge !== null) {
            $out['codeChallenge'] = $this->codeChallenge;
        }
        if ($this->codeChallengeMethod !== null) {
            $out['codeChallengeMethod'] = $this->codeChallengeMethod;
        }
        if ($this->nonce !== null) {
            $out['nonce'] = $this->nonce;
        }
        return $out;
    }
}

final class MintAuthorizationCodeResponse
{
    public function __construct(public readonly string $code) {}

    public static function fromArray(array $data): self
    {
        return new self($data['code']);
    }
}

/**
 * Payload for `POST /_fakecloud/cognito/compromised-passwords`. Each
 * plaintext is SHA-256 hashed server-side and added to the per-account
 * compromised-password set; subsequent `SignUp` / `AdminInitiateAuth`
 * fail with `InvalidPasswordException` on any pool whose
 * `CompromisedCredentialsRiskConfiguration.Actions.EventAction` is
 * `BLOCK`.
 */
final class CompromisedPasswordsRequest
{
    public function __construct(
        /** @var string[] */
        public readonly array $passwords,
    ) {}

    public function toArray(): array
    {
        return ['passwords' => $this->passwords];
    }
}

final class CompromisedPasswordsResponse
{
    public function __construct(public readonly int $added) {}

    public static function fromArray(array $data): self
    {
        return new self((int) $data['added']);
    }
}

/**
 * Registered WebAuthn credential surfaced by
 * `GET /_fakecloud/cognito/webauthn-credentials`. `$attestationInfo`
 * is the parsed-attestation associative array (packed format details,
 * AAGUID, certificate chain summary, signature counter); its shape
 * depends on the attestation format so it stays untyped.
 */
final class WebAuthnCredential
{
    public function __construct(
        public readonly string $accountId,
        public readonly string $poolUser,
        public readonly string $credentialId,
        public readonly string $relyingPartyId,
        public readonly mixed $attestationInfo,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['account_id'],
            $data['pool_user'],
            $data['credential_id'],
            $data['relying_party_id'],
            $data['attestation_info'] ?? null,
        );
    }
}

final class WebAuthnCredentialsResponse
{
    public function __construct(
        /** @var WebAuthnCredential[] */
        public readonly array $credentials,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(WebAuthnCredential::fromArray(...), $data['credentials'] ?? []));
    }
}

// ── Step Functions ─────────────────────────────────────────────

final class StepFunctionsExecution
{
    public function __construct(
        public readonly string $executionArn,
        public readonly string $stateMachineArn,
        public readonly string $name,
        public readonly string $status,
        public readonly string $startDate,
        public readonly ?string $input,
        public readonly ?string $output,
        public readonly ?string $stopDate,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['executionArn'],
            $data['stateMachineArn'],
            $data['name'],
            $data['status'],
            $data['startDate'],
            $data['input'] ?? null,
            $data['output'] ?? null,
            $data['stopDate'] ?? null,
        );
    }
}

final class StepFunctionsExecutionsResponse
{
    public function __construct(
        /** @var StepFunctionsExecution[] */
        public readonly array $executions,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(StepFunctionsExecution::fromArray(...), $data['executions']));
    }
}

final class StepFunctionsSyncBillingDetails
{
    public function __construct(
        public readonly int $billedDurationInMilliseconds,
        public readonly int $billedMemoryUsedInMb,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (int) $data['billedDurationInMilliseconds'],
            (int) $data['billedMemoryUsedInMb'],
        );
    }
}

final class StepFunctionsSyncExecution
{
    public function __construct(
        public readonly string $executionArn,
        public readonly string $stateMachineArn,
        public readonly string $name,
        public readonly string $status,
        public readonly ?string $input,
        public readonly ?string $output,
        public readonly string $startedAt,
        public readonly ?string $stoppedAt,
        public readonly int $durationMs,
        public readonly StepFunctionsSyncBillingDetails $billingDetails,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['executionArn'],
            $data['stateMachineArn'],
            $data['name'],
            $data['status'],
            $data['input'] ?? null,
            $data['output'] ?? null,
            $data['startedAt'],
            $data['stoppedAt'] ?? null,
            (int) $data['durationMs'],
            StepFunctionsSyncBillingDetails::fromArray($data['billingDetails']),
        );
    }
}

final class StepFunctionsSyncExecutionsResponse
{
    public function __construct(
        /** @var StepFunctionsSyncExecution[] */
        public readonly array $executions,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(StepFunctionsSyncExecution::fromArray(...), $data['executions']));
    }
}

final class StepFunctionsExecutionTreeNode
{
    public function __construct(
        public readonly string $arn,
        public readonly string $stateMachineArn,
        public readonly string $status,
        public readonly string $startedAt,
        public readonly ?string $stoppedAt,
        /** @var StepFunctionsExecutionTreeNode[] */
        public readonly array $children,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['arn'],
            $data['stateMachineArn'],
            $data['status'],
            $data['startedAt'],
            $data['stoppedAt'] ?? null,
            array_map(self::fromArray(...), $data['children'] ?? []),
        );
    }
}

final class StepFunctionsExecutionTreeResponse
{
    public function __construct(
        public readonly string $rootArn,
        public readonly StepFunctionsExecutionTreeNode $tree,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['rootArn'],
            StepFunctionsExecutionTreeNode::fromArray($data['tree']),
        );
    }
}

final class SfnEnqueueActivityTaskRequest
{
    public function __construct(
        public readonly string $activityArn,
        public readonly ?string $input = null,
        public readonly ?int $heartbeatSeconds = null,
        public readonly ?int $timeoutSeconds = null,
    ) {}

    public function toArray(): array
    {
        $out = ['activityArn' => $this->activityArn];
        if ($this->input !== null) {
            $out['input'] = $this->input;
        }
        if ($this->heartbeatSeconds !== null) {
            $out['heartbeatSeconds'] = $this->heartbeatSeconds;
        }
        if ($this->timeoutSeconds !== null) {
            $out['timeoutSeconds'] = $this->timeoutSeconds;
        }
        return $out;
    }
}

final class SfnEnqueueActivityTaskResponse
{
    public function __construct(
        public readonly string $taskToken,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['taskToken']);
    }
}

// ── Bedrock ────────────────────────────────────────────────────

final class BedrockInvocation
{
    public function __construct(
        public readonly string $modelId,
        public readonly string $input,
        public readonly ?string $output,
        public readonly string $timestamp,
        public readonly ?string $error,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['modelId'], $data['input'], $data['output'] ?? null, $data['timestamp'], $data['error'] ?? null);
    }
}

final class BedrockInvocationsResponse
{
    public function __construct(
        /** @var BedrockInvocation[] */
        public readonly array $invocations,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(BedrockInvocation::fromArray(...), $data['invocations']));
    }
}

final class BedrockModelResponseConfig
{
    public function __construct(
        public readonly string $status,
        public readonly string $modelId,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['status'], $data['modelId']);
    }
}

final class BedrockResponseRule
{
    public function __construct(
        public readonly ?string $promptContains,
        public readonly string $response,
    ) {}

    public function toArray(): array
    {
        $data = ['response' => $this->response];
        if ($this->promptContains !== null) {
            $data['promptContains'] = $this->promptContains;
        }
        return $data;
    }
}

final class BedrockFaultRule
{
    public function __construct(
        public readonly string $errorType,
        public readonly ?string $message = null,
        public readonly ?int $httpStatus = null,
        public readonly ?int $count = null,
        public readonly ?string $modelId = null,
        public readonly ?string $operation = null,
    ) {}

    public function toArray(): array
    {
        $data = ['errorType' => $this->errorType];
        if ($this->message !== null) {
            $data['message'] = $this->message;
        }
        if ($this->httpStatus !== null) {
            $data['httpStatus'] = $this->httpStatus;
        }
        if ($this->count !== null) {
            $data['count'] = $this->count;
        }
        if ($this->modelId !== null) {
            $data['modelId'] = $this->modelId;
        }
        if ($this->operation !== null) {
            $data['operation'] = $this->operation;
        }
        return $data;
    }
}

final class BedrockFaultRuleState
{
    public function __construct(
        public readonly string $errorType,
        public readonly ?string $message,
        public readonly int $httpStatus,
        public readonly int $remaining,
        public readonly ?string $modelId,
        public readonly ?string $operation,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['errorType'],
            $data['message'] ?? null,
            $data['httpStatus'],
            $data['remaining'],
            $data['modelId'] ?? null,
            $data['operation'] ?? null,
        );
    }
}

final class BedrockFaultsResponse
{
    public function __construct(
        /** @var BedrockFaultRuleState[] */
        public readonly array $faults,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(BedrockFaultRuleState::fromArray(...), $data['faults']));
    }
}

final class BedrockStatusResponse
{
    public function __construct(
        public readonly string $status,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['status']);
    }
}

// ── Bedrock Agent (control plane) ──────────────────────────────

final class BedrockAgentAliasSummary
{
    public function __construct(
        public readonly string $aliasId,
        public readonly string $aliasName,
        public readonly string $agentVersion,
        public readonly string $aliasArn,
        public readonly string $status,
        public readonly string $createdAt,
        public readonly string $updatedAt,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['aliasId'] ?? '',
            $data['aliasName'] ?? '',
            $data['agentVersion'] ?? '',
            $data['aliasArn'] ?? '',
            $data['status'] ?? '',
            $data['createdAt'] ?? '',
            $data['updatedAt'] ?? '',
        );
    }
}

final class BedrockAgentVersionSummary
{
    public function __construct(
        public readonly string $agentVersion,
        public readonly string $createdAt,
        public readonly ?string $instruction,
        public readonly ?string $foundationModel,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['agentVersion'] ?? '',
            $data['createdAt'] ?? '',
            $data['instruction'] ?? null,
            $data['foundationModel'] ?? null,
        );
    }
}

final class BedrockAgentKnowledgeBaseSummary
{
    public function __construct(
        public readonly string $knowledgeBaseId,
        public readonly string $state,
        public readonly ?string $description,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['knowledgeBaseId'] ?? '',
            $data['state'] ?? '',
            $data['description'] ?? null,
        );
    }
}

final class BedrockAgentCollaboratorSummary
{
    public function __construct(
        public readonly string $collaboratorId,
        public readonly string $collaboratorName,
        public readonly string $collaboratorAliasArn,
        public readonly string $relayConversationHistory,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['collaboratorId'] ?? '',
            $data['collaboratorName'] ?? '',
            $data['collaboratorAliasArn'] ?? '',
            $data['relayConversationHistory'] ?? '',
        );
    }
}

final class BedrockAgentRow
{
    public function __construct(
        public readonly string $agentId,
        public readonly string $agentName,
        public readonly string $agentArn,
        public readonly string $agentStatus,
        public readonly ?string $foundationModel,
        public readonly ?string $instruction,
        /** @var BedrockAgentKnowledgeBaseSummary[] */
        public readonly array $knowledgeBases,
        /** @var array<int, mixed> */
        public readonly array $actionGroups,
        /** @var BedrockAgentCollaboratorSummary[] */
        public readonly array $collaborators,
        /** @var BedrockAgentAliasSummary[] */
        public readonly array $aliases,
        /** @var BedrockAgentVersionSummary[] */
        public readonly array $versions,
        public readonly mixed $promptOverrides,
        public readonly string $createdAt,
        public readonly string $updatedAt,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['agentId'] ?? '',
            $data['agentName'] ?? '',
            $data['agentArn'] ?? '',
            $data['agentStatus'] ?? '',
            $data['foundationModel'] ?? null,
            $data['instruction'] ?? null,
            array_map(BedrockAgentKnowledgeBaseSummary::fromArray(...), $data['knowledgeBases'] ?? []),
            $data['actionGroups'] ?? [],
            array_map(BedrockAgentCollaboratorSummary::fromArray(...), $data['collaborators'] ?? []),
            array_map(BedrockAgentAliasSummary::fromArray(...), $data['aliases'] ?? []),
            array_map(BedrockAgentVersionSummary::fromArray(...), $data['versions'] ?? []),
            $data['promptOverrides'] ?? null,
            $data['createdAt'] ?? '',
            $data['updatedAt'] ?? '',
        );
    }
}

final class BedrockAgentAgentsResponse
{
    public function __construct(
        /** @var BedrockAgentRow[] */
        public readonly array $agents,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(BedrockAgentRow::fromArray(...), $data['agents'] ?? []));
    }
}

// ── Bedrock Agent Runtime (data plane) ────────────────────────

final class BedrockAgentRuntimeInvocation
{
    public function __construct(
        public readonly string $invocationId,
        public readonly string $op,
        public readonly ?string $agentId,
        public readonly ?string $flowId,
        public readonly ?string $sessionId,
        public readonly string $input,
        public readonly string $output,
        public readonly int $outputChunks,
        public readonly mixed $trace,
        /** @var array<int, mixed> */
        public readonly array $citations,
        public readonly string $invokedAt,
        public readonly int $durationMs,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['invocationId'] ?? '',
            $data['op'] ?? '',
            $data['agentId'] ?? null,
            $data['flowId'] ?? null,
            $data['sessionId'] ?? null,
            $data['input'] ?? '',
            $data['output'] ?? '',
            (int) ($data['outputChunks'] ?? 0),
            $data['trace'] ?? null,
            $data['citations'] ?? [],
            $data['invokedAt'] ?? '',
            (int) ($data['durationMs'] ?? 0),
        );
    }
}

final class BedrockAgentRuntimeInvocationsResponse
{
    public function __construct(
        /** @var BedrockAgentRuntimeInvocation[] */
        public readonly array $invocations,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(BedrockAgentRuntimeInvocation::fromArray(...), $data['invocations'] ?? []));
    }
}

// ── API Gateway v2 ─────────────────────────────────────────────

final class ApiGatewayV2Request
{
    public function __construct(
        public readonly string $requestId,
        public readonly string $apiId,
        public readonly string $stage,
        public readonly string $method,
        public readonly string $path,
        /** @var array<string, string> */
        public readonly array $headers,
        /** @var array<string, string> */
        public readonly array $queryParams,
        public readonly ?string $body,
        public readonly string $timestamp,
        public readonly int $statusCode,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['requestId'],
            $data['apiId'],
            $data['stage'],
            $data['method'],
            $data['path'],
            $data['headers'] ?? [],
            $data['queryParams'] ?? [],
            $data['body'] ?? null,
            $data['timestamp'],
            $data['statusCode'],
        );
    }
}

final class ApiGatewayV2RequestsResponse
{
    public function __construct(
        /** @var ApiGatewayV2Request[] */
        public readonly array $requests,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(ApiGatewayV2Request::fromArray(...), $data['requests']));
    }
}

// ── IAM ───────────────────────────────────────────────────────

final class CreateAdminResponse
{
    public function __construct(
        public readonly string $accessKeyId,
        public readonly string $secretAccessKey,
        public readonly string $accountId,
        public readonly string $arn,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['accessKeyId'],
            $data['secretAccessKey'],
            $data['accountId'],
            $data['arn'],
        );
    }
}

// ── ECR ────────────────────────────────────────────────────────────

final class EcrTag
{
    public function __construct(
        public readonly string $key,
        public readonly string $value,
    ) {}

    public static function fromArray(array $d): self
    {
        return new self($d['key'] ?? '', $d['value'] ?? '');
    }
}

final class EcrRepository
{
    /** @param EcrTag[] $tags */
    public function __construct(
        public readonly string $repositoryName,
        public readonly string $repositoryArn,
        public readonly string $registryId,
        public readonly string $repositoryUri,
        public readonly string $imageTagMutability,
        public readonly bool $scanOnPush,
        public readonly string $createdAt,
        public readonly array $tags,
        public readonly bool $hasPolicy,
        public readonly bool $hasLifecyclePolicy,
        public readonly int $imageCount,
        public readonly int $layerCount,
    ) {}

    public static function fromArray(array $d): self
    {
        return new self(
            $d['repositoryName'],
            $d['repositoryArn'],
            $d['registryId'],
            $d['repositoryUri'],
            $d['imageTagMutability'],
            $d['scanOnPush'] ?? false,
            $d['createdAt'],
            array_map(static fn($t) => EcrTag::fromArray($t), $d['tags'] ?? []),
            $d['hasPolicy'] ?? false,
            $d['hasLifecyclePolicy'] ?? false,
            $d['imageCount'] ?? 0,
            $d['layerCount'] ?? 0,
        );
    }
}

final class EcrRepositoriesResponse
{
    /** @param EcrRepository[] $repositories */
    public function __construct(public readonly array $repositories) {}

    public static function fromArray(array $d): self
    {
        return new self(
            array_map(static fn($r) => EcrRepository::fromArray($r), $d['repositories'] ?? []),
        );
    }
}

final class EcrImage
{
    /** @param string[] $imageTags */
    public function __construct(
        public readonly string $repositoryName,
        public readonly string $imageDigest,
        public readonly array $imageTags,
        public readonly int $imageSizeInBytes,
        public readonly string $imageManifestMediaType,
        public readonly string $imagePushedAt,
    ) {}

    public static function fromArray(array $d): self
    {
        return new self(
            $d['repositoryName'],
            $d['imageDigest'],
            $d['imageTags'] ?? [],
            $d['imageSizeInBytes'] ?? 0,
            $d['imageManifestMediaType'] ?? '',
            $d['imagePushedAt'] ?? '',
        );
    }
}

final class EcrImagesResponse
{
    /** @param EcrImage[] $images */
    public function __construct(public readonly array $images) {}

    public static function fromArray(array $d): self
    {
        return new self(
            array_map(static fn($i) => EcrImage::fromArray($i), $d['images'] ?? []),
        );
    }
}

final class EcrPullThroughRule
{
    public function __construct(
        public readonly string $ecrRepositoryPrefix,
        public readonly string $upstreamRegistryUrl,
        public readonly string $createdAt,
        public readonly string $updatedAt,
        public readonly ?string $upstreamRegistry = null,
        public readonly ?string $credentialArn = null,
        public readonly ?string $customRoleArn = null,
    ) {}

    public static function fromArray(array $d): self
    {
        return new self(
            $d['ecrRepositoryPrefix'],
            $d['upstreamRegistryUrl'],
            $d['createdAt'],
            $d['updatedAt'],
            $d['upstreamRegistry'] ?? null,
            $d['credentialArn'] ?? null,
            $d['customRoleArn'] ?? null,
        );
    }
}

final class EcrPullThroughRulesResponse
{
    /** @param EcrPullThroughRule[] $rules */
    public function __construct(public readonly array $rules) {}

    public static function fromArray(array $d): self
    {
        return new self(
            array_map(static fn($r) => EcrPullThroughRule::fromArray($r), $d['rules'] ?? []),
        );
    }
}

// ── ECS ───────────────────────────────────────────────────────

final class EcsTag
{
    public function __construct(
        public readonly string $key,
        public readonly string $value,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['key'], $data['value']);
    }
}

final class EcsCluster
{
    public function __construct(
        public readonly string $clusterName,
        public readonly string $clusterArn,
        public readonly string $status,
        public readonly int $runningTasksCount,
        public readonly int $pendingTasksCount,
        public readonly int $activeServicesCount,
        public readonly int $registeredContainerInstancesCount,
        /** @var string[] */
        public readonly array $capacityProviders,
        /** @var EcsTag[] */
        public readonly array $tags,
        public readonly string $createdAt,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['clusterName'],
            $data['clusterArn'],
            $data['status'],
            (int) ($data['runningTasksCount'] ?? 0),
            (int) ($data['pendingTasksCount'] ?? 0),
            (int) ($data['activeServicesCount'] ?? 0),
            (int) ($data['registeredContainerInstancesCount'] ?? 0),
            $data['capacityProviders'] ?? [],
            array_map(EcsTag::fromArray(...), $data['tags'] ?? []),
            $data['createdAt'] ?? '',
        );
    }
}

final class EcsClustersResponse
{
    public function __construct(
        /** @var EcsCluster[] */
        public readonly array $clusters,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(EcsCluster::fromArray(...), $data['clusters'] ?? []));
    }
}

final class EcsTaskContainer
{
    public function __construct(
        public readonly string $name,
        public readonly string $image,
        public readonly string $lastStatus,
        public readonly ?int $exitCode,
        public readonly ?string $runtimeId,
        public readonly bool $essential,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'] ?? '',
            $data['image'] ?? '',
            $data['lastStatus'] ?? '',
            isset($data['exitCode']) ? (int) $data['exitCode'] : null,
            $data['runtimeId'] ?? null,
            (bool) ($data['essential'] ?? false),
        );
    }
}

final class EcsTask
{
    public function __construct(
        public readonly string $taskArn,
        public readonly string $taskId,
        public readonly string $clusterArn,
        public readonly string $clusterName,
        public readonly string $taskDefinitionArn,
        public readonly string $family,
        public readonly int $revision,
        public readonly string $lastStatus,
        public readonly string $desiredStatus,
        public readonly string $launchType,
        public readonly string $createdAt,
        public readonly ?string $startedAt,
        public readonly ?string $stoppingAt,
        public readonly ?string $stoppedAt,
        public readonly ?string $stopCode,
        public readonly ?string $stoppedReason,
        /** @var EcsTaskContainer[] */
        public readonly array $containers,
        public readonly int $capturedLogBytes,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['taskArn'] ?? '',
            $data['taskId'] ?? '',
            $data['clusterArn'] ?? '',
            $data['clusterName'] ?? '',
            $data['taskDefinitionArn'] ?? '',
            $data['family'] ?? '',
            (int) ($data['revision'] ?? 0),
            $data['lastStatus'] ?? '',
            $data['desiredStatus'] ?? '',
            $data['launchType'] ?? '',
            $data['createdAt'] ?? '',
            $data['startedAt'] ?? null,
            $data['stoppingAt'] ?? null,
            $data['stoppedAt'] ?? null,
            $data['stopCode'] ?? null,
            $data['stoppedReason'] ?? null,
            array_map(EcsTaskContainer::fromArray(...), $data['containers'] ?? []),
            (int) ($data['capturedLogBytes'] ?? 0),
        );
    }
}

final class EcsTasksResponse
{
    public function __construct(
        /** @var EcsTask[] */
        public readonly array $tasks,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(EcsTask::fromArray(...), $data['tasks'] ?? []));
    }
}

final class EcsTaskLogsResponse
{
    public function __construct(
        public readonly string $taskArn,
        public readonly string $logs,
        public readonly string $lastStatus,
        public readonly ?int $exitCode,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['taskArn'] ?? '',
            $data['logs'] ?? '',
            $data['lastStatus'] ?? '',
            isset($data['exitCode']) ? (int) $data['exitCode'] : null,
        );
    }
}

final class EcsMarkFailedRequest
{
    public function __construct(
        public readonly ?int $exitCode = null,
        public readonly ?string $reason = null,
    ) {}

    public function toArray(): array
    {
        $out = [];
        if ($this->exitCode !== null) {
            $out['exitCode'] = $this->exitCode;
        }
        if ($this->reason !== null) {
            $out['reason'] = $this->reason;
        }
        return $out;
    }
}

final class EcsLifecycleEvent
{
    public function __construct(
        public readonly string $at,
        public readonly string $eventType,
        public readonly ?string $taskArn,
        public readonly ?string $clusterArn,
        public readonly ?string $lastStatus,
        public readonly mixed $detail,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['at'] ?? '',
            $data['eventType'] ?? '',
            $data['taskArn'] ?? null,
            $data['clusterArn'] ?? null,
            $data['lastStatus'] ?? null,
            $data['detail'] ?? null,
        );
    }
}

final class EcsEventsResponse
{
    public function __construct(
        /** @var EcsLifecycleEvent[] */
        public readonly array $events,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(EcsLifecycleEvent::fromArray(...), $data['events'] ?? []));
    }
}

final class EcsTaskMetadataLimits
{
    public function __construct(
        public readonly ?float $cpu,
        public readonly ?int $memory,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            isset($data['cpu']) ? (float) $data['cpu'] : null,
            isset($data['memory']) ? (int) $data['memory'] : null,
        );
    }
}

final class EcsTaskMetadataPort
{
    public function __construct(
        public readonly ?int $containerPort,
        public readonly ?int $hostPort,
        public readonly ?string $protocol,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            isset($data['containerPort']) ? (int) $data['containerPort'] : null,
            isset($data['hostPort']) ? (int) $data['hostPort'] : null,
            $data['protocol'] ?? null,
        );
    }
}

final class EcsTaskMetadataContainer
{
    public function __construct(
        public readonly string $name,
        public readonly string $image,
        public readonly ?string $imageId,
        /** @var EcsTaskMetadataPort[] */
        public readonly array $ports,
        /** @var array<string, string> */
        public readonly array $labels,
        public readonly string $desiredStatus,
        public readonly string $knownStatus,
        public readonly EcsTaskMetadataLimits $limits,
        public readonly ?string $createdAt,
        public readonly ?string $startedAt,
        public readonly ?int $exitCode,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'],
            $data['image'],
            $data['imageId'] ?? null,
            array_map(EcsTaskMetadataPort::fromArray(...), $data['ports'] ?? []),
            $data['labels'] ?? [],
            $data['desiredStatus'],
            $data['knownStatus'],
            EcsTaskMetadataLimits::fromArray($data['limits'] ?? []),
            $data['createdAt'] ?? null,
            $data['startedAt'] ?? null,
            isset($data['exitCode']) ? (int) $data['exitCode'] : null,
        );
    }
}

final class EcsTaskMetadata
{
    public function __construct(
        public readonly string $cluster,
        public readonly string $taskArn,
        public readonly string $family,
        public readonly int $revision,
        public readonly string $desiredStatus,
        public readonly string $knownStatus,
        /** @var EcsTaskMetadataContainer[] */
        public readonly array $containers,
        public readonly ?string $pullStartedAt,
        public readonly ?string $pullStoppedAt,
        public readonly string $availabilityZone,
        public readonly string $launchType,
        public readonly ?string $vpcId,
        public readonly ?string $eniId,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['cluster'],
            $data['taskArn'],
            $data['family'],
            (int) $data['revision'],
            $data['desiredStatus'],
            $data['knownStatus'],
            array_map(EcsTaskMetadataContainer::fromArray(...), $data['containers'] ?? []),
            $data['pullStartedAt'] ?? null,
            $data['pullStoppedAt'] ?? null,
            $data['availabilityZone'],
            $data['launchType'],
            $data['vpcId'] ?? null,
            $data['eniId'] ?? null,
        );
    }
}

final class EcsTaskMetadataResponse
{
    public function __construct(public readonly EcsTaskMetadata $task) {}

    public static function fromArray(array $data): self
    {
        return new self(EcsTaskMetadata::fromArray($data['task']));
    }
}

// ── ELBv2 ──────────────────────────────────────────────────────

final class Elbv2Tag
{
    public function __construct(
        public readonly string $key,
        public readonly string $value,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['key'], $data['value']);
    }
}

final class Elbv2AvailabilityZone
{
    public function __construct(
        public readonly string $zoneName,
        public readonly string $subnetId,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self($data['zoneName'], $data['subnetId']);
    }
}

final class Elbv2LoadBalancer
{
    public function __construct(
        public readonly string $arn,
        public readonly string $name,
        public readonly string $dnsName,
        public readonly string $scheme,
        public readonly string $vpcId,
        public readonly string $stateCode,
        public readonly ?string $stateReason,
        public readonly string $lbType,
        public readonly string $ipAddressType,
        /** @var Elbv2AvailabilityZone[] */
        public readonly array $availabilityZones,
        /** @var string[] */
        public readonly array $securityGroups,
        public readonly string $createdTime,
        /** @var Elbv2Tag[] */
        public readonly array $tags,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['arn'],
            $data['name'],
            $data['dnsName'],
            $data['scheme'],
            $data['vpcId'],
            $data['stateCode'],
            $data['stateReason'] ?? null,
            $data['lbType'],
            $data['ipAddressType'],
            array_map(Elbv2AvailabilityZone::fromArray(...), $data['availabilityZones'] ?? []),
            $data['securityGroups'] ?? [],
            $data['createdTime'],
            array_map(Elbv2Tag::fromArray(...), $data['tags'] ?? []),
        );
    }
}

final class Elbv2LoadBalancersResponse
{
    public function __construct(
        /** @var Elbv2LoadBalancer[] */
        public readonly array $loadBalancers,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(Elbv2LoadBalancer::fromArray(...), $data['loadBalancers'] ?? []));
    }
}

final class Elbv2Target
{
    public function __construct(
        public readonly string $id,
        public readonly ?int $port,
        public readonly ?string $availabilityZone,
        public readonly string $healthState,
        public readonly ?string $healthReason,
        public readonly ?string $healthDescription,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['id'],
            isset($data['port']) ? (int) $data['port'] : null,
            $data['availabilityZone'] ?? null,
            $data['healthState'],
            $data['healthReason'] ?? null,
            $data['healthDescription'] ?? null,
        );
    }
}

final class Elbv2TargetGroup
{
    public function __construct(
        public readonly string $arn,
        public readonly string $name,
        public readonly ?string $protocol,
        public readonly ?int $port,
        public readonly ?string $vpcId,
        public readonly string $targetType,
        /** @var string[] */
        public readonly array $loadBalancerArns,
        /** @var Elbv2Target[] */
        public readonly array $targets,
        public readonly ?string $healthCheckProtocol,
        public readonly ?string $healthCheckPort,
        public readonly ?string $healthCheckPath,
        public readonly int $healthyThresholdCount,
        public readonly int $unhealthyThresholdCount,
        public readonly string $createdTime,
        /** @var Elbv2Tag[] */
        public readonly array $tags,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['arn'],
            $data['name'],
            $data['protocol'] ?? null,
            isset($data['port']) ? (int) $data['port'] : null,
            $data['vpcId'] ?? null,
            $data['targetType'],
            $data['loadBalancerArns'] ?? [],
            array_map(Elbv2Target::fromArray(...), $data['targets'] ?? []),
            $data['healthCheckProtocol'] ?? null,
            $data['healthCheckPort'] ?? null,
            $data['healthCheckPath'] ?? null,
            (int) ($data['healthyThresholdCount'] ?? 0),
            (int) ($data['unhealthyThresholdCount'] ?? 0),
            $data['createdTime'],
            array_map(Elbv2Tag::fromArray(...), $data['tags'] ?? []),
        );
    }
}

final class Elbv2TargetGroupsResponse
{
    public function __construct(
        /** @var Elbv2TargetGroup[] */
        public readonly array $targetGroups,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(Elbv2TargetGroup::fromArray(...), $data['targetGroups'] ?? []));
    }
}

final class Elbv2Listener
{
    public function __construct(
        public readonly string $arn,
        public readonly string $loadBalancerArn,
        public readonly ?int $port,
        public readonly ?string $protocol,
        public readonly ?string $sslPolicy,
        /** @var string[] */
        public readonly array $certificateArns,
        public readonly ?string $defaultActionType,
        public readonly ?string $defaultTargetGroupArn,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['arn'],
            $data['loadBalancerArn'],
            isset($data['port']) ? (int) $data['port'] : null,
            $data['protocol'] ?? null,
            $data['sslPolicy'] ?? null,
            $data['certificateArns'] ?? [],
            $data['defaultActionType'] ?? null,
            $data['defaultTargetGroupArn'] ?? null,
        );
    }
}

final class Elbv2ListenersResponse
{
    public function __construct(
        /** @var Elbv2Listener[] */
        public readonly array $listeners,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(Elbv2Listener::fromArray(...), $data['listeners'] ?? []));
    }
}

final class Elbv2Rule
{
    public function __construct(
        public readonly string $arn,
        public readonly string $listenerArn,
        public readonly string $priority,
        public readonly bool $isDefault,
        /** @var string[] */
        public readonly array $conditionFields,
        public readonly ?string $actionType,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            $data['arn'],
            $data['listenerArn'],
            $data['priority'],
            (bool) ($data['isDefault'] ?? false),
            $data['conditionFields'] ?? [],
            $data['actionType'] ?? null,
        );
    }
}

final class Elbv2RulesResponse
{
    public function __construct(
        /** @var Elbv2Rule[] */
        public readonly array $rules,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(Elbv2Rule::fromArray(...), $data['rules'] ?? []));
    }
}

final class Elbv2FlushAccessLogsResponse
{
    public function __construct(
        public readonly bool $flushed,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self((bool) ($data['flushed'] ?? false));
    }
}

// ── CloudWatch Logs ────────────────────────────────────────────────

/**
 * Admin payload for `/_fakecloud/logs/anomalies/inject`. Lets tests
 * seed synthetic CloudWatch Logs anomalies so they can exercise
 * `ListAnomalies`/`UpdateAnomaly` deterministically.
 *
 * @param list<string> $logGroupArns
 */
final class LogsAnomalyInjectRequest
{
    /** @param list<string> $logGroupArns */
    public function __construct(
        public readonly string $anomalyDetectorArn,
        public readonly string $patternString,
        public readonly array $logGroupArns = [],
        public readonly ?string $priority = null,
    ) {}

    public function toArray(): array
    {
        $a = [
            'anomalyDetectorArn' => $this->anomalyDetectorArn,
            'patternString' => $this->patternString,
            'logGroupArns' => $this->logGroupArns,
        ];
        if ($this->priority !== null) {
            $a['priority'] = $this->priority;
        }
        return $a;
    }
}

final class LogsAnomalyInjectResponse
{
    public function __construct(
        public readonly string $anomalyId,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self((string) $data['anomalyId']);
    }
}

/**
 * One entry of GET /_fakecloud/logs/delivery-config. Joins a Delivery
 * with the logType from its DeliverySource so test code can assert
 * end-to-end wiring without re-querying the AWS-shaped APIs.
 */
final class LogsDeliveryConfiguration
{
    /**
     * @param string[] $recordFields
     * @param mixed $s3DeliveryConfiguration
     */
    public function __construct(
        public readonly string $id,
        public readonly string $name,
        public readonly string $deliveryDestinationArn,
        public readonly string $deliverySourceName,
        public readonly string $logType,
        public readonly int $createdAt,
        public readonly array $recordFields = [],
        public readonly ?string $fieldDelimiter = null,
        public readonly mixed $s3DeliveryConfiguration = null,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['id'] ?? ''),
            (string) ($data['name'] ?? ''),
            (string) ($data['deliveryDestinationArn'] ?? ''),
            (string) ($data['deliverySourceName'] ?? ''),
            (string) ($data['logType'] ?? ''),
            (int) ($data['createdAt'] ?? 0),
            array_values(array_map('strval', $data['recordFields'] ?? [])),
            isset($data['fieldDelimiter']) ? (string) $data['fieldDelimiter'] : null,
            $data['s3DeliveryConfiguration'] ?? null,
        );
    }
}

final class LogsDeliveryConfigResponse
{
    /** @param LogsDeliveryConfiguration[] $configurations */
    public function __construct(
        public readonly array $configurations,
    ) {}

    public static function fromArray(array $data): self
    {
        $items = array_map(
            static fn ($c) => LogsDeliveryConfiguration::fromArray($c),
            $data['configurations'] ?? []
        );
        return new self(array_values($items));
    }
}

/** One parsed IndexPolicy on a log group. */
final class LogsFieldIndex
{
    /** @param string[] $fields */
    public function __construct(
        public readonly array $fields,
        public readonly int $createdAt,
        public readonly int $lastUsedAt,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            array_values(array_map('strval', $data['fields'] ?? [])),
            (int) ($data['createdAt'] ?? 0),
            (int) ($data['lastUsedAt'] ?? 0),
        );
    }
}

final class LogsFieldIndexesResponse
{
    /** @param LogsFieldIndex[] $indexes */
    public function __construct(
        public readonly string $logGroupName,
        public readonly array $indexes,
    ) {}

    public static function fromArray(array $data): self
    {
        $indexes = array_map(
            static fn ($i) => LogsFieldIndex::fromArray($i),
            $data['indexes'] ?? []
        );
        return new self(
            (string) ($data['logGroupName'] ?? ''),
            array_values($indexes),
        );
    }
}

// ── Organizations ─────────────────────────────────────────────────

/**
 * A single Organizations tag (key/value).
 */
final class OrganizationsTag
{
    public function __construct(
        public readonly string $key,
        public readonly string $value,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self((string) ($data['key'] ?? ''), (string) ($data['value'] ?? ''));
    }
}

final class OrganizationsAccount
{
    public function __construct(
        public readonly string $id,
        public readonly string $arn,
        public readonly string $email,
        public readonly string $name,
        public readonly string $status,
        public readonly string $joinedMethod,
        public readonly string $joinedTimestamp,
        public readonly ?string $parentOuId = null,
        /** @var OrganizationsTag[] */
        public readonly array $tags = [],
        /** @var string[] */
        public readonly array $scpAttached = [],
    ) {}

    public static function fromArray(array $data): self
    {
        $tags = array_map(OrganizationsTag::fromArray(...), $data['tags'] ?? []);
        $scp = array_map('strval', $data['scpAttached'] ?? []);
        return new self(
            (string) ($data['id'] ?? ''),
            (string) ($data['arn'] ?? ''),
            (string) ($data['email'] ?? ''),
            (string) ($data['name'] ?? ''),
            (string) ($data['status'] ?? ''),
            (string) ($data['joinedMethod'] ?? ''),
            (string) ($data['joinedTimestamp'] ?? ''),
            isset($data['parentOuId']) ? (string) $data['parentOuId'] : null,
            $tags,
            $scp,
        );
    }
}

final class OrganizationsAccountsResponse
{
    public function __construct(
        /** @var OrganizationsAccount[] */
        public readonly array $accounts,
        public readonly ?string $managementAccountId = null,
        public readonly ?string $masterAccountId = null,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            array_map(OrganizationsAccount::fromArray(...), $data['accounts'] ?? []),
            isset($data['managementAccountId']) ? (string) $data['managementAccountId'] : null,
            isset($data['masterAccountId']) ? (string) $data['masterAccountId'] : null,
        );
    }
}

// ── Athena ────────────────────────────────────────────────────────

final class AthenaNamedQuery
{
    public function __construct(
        public readonly string $namedQueryId,
        public readonly string $name,
        public readonly ?string $description,
        public readonly string $database,
        public readonly string $queryString,
        public readonly string $workgroup,
        public readonly ?string $lastUsedAt,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['namedQueryId'] ?? ''),
            (string) ($data['name'] ?? ''),
            isset($data['description']) ? (string) $data['description'] : null,
            (string) ($data['database'] ?? ''),
            (string) ($data['queryString'] ?? ''),
            (string) ($data['workgroup'] ?? ''),
            isset($data['lastUsedAt']) ? (string) $data['lastUsedAt'] : null,
        );
    }
}

final class AthenaNamedQueriesResponse
{
    public function __construct(
        /** @var AthenaNamedQuery[] */
        public readonly array $queries,
    ) {}

    public static function fromArray(array $data): self
    {
        return new self(array_map(AthenaNamedQuery::fromArray(...), $data['queries'] ?? []));
    }
}
