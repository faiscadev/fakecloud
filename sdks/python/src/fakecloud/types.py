"""Dataclass types matching the fakecloud introspection API responses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def _camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    import re

    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def _convert_keys(data: dict) -> dict:
    """Recursively convert camelCase dict keys to snake_case."""
    result = {}
    for key, value in data.items():
        snake_key = _camel_to_snake(key)
        if isinstance(value, dict):
            value = _convert_keys(value)
        elif isinstance(value, list):
            value = [_convert_keys(v) if isinstance(v, dict) else v for v in value]
        result[snake_key] = value
    return result


# ── Health & Reset ──────────────────────────────────────────────────


@dataclass
class HealthResponse:
    status: str
    version: str
    services: List[str]

    @classmethod
    def from_dict(cls, data: dict) -> HealthResponse:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class ResetResponse:
    status: str

    @classmethod
    def from_dict(cls, data: dict) -> ResetResponse:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class ResetServiceResponse:
    reset: str

    @classmethod
    def from_dict(cls, data: dict) -> ResetServiceResponse:
        d = _convert_keys(data)
        return cls(**d)


# ── Lambda ──────────────────────────────────────────────────────────


@dataclass
class LambdaInvocation:
    function_arn: str
    payload: str
    source: str
    timestamp: str

    @classmethod
    def from_dict(cls, data: dict) -> LambdaInvocation:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class LambdaInvocationsResponse:
    invocations: List[LambdaInvocation]

    @classmethod
    def from_dict(cls, data: dict) -> LambdaInvocationsResponse:
        d = _convert_keys(data)
        return cls(
            invocations=[LambdaInvocation.from_dict(i) for i in data.get("invocations", [])],
        )


@dataclass
class WarmContainer:
    function_name: str
    runtime: str
    container_id: str
    last_used_secs_ago: int

    @classmethod
    def from_dict(cls, data: dict) -> WarmContainer:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class WarmContainersResponse:
    containers: List[WarmContainer]

    @classmethod
    def from_dict(cls, data: dict) -> WarmContainersResponse:
        return cls(
            containers=[WarmContainer.from_dict(c) for c in data.get("containers", [])],
        )


@dataclass
class EvictContainerResponse:
    evicted: bool

    @classmethod
    def from_dict(cls, data: dict) -> EvictContainerResponse:
        d = _convert_keys(data)
        return cls(**d)


# ── SES ─────────────────────────────────────────────────────────────


@dataclass
class SentEmail:
    message_id: str
    from_addr: str
    to: List[str]
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    subject: Optional[str] = None
    html_body: Optional[str] = None
    text_body: Optional[str] = None
    raw_data: Optional[str] = None
    template_name: Optional[str] = None
    template_data: Optional[str] = None
    timestamp: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> SentEmail:
        d = _convert_keys(data)
        # The JSON field is "from" but that's a Python keyword, so we map it.
        if "from" in data:
            d["from_addr"] = data["from"]
        d.pop("from", None)
        return cls(**d)


@dataclass
class SesEmailsResponse:
    emails: List[SentEmail]

    @classmethod
    def from_dict(cls, data: dict) -> SesEmailsResponse:
        return cls(
            emails=[SentEmail.from_dict(e) for e in data.get("emails", [])],
        )


@dataclass
class InboundEmailRequest:
    from_addr: str
    to: List[str]
    subject: str
    body: str

    def to_dict(self) -> dict:
        return {
            "from": self.from_addr,
            "to": self.to,
            "subject": self.subject,
            "body": self.body,
        }


@dataclass
class InboundActionExecuted:
    rule: str
    action_type: str

    @classmethod
    def from_dict(cls, data: dict) -> InboundActionExecuted:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class InboundEmailResponse:
    message_id: str
    matched_rules: List[str]
    actions_executed: List[InboundActionExecuted]

    @classmethod
    def from_dict(cls, data: dict) -> InboundEmailResponse:
        d = _convert_keys(data)
        return cls(
            message_id=d["message_id"],
            matched_rules=d.get("matched_rules", []),
            actions_executed=[
                InboundActionExecuted.from_dict(a)
                for a in data.get("actionsExecuted", [])
            ],
        )


# ── SNS ─────────────────────────────────────────────────────────────


@dataclass
class SnsMessage:
    message_id: str
    topic_arn: str
    message: str
    subject: Optional[str] = None
    timestamp: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> SnsMessage:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class SnsMessagesResponse:
    messages: List[SnsMessage]

    @classmethod
    def from_dict(cls, data: dict) -> SnsMessagesResponse:
        return cls(
            messages=[SnsMessage.from_dict(m) for m in data.get("messages", [])],
        )


@dataclass
class PendingConfirmation:
    subscription_arn: str
    topic_arn: str
    protocol: str
    endpoint: str
    token: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> PendingConfirmation:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class PendingConfirmationsResponse:
    pending_confirmations: List[PendingConfirmation]

    @classmethod
    def from_dict(cls, data: dict) -> PendingConfirmationsResponse:
        return cls(
            pending_confirmations=[
                PendingConfirmation.from_dict(p)
                for p in data.get("pendingConfirmations", [])
            ],
        )


@dataclass
class ConfirmSubscriptionRequest:
    subscription_arn: str

    def to_dict(self) -> dict:
        return {"subscriptionArn": self.subscription_arn}


@dataclass
class ConfirmSubscriptionResponse:
    confirmed: bool

    @classmethod
    def from_dict(cls, data: dict) -> ConfirmSubscriptionResponse:
        d = _convert_keys(data)
        return cls(**d)


# ── SQS ─────────────────────────────────────────────────────────────


@dataclass
class SqsMessageInfo:
    message_id: str
    body: str
    receive_count: int
    in_flight: bool
    created_at: str

    @classmethod
    def from_dict(cls, data: dict) -> SqsMessageInfo:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class SqsQueueMessages:
    queue_url: str
    queue_name: str
    messages: List[SqsMessageInfo]

    @classmethod
    def from_dict(cls, data: dict) -> SqsQueueMessages:
        d = _convert_keys(data)
        return cls(
            queue_url=d["queue_url"],
            queue_name=d["queue_name"],
            messages=[SqsMessageInfo.from_dict(m) for m in data.get("messages", [])],
        )


@dataclass
class SqsMessagesResponse:
    queues: List[SqsQueueMessages]

    @classmethod
    def from_dict(cls, data: dict) -> SqsMessagesResponse:
        return cls(
            queues=[SqsQueueMessages.from_dict(q) for q in data.get("queues", [])],
        )


@dataclass
class ExpirationTickResponse:
    expired_messages: int

    @classmethod
    def from_dict(cls, data: dict) -> ExpirationTickResponse:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class ForceDlqResponse:
    moved_messages: int

    @classmethod
    def from_dict(cls, data: dict) -> ForceDlqResponse:
        d = _convert_keys(data)
        return cls(**d)


# ── EventBridge ─────────────────────────────────────────────────────


@dataclass
class EventBridgeEvent:
    event_id: str
    source: str
    detail_type: str
    detail: str
    bus_name: str
    timestamp: str

    @classmethod
    def from_dict(cls, data: dict) -> EventBridgeEvent:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class EventBridgeLambdaDelivery:
    function_arn: str
    payload: str
    timestamp: str

    @classmethod
    def from_dict(cls, data: dict) -> EventBridgeLambdaDelivery:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class EventBridgeLogDelivery:
    log_group_arn: str
    payload: str
    timestamp: str

    @classmethod
    def from_dict(cls, data: dict) -> EventBridgeLogDelivery:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class EventBridgeDeliveries:
    lambda_deliveries: List[EventBridgeLambdaDelivery] = field(default_factory=list)
    log_deliveries: List[EventBridgeLogDelivery] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> EventBridgeDeliveries:
        return cls(
            lambda_deliveries=[
                EventBridgeLambdaDelivery.from_dict(d)
                for d in data.get("lambda", [])
            ],
            log_deliveries=[
                EventBridgeLogDelivery.from_dict(d) for d in data.get("logs", [])
            ],
        )


@dataclass
class EventHistoryResponse:
    events: List[EventBridgeEvent]
    deliveries: EventBridgeDeliveries

    @classmethod
    def from_dict(cls, data: dict) -> EventHistoryResponse:
        return cls(
            events=[EventBridgeEvent.from_dict(e) for e in data.get("events", [])],
            deliveries=EventBridgeDeliveries.from_dict(data.get("deliveries", {})),
        )


@dataclass
class FireRuleRequest:
    rule_name: str
    bus_name: Optional[str] = None

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {"ruleName": self.rule_name}
        if self.bus_name is not None:
            d["busName"] = self.bus_name
        return d


@dataclass
class FireRuleTarget:
    target_type: str
    arn: str

    @classmethod
    def from_dict(cls, data: dict) -> FireRuleTarget:
        return cls(target_type=data.get("type", ""), arn=data.get("arn", ""))


@dataclass
class FireRuleResponse:
    targets: List[FireRuleTarget]

    @classmethod
    def from_dict(cls, data: dict) -> FireRuleResponse:
        return cls(
            targets=[FireRuleTarget.from_dict(t) for t in data.get("targets", [])],
        )


# ── S3 ──────────────────────────────────────────────────────────────


@dataclass
class S3Notification:
    bucket: str
    key: str
    event_type: str
    timestamp: str

    @classmethod
    def from_dict(cls, data: dict) -> S3Notification:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class S3NotificationsResponse:
    notifications: List[S3Notification]

    @classmethod
    def from_dict(cls, data: dict) -> S3NotificationsResponse:
        return cls(
            notifications=[
                S3Notification.from_dict(n) for n in data.get("notifications", [])
            ],
        )


@dataclass
class LifecycleTickResponse:
    processed_buckets: int
    expired_objects: int
    transitioned_objects: int

    @classmethod
    def from_dict(cls, data: dict) -> LifecycleTickResponse:
        d = _convert_keys(data)
        return cls(**d)


# ── DynamoDB ────────────────────────────────────────────────────────


@dataclass
class TtlTickResponse:
    expired_items: int

    @classmethod
    def from_dict(cls, data: dict) -> TtlTickResponse:
        d = _convert_keys(data)
        return cls(**d)


# ── SecretsManager ──────────────────────────────────────────────────


@dataclass
class RotationTickResponse:
    rotated_secrets: List[str]

    @classmethod
    def from_dict(cls, data: dict) -> RotationTickResponse:
        d = _convert_keys(data)
        return cls(**d)


# ── Cognito ─────────────────────────────────────────────────────────


@dataclass
class UserConfirmationCodes:
    confirmation_code: Optional[str] = None
    attribute_verification_codes: Any = None

    @classmethod
    def from_dict(cls, data: dict) -> UserConfirmationCodes:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class ConfirmationCode:
    pool_id: str
    username: str
    code: str
    code_type: str
    attribute: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> ConfirmationCode:
        d = _convert_keys(data)
        # JSON uses "type" which maps to "code_type"
        if "type" in data:
            d["code_type"] = data["type"]
        d.pop("type", None)
        return cls(**d)


@dataclass
class ConfirmationCodesResponse:
    codes: List[ConfirmationCode]

    @classmethod
    def from_dict(cls, data: dict) -> ConfirmationCodesResponse:
        return cls(
            codes=[ConfirmationCode.from_dict(c) for c in data.get("codes", [])],
        )


@dataclass
class ConfirmUserRequest:
    user_pool_id: str
    username: str

    def to_dict(self) -> dict:
        return {"userPoolId": self.user_pool_id, "username": self.username}


@dataclass
class ConfirmUserResponse:
    confirmed: bool
    error: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> ConfirmUserResponse:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class TokenInfo:
    token_type: str
    username: str
    pool_id: str
    client_id: str
    issued_at: float

    @classmethod
    def from_dict(cls, data: dict) -> TokenInfo:
        d = _convert_keys(data)
        # JSON uses "type" which maps to "token_type"
        if "type" in data:
            d["token_type"] = data["type"]
        d.pop("type", None)
        return cls(**d)


@dataclass
class TokensResponse:
    tokens: List[TokenInfo]

    @classmethod
    def from_dict(cls, data: dict) -> TokensResponse:
        return cls(
            tokens=[TokenInfo.from_dict(t) for t in data.get("tokens", [])],
        )


@dataclass
class ExpireTokensRequest:
    user_pool_id: Optional[str] = None
    username: Optional[str] = None

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        if self.user_pool_id is not None:
            d["userPoolId"] = self.user_pool_id
        if self.username is not None:
            d["username"] = self.username
        return d


@dataclass
class ExpireTokensResponse:
    expired_tokens: int

    @classmethod
    def from_dict(cls, data: dict) -> ExpireTokensResponse:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class AuthEvent:
    event_type: str
    username: str
    user_pool_id: str
    client_id: Optional[str] = None
    timestamp: float = 0.0
    success: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> AuthEvent:
        d = _convert_keys(data)
        return cls(**d)


@dataclass
class AuthEventsResponse:
    events: List[AuthEvent]

    @classmethod
    def from_dict(cls, data: dict) -> AuthEventsResponse:
        return cls(
            events=[AuthEvent.from_dict(e) for e in data.get("events", [])],
        )
