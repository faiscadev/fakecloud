"""Responses carrying fields this SDK version does not know still decode."""

from __future__ import annotations

from fakecloud.types import (
    ApiGatewayV2Request,
    OrganizationsAccount,
    ServiceQuotasEnforcementResponse,
    SqsMessagesResponse,
)


def test_unknown_fields_on_nested_items_are_dropped() -> None:
    out = SqsMessagesResponse.from_dict(
        {
            "queues": [
                {
                    "queueUrl": "http://localhost:4566/123456789012/q",
                    "queueName": "q",
                    "messages": [
                        {
                            "messageId": "m-1",
                            "body": "hello",
                            "receiveCount": 0,
                            "inFlight": False,
                            "createdAt": "2026-10-30T00:00:00+00:00",
                            "fieldFromANewerServer": "x",
                        }
                    ],
                }
            ]
        }
    )
    assert out.queues[0].messages[0].body == "hello"


def test_unknown_fields_on_flat_items_are_dropped() -> None:
    out = ApiGatewayV2Request.from_dict(
        {
            "requestId": "r-1",
            "apiId": "a",
            "stage": "$default",
            "method": "GET",
            "path": "/",
            "headers": {},
            "queryParams": {},
            "timestamp": "2026-10-30T00:00:00+00:00",
            "statusCode": 200,
            "fieldFromANewerServer": 1,
        }
    )
    assert out.request_id == "r-1"


def test_unknown_fields_on_hand_built_children_are_dropped() -> None:
    account = OrganizationsAccount.from_dict(
        {
            "id": "123456789012",
            "arn": "arn:aws:organizations::123456789012:account/o-1/123456789012",
            "email": "a@example.com",
            "name": "a",
            "status": "ACTIVE",
            "joinedMethod": "CREATED",
            "joinedTimestamp": "2026-10-30T00:00:00+00:00",
            "tags": [{"key": "k", "value": "v", "fieldFromANewerServer": 1}],
        }
    )
    assert account.tags[0].key == "k"

    enforcement = ServiceQuotasEnforcementResponse.from_dict(
        {
            "enforceAll": False,
            "overrides": [
                {
                    "serviceCode": "sqs",
                    "quotaCode": "L-1",
                    "enforce": True,
                    "fieldFromANewerServer": 1,
                }
            ],
            "accountOverrides": [
                {
                    "accountId": "123456789012",
                    "serviceCode": "sqs",
                    "quotaCode": "L-1",
                    "enforce": True,
                    "fieldFromANewerServer": 1,
                }
            ],
        }
    )
    assert enforcement.overrides[0].enforce is True
    assert enforcement.account_overrides[0].account_id == "123456789012"
