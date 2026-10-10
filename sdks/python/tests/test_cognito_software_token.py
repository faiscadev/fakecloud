"""Unit tests for the Cognito software-token helper against a mocked transport.

No fakecloud server is needed: each test serves a canned response through
``httpx.MockTransport`` and asserts on the request the SDK sends.
"""

from __future__ import annotations

import json
from typing import List

import httpx
import pytest

from fakecloud.client import CognitoClient, FakeCloudError, _SyncCognitoClient
from fakecloud.types import SetSoftwareTokenRequest

BASE = "http://fc.test"

REQ = SetSoftwareTokenRequest(
    user_pool_id="us-east-1_Local", username="alice", secret_code="JBSWY3DPEHPK3PXP"
)


def test_set_software_token_posts_secret() -> None:
    seen: List[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"enrolled": True})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    resp = _SyncCognitoClient(client, BASE).set_software_token(REQ)
    assert resp.enrolled is True
    req = seen[0]
    assert req.method == "POST"
    assert req.url.path == "/_fakecloud/cognito/software-token"
    assert json.loads(req.content) == {
        "userPoolId": "us-east-1_Local",
        "username": "alice",
        "secretCode": "JBSWY3DPEHPK3PXP",
    }


async def test_async_set_software_token_surfaces_errors() -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "user not found in pool"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FakeCloudError) as exc:
            await CognitoClient(client, BASE).set_software_token(REQ)
        assert exc.value.status == 404
