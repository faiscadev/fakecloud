"""
Pytest conftest for running Moto's test suite against FakeCloud.

This file is merged into moto's tests/conftest.py before running.
It works alongside TEST_SERVER_MODE=true which makes @mock_aws patch
boto3.client/resource to point at FakeCloud instead of mocking internally.

Environment variables set by run.sh:
    TEST_SERVER_MODE=true
    TEST_SERVER_MODE_ENDPOINT=http://localhost:4566
    MOTO_CALL_RESET_API=false  (FakeCloud doesn't have /moto-api/reset)
    AWS_DEFAULT_REGION=us-east-1
    AWS_ACCESS_KEY_ID=testing
    AWS_SECRET_ACCESS_KEY=testing
"""

import logging
import os

import pytest
import requests

logger = logging.getLogger("fakecloud-compat")

FAKECLOUD_ENDPOINT = os.environ.get(
    "TEST_SERVER_MODE_ENDPOINT", "http://localhost:4566"
)


@pytest.fixture(autouse=True)
def _fakecloud_reset():
    """Try to reset FakeCloud state before each test.

    Attempts /_fakecloud/reset first, then /moto-api/reset.
    If neither exists, logs a warning and continues.
    """
    for path in ("/_fakecloud/reset", "/moto-api/reset"):
        try:
            resp = requests.post(f"{FAKECLOUD_ENDPOINT}{path}", timeout=2)
            if resp.status_code < 500:
                break
        except requests.exceptions.RequestException:
            pass
    yield
