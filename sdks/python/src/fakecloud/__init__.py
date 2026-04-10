"""fakecloud — Python SDK for the fakecloud local AWS emulator."""

from fakecloud.client import (
    CognitoClient,
    DynamoDbClient,
    ElastiCacheClient,
    EventsClient,
    FakeCloud,
    FakeCloudSync,
    LambdaClient,
    RdsClient,
    S3Client,
    SecretsManagerClient,
    SesClient,
    SnsClient,
    SqsClient,
)

__all__ = [
    "CognitoClient",
    "DynamoDbClient",
    "ElastiCacheClient",
    "EventsClient",
    "FakeCloud",
    "FakeCloudSync",
    "LambdaClient",
    "RdsClient",
    "S3Client",
    "SecretsManagerClient",
    "SesClient",
    "SnsClient",
    "SqsClient",
]
