"""fakecloud — Python SDK for the fakecloud local AWS emulator."""

from fakecloud.client import (
    BedrockClient,
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
    "BedrockClient",
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
