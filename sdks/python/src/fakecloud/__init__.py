"""fakecloud — Python SDK for the fakecloud local AWS emulator."""

from fakecloud.client import (
    CognitoClient,
    DynamoDbClient,
    EventsClient,
    FakeCloud,
    FakeCloudSync,
    LambdaClient,
    S3Client,
    SecretsManagerClient,
    SesClient,
    SnsClient,
    SqsClient,
)

__all__ = [
    "CognitoClient",
    "DynamoDbClient",
    "EventsClient",
    "FakeCloud",
    "FakeCloudSync",
    "LambdaClient",
    "S3Client",
    "SecretsManagerClient",
    "SesClient",
    "SnsClient",
    "SqsClient",
]
