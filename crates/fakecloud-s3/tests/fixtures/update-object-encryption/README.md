# UpdateObjectEncryption refusals

Recorded with boto3 1.42.97 against a real account (us-east-1, 2026-11-12):
`s3.update_object_encryption(Bucket=..., Key=..., ObjectEncryption={"SSEKMS":
{"KMSKeyArn": ..., "BucketKeyEnabled": ...}})` on an SSE-S3 object, the body
form the API reference defines. The account id is normalized to
`123456789012`; `111122223333` stands for another account, whose key was
named. Each refusal left the object as it was: `HeadObject` still reported
`AES256` with no KMS key.

- `refusals.txt`: one `== <case>` block per call, with the `KMSKeyArn` (and
  `BucketKeyEnabled`, when sent), the HTTP status and error code, and the
  error message exactly as returned. A key that does not exist is refused by
  KMS (`KMS.NotFoundException`, as on `PutObject`), a value that is not an
  ARN and an alias ARN are `InvalidRequest`, and a key of another account
  is `AccessDenied`.

`fakecloud-s3`'s `update_object_encryption_tests` hold the handler's
refusals to these recordings.
