# CreateGrant refusals

Recorded against a real account (us-east-1, 2026-11-12): `CreateGrant` with
`KeyId` the ARN of a key that does not exist in the caller's account and
`Operations` `["Decrypt"]`, varying the grantee and retiring principals and
the `SourceArn` constraint. The account id is normalized to
`123456789012`. Public botocore does not model `GranteeServicePrincipal`,
`RetiringServicePrincipal` or `Constraints.SourceArn` yet (its client-side
validation rejects them), but the live API accepts them.

- `refusals.txt`: one `== <case>` block per call: the request members besides
  `KeyId` and `Operations` (`Request:`, JSON), the HTTP status and error code,
  and the error message exactly as returned. Every refusal is
  `ValidationException`, and they all come before the key is looked up: a
  request that passes them is answered `NotFoundException` for the missing
  key. A `SourceArn` off the model's pattern is refused before the
  principal checks; a `SourceArn` is accepted with an IAM grantee and
  alongside `EncryptionContextSubset`. The principals and ARNs inside the
  requests stand for the ones recorded; the messages that quote a request
  value quote the recorded one.

`fakecloud-kms`'s `create_grant_refusals_match_the_recordings` replays these
through the handler.
