# Condition keys of service-forwarded KMS calls

`recordings.md` records, against real AWS (us-east-1), which global condition
keys a KMS request carries when a service makes it with the caller's
credentials (a forward access session): S3 SSE-KMS `PutObject`, SQS
`SendMessage` to an `alias/aws/sqs` queue, SNS `Publish` to an `alias/aws/sns`
topic and SSM `PutParameter` `SecureString`. The caller's account and the key
and authorization ids are redacted; `556613725826` is the AWS-owned account
SSM's calls name in `aws:CalledVia`, not a customer account.

`fakecloud_kms::aws_managed::called_via` is the table of the recorded
`aws:CalledVia` values (S3 `fas.s3.amazonaws.com`, SQS `sqs.amazonaws.com`,
SNS `sns.amazonaws.com`, SSM in us-east-1 `556613725826`), with DynamoDB's
`dynamodb.amazonaws.com` from the IAM user guide's own `aws:CalledVia`
example. Services not recorded use their service principal.
