# Real-AWS recordings: condition keys on service-forwarded KMS calls (us-east-1, 2026-11-12)

Method: per case, a fresh IAM role (trust = own account root) with an identity policy allowing the data-plane action and kms:GenerateDataKey/Encrypt/Decrypt on *, plus one `Deny kms:*` statement with the tested condition. The caller assumed the role and made one request using the service's AWS-managed key. "matches" = the Deny fired (request refused naming the kms action); "no match" = request succeeded.

## S3 PutObject with ServerSideEncryption=aws:kms (aws/s3 key)
- Null aws:CalledVia false -> matches (key present); Null true -> no match
- ForAnyValue:StringEquals aws:CalledVia "s3.amazonaws.com" -> no match
- ForAnyValue:StringEquals aws:CalledVia "fas.s3.amazonaws.com" -> matches
- StringEquals aws:CalledViaFirst "fas.s3.amazonaws.com" -> matches; aws:CalledViaLast "fas.s3.amazonaws.com" -> matches
- StringEquals aws:CalledViaLast "s3.amazonaws.com" / "s3.us-east-1.amazonaws.com" -> no match
- ForAllValues:StringEquals aws:CalledVia "x.example" -> no match (list non-empty)
- Bool aws:ViaAWSService true -> matches
- Bool aws:PrincipalIsAWSService true -> no match
- StringEquals kms:ViaService "s3.us-east-1.amazonaws.com" -> matches
- StringEquals aws:ResourceAccount <own acct> -> matches; StringEquals aws:RequestedRegion "us-east-1" -> matches
- Refusal: HTTP 403 `AccessDenied` "User: arn:aws:sts::<acct>:assumed-role/<role>/probe is not authorized to perform: kms:GenerateDataKey on resource: arn:aws:kms:us-east-1:<acct>:key/<id> with an explicit deny in an identity-based policy. Go to https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/authorization-details/<authz-id> for complete details, or call the GetRequestAuthorizationDetails API with the following authorization id: <authz-id>"

## SQS SendMessage to a queue with KmsMasterKeyId=alias/aws/sqs
- Null aws:CalledVia false -> matches
- ForAnyValue:StringEquals aws:CalledVia "sqs.amazonaws.com" -> matches
- ForAnyValue:StringEquals aws:CalledVia "fas.sqs.amazonaws.com" -> no match
- Bool aws:ViaAWSService true -> matches
- Refusal code `KMS.AccessDeniedException`, message starts "User: arn:aws:sts::<acct>:assumed-role/<role>/probe is not authorized to perform: kms:GenerateDataKey on resource: arn:aws:kms:us-east-1:<acct>:key/<id>"

## SNS Publish to a topic with KmsMasterKeyId=alias/aws/sns
- Null aws:CalledVia false -> matches
- ForAnyValue:StringEquals aws:CalledVia "sns.amazonaws.com" -> matches
- ForAnyValue:StringEquals aws:CalledVia "fas.sns.amazonaws.com" -> no match
- Bool aws:ViaAWSService true -> matches
- Refusal code `KMSAccessDenied`, message starts "User: arn:aws:sts::<acct>:assumed-role/<role>/probe is not authorized to perform: kms:GenerateDataKey on resource: arn:aws:kms:us-east-1:<acct>:key/<id>"

## SSM PutParameter Type=SecureString (aws/ssm key)
- Null aws:CalledVia false -> matches
- ForAnyValue:StringEquals "ssm.amazonaws.com" / "fas.ssm.amazonaws.com" -> no match
- ForAnyValue:StringLike: "*" matches; "*.*", "*ssm*", "*aws*", "*amazonaws.com", "fas.*" do not
- Exact value (found by per-position StringLike): `556613725826` (12 digits, an AWS-owned account id, not the caller's account), i.e. aws:CalledVia = ["556613725826"]
- Bool aws:ViaAWSService true -> matches
- The KMS action SSM calls for PutParameter SecureString is kms:Encrypt; refusal code `AccessDeniedException`, message starts "User: arn:aws:sts::<acct>:assumed-role/<role>/probe is not authorized to perform: kms:Encrypt on resource: arn:aws:kms:us-east-1:<acct>:key/<id>"

## Not recorded
Secrets Manager (billed per secret), DynamoDB (AWS docs' own CalledVia example uses dynamodb.amazonaws.com), other regions (SSM's account id is likely region-specific).
