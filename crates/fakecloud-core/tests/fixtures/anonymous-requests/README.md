# Real AWS (us-east-1, 2026-11-12/13): unsigned (anonymous) requests to non-S3 services

No Authorization header, no presign query, no session token. RequestIds elided.

## Query protocol
- STS `Action=AssumeRoot&Version=2011-06-15&TargetPrincipal=111122223333&TaskPolicyArn.arn=bogus` (invalid TaskPolicyArn) -> HTTP 403
  `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>MissingAuthenticationToken</Code><Message>Request is missing Authentication Token</Message></Error>...</ErrorResponse>`
  => anonymous refusal comes BEFORE TaskPolicyArn validation.
- STS `Action=GetCallerIdentity` -> HTTP 403, same MissingAuthenticationToken body.
- STS with a Query body but Content-Type text/plain or none -> HTTP 302 (not processed as an API call); with application/x-www-form-urlencoded -> processed.
- IAM `Action=ListUsers&Version=2010-05-08` (iam.amazonaws.com) -> HTTP 403 `<ErrorResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><Error><Type>Sender</Type><Code>MissingAuthenticationToken</Code><Message>Request is missing Authentication Token</Message></Error>`
- SNS `Action=ListTopics&Version=2010-03-31` -> HTTP 403 `<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><Error><Type>Sender</Type><Code>MissingAuthenticationToken</Code><Message>Request is missing Authentication Token</Message></Error>`
- SQS `Action=ListQueues&Version=2012-11-05` -> HTTP 403 `<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>Access to the resource https://sqs.us-east-1.amazonaws.com/ is denied.</Message><Detail/></Error><RequestId>..</RequestId></ErrorResponse>` (SQS supports anonymous access through queue policies, so it authorizes rather than refusing for a missing token)
  - Queue operations on an existing queue with no policy name the LOWERCASED action and the queue ARN, not a URL. `ReceiveMessage` -> HTTP 403 `<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>User: anonymous is not authorized to perform: sqs:receivemessage on resource: arn:aws:sqs:us-east-1:<acct>:<queue> because no resource-based policy allows the sqs:receivemessage action</Message><Detail/></Error>...</ErrorResponse>`. Same pattern for `SendMessage` (`sqs:sendmessage`) and `GetQueueAttributes` (`sqs:getqueueattributes`). Only the service-level `ListQueues` uses "Access to the resource https://sqs.<region>.amazonaws.com/ is denied.".
- EC2 `Action=DescribeRegions&Version=2016-11-15` -> HTTP 400 `<Response><Errors><Error><Code>MissingParameter</Code><Message>The request must contain the parameter AWSAccessKeyId</Message></Error></Errors><RequestID>..</RequestID></Response>`

## JSON protocol
- KMS `TrentService.ListKeys` `{}` -> HTTP 400 `{"__type":"MissingAuthenticationTokenException","message":"Missing Authentication Token"}`
- SSM `AmazonSSM.DescribeParameters` -> HTTP 400 `{"__type":"MissingAuthenticationTokenException","message":"Missing Authentication Token"}`
- CloudWatch Logs `Logs_20140328.DescribeLogGroups` -> HTTP 400 `{"__type":"MissingAuthenticationTokenException","message":"Missing Authentication Token"}`
- DynamoDB `DynamoDB_20120810.ListTables` -> HTTP 400 `{"__type":"com.amazon.coral.service#MissingAuthenticationTokenException","message":"Request is missing Authentication Token"}`
- Secrets Manager `secretsmanager.ListSecrets` -> HTTP 400 `{"__type":"UnrecognizedClientException", "message":"The security token included in the request is invalid."}`
- Cognito User Pools `AWSCognitoIdentityProviderService.InitiateAuth` (an unauthenticated API) -> processed: HTTP 400 `{"__type":"ResourceNotFoundException","message":"User pool client 1example23456789 does not exist."}`

## REST
- Lambda `GET /2015-03-31/functions/` -> HTTP 403 `{"message":"Missing Authentication Token"}`
- Route 53 `GET /2013-04-01/hostedzone` -> HTTP 403 `<?xml version="1.0"?><ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Error><Type>Sender</Type><Code>MissingAuthenticationToken</Code><Message>Request is missing Authentication Token</Message></Error></ErrorResponse>`
