# S3 request refusals recorded against a real account

Recorded in us-east-1 on 2026-11-12 with raw signed (an IAM admin user) and
unsigned requests; RequestId/HostId and probe bucket names elided. The unit and
e2e tests that pin these behaviours cite this file.

## 400 InvalidArgument: conflicting resource-type query parameters

Signed and anonymous requests get the same answer, so the check runs before
authorization (an anonymous request with a single sub-resource, e.g. `GET /b?cors`,
gets 403 AccessDenied "Access Denied" instead). A request signed with an unknown
access key gets the same 400, so the check also precedes key validation.

Format: `<Error><Code>InvalidArgument</Code><Message>Conflicting query string parameters: <names sorted, ", "-joined></Message><ArgumentName>ResourceType</ArgumentName><ArgumentValue><first sorted name></ArgumentValue>...</Error>`

- `GET /b?versions&cors`, `GET /b?cors&versions` -> "cors, versions", ArgumentValue `cors`
- `GET /b?versions&object-lock` -> "object-lock, versions"
- `GET /b?location&versions` -> "location, versions"
- `PUT /b/k?partNumber=x&uploadId=u&tagging` (and `partNumber=1`) -> "partNumber, tagging, uploadId", ArgumentValue `partNumber`. `partNumber` and `uploadId` count as resource types; the pair alone is UploadPart.
- `POST /b/k?restore&uploadId=u` -> "restore, uploadId"
- `DELETE /b?publicAccessBlock&lifecycle` -> "lifecycle, publicAccessBlock"
- `GET /b/k?tagging&acl`, `?acl&tagging` -> "acl, tagging"
- No state changes in any of these.

## 405 MethodNotAllowed

- `PUT /b?location`, `DELETE /b?location` -> 405 `<Code>MethodNotAllowed</Code><Message>The specified method is not allowed against this resource.</Message><Method>PUT|DELETE</Method><ResourceType>LOCATION</ResourceType>`; no bucket created or deleted.
- `PUT /b/k?uploadId=u` (no partNumber) -> 405, `<ResourceType>UPLOAD</ResourceType>`.

## Anonymous requests, bucket with CORS rule `{AllowedOrigins:*, AllowedMethods:GET,PUT, AllowedHeaders:*}`, `Origin: https://example.com`

- `GET /b?cors&versions` -> the 400 conflict, with NO `Access-Control-*` / `Vary` headers.
- `GET /b/missing` -> 403 AccessDenied WITH `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods: GET, PUT`, `Vary: Origin, Access-Control-Request-Headers, Access-Control-Request-Method`.
- `PUT /b/k?uploadId=u` -> the 405 above, WITH the CORS headers.
- `PUT /b/k?partNumber=1` -> 400 InvalidArgument "This operation does not accept partNumber without uploadId", `<ArgumentName>partNumber</ArgumentName><ArgumentValue>partNumber</ArgumentValue>`, WITH the CORS headers. (A request signed with an unknown key gets the same 400.)
- `GET /b/k?uploadId=u`, `PUT /b/k?partNumber=0&uploadId=u`, `PUT /b/k?partNumber=x&uploadId=u` (no such upload) -> 404 NoSuchUpload "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed." `<UploadId>u</UploadId>`, WITH the CORS headers: the upload lookup precedes authorization and part-number validation.

## Broken aws-chunked framing (anonymous; body `zz;garbage\r\nabc\r\n`, `Content-Encoding: aws-chunked`, `x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER`, `x-amz-decoded-content-length: 3`)

- `PUT /<missing-bucket>/k`, `/<missing-bucket>/k?tagging` -> 404 NoSuchBucket "The specified bucket does not exist" (bucket existence first).
- `PUT /<existing bucket>/k` -> 403 AccessDenied "Access Denied" (authorization before framing).
- `PUT /<existing bucket>/k?tagging` -> 400 InvalidRequest "The value of x-amz-content-sha256 header is invalid."

## POST Object (forms from boto3 `generate_presigned_post`, sent raw)

- Policy `{"acl":"private"}`; form `acl=private` plus a second `acl=public-read` or `Acl=public-read` -> 403 AccessDenied "Invalid according to Policy: Policy Condition failed: [\"eq\", \"$acl\", \"private\"]"; nothing stored. The single-field form -> 204.
- Policy `{"x-amz-server-side-encryption":"AES256"}`; form adds `X-Amz-Server-Side-Encryption=aws:kms` -> 403 "Invalid according to Policy: Policy Condition failed: [\"eq\", \"$x-amz-server-side-encryption\", \"AES256\"]". Every occurrence of a field (case-insensitive) is checked.
- `key` only after the file field -> 400 `<Code>InvalidArgument</Code><Message>Bucket POST must contain a field named 'key'.  If it is specified, please check the order of the fields.</Message><ArgumentName>key</ArgumentName><ArgumentValue></ArgumentValue>`.
- A plain CopyObject with only `s3:GetObject` + `s3:PutObject` (source without annotations) is allowed.

## Tag sets

- POST `tagging` with a commented-out `<Tag>` and a keyless `<Tag>` -> 400 MalformedXML "The XML you provided was not well-formed or did not validate against our published schema"; with a valid tag plus a commented-out one -> 204, stored exactly the valid tag.
- Anonymous form with malformed `tagging` -> 400 MalformedXML; with valid `tagging` -> 403 AccessDenied (validation before authorization).
- PutObjectTagging: duplicate keys -> 400 InvalidTag "Cannot provide multiple Tags with the same key"; truncated XML or keyless `<Tag>` -> 400 MalformedXML; key `aws:x` -> 400 InvalidTag "Your TagKey cannot be prefixed with aws:".
- Empty or self-closing `<Key/>`, a 129-character key, or key `a*b` -> 400 InvalidTag "The TagKey you have provided is invalid"; a 128-character key and an empty value are accepted; a 257-character value -> 400 InvalidTag "The TagValue you have provided is invalid"; `<Tag >` and CDATA keys are ordinary XML.
- PutObject `x-amz-tagging`: 129-character key / 257-character value -> the same InvalidTag messages; `=1` or `a=1&a=2` -> 400 InvalidArgument "The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates."

## PutBucketWebsite (all 400 InvalidArgument)

- IndexDocument Suffix `a/b` or empty -> "The IndexDocument Suffix is not well formed"
- ErrorDocument without IndexDocument or RedirectAllRequestsTo -> "A value for IndexDocument Suffix must be provided if RedirectAllRequestsTo is empty"
- RedirectAllRequestsTo with IndexDocument -> "RedirectAllRequestsTo cannot be provided in conjunction with other Routing Rules."
