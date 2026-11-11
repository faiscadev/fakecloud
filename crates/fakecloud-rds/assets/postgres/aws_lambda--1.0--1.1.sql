-- aws_lambda extension upgrade 1.0 -> 1.1 (fakecloud)
-- aws_lambda.invoke raises RDS's "lambda invocation failed" error when the
-- function returns an error.

\echo Use "ALTER EXTENSION aws_lambda UPDATE TO '1.1'" to load this file. \quit

-- The bridge call. Besides the invoke result it reports a function error
-- (`X-Amz-Function-Error`), which `aws_lambda.invoke` turns into the RDS
-- "lambda invocation failed" error.
CREATE FUNCTION aws_lambda._invoke_bridge(
    function_name text,
    payload json,
    region text,
    invocation_type text
) RETURNS TABLE(
    status_code integer,
    payload json,
    executed_version text,
    log_result text,
    function_error text,
    function_arn text,
    error_details text
)
LANGUAGE plpython3u
AS $$
import json
import os
import urllib.request
import urllib.error

endpoint = os.environ.get('FAKECLOUD_ENDPOINT')
if not endpoint:
    plpy.error('aws_lambda: FAKECLOUD_ENDPOINT not set on the database container')

account_id = os.environ.get('FAKECLOUD_ACCOUNT_ID', '000000000000')
default_region = os.environ.get('FAKECLOUD_REGION', 'us-east-1')

body = {
    'function_name': function_name,
    'payload': json.loads(payload) if payload is not None else None,
    'invocation_type': invocation_type,
    'region': region or default_region,
}

req = urllib.request.Request(
    endpoint.rstrip('/') + '/_fakecloud/rds/lambda-invoke',
    data=json.dumps(body).encode('utf-8'),
    headers={
        'Content-Type': 'application/json',
        'X-Fakecloud-Account-Id': account_id,
    },
    method='POST',
)

http_status = None
function_error = None
function_arn = function_name
try:
    with urllib.request.urlopen(req, timeout=300) as resp:
        raw = resp.read()
        http_status = resp.status
        function_error = resp.headers.get('X-Amz-Function-Error')
        function_arn = resp.headers.get('X-Fakecloud-Function-Arn') or function_name
except urllib.error.HTTPError as e:
    raw = e.read()
    http_status = e.code
except Exception as e:
    plpy.error('aws_lambda: bridge call failed: {}'.format(e))

error_details = None
if function_error:
    try:
        error_details = json.dumps(json.loads(raw).get('payload'))
    except (ValueError, AttributeError):
        error_details = raw.decode('utf-8', errors='replace')

try:
    parsed = json.loads(raw)
except ValueError:
    parsed = {
        'status_code': http_status,
        'payload': {'errorMessage': raw.decode('utf-8', errors='replace')},
    }

status_code = parsed.get('status_code')
if status_code is None:
    status_code = http_status if http_status is not None else 0

return [(
    int(status_code),
    json.dumps(parsed.get('payload')) if parsed.get('payload') is not None else None,
    parsed.get('executed_version'),
    parsed.get('log_result'),
    function_error,
    function_arn,
    error_details,
)]
$$;

-- Raise RDS's error for a function that returned an error; true otherwise.
CREATE FUNCTION aws_lambda._check_function_error(
    function_error text,
    function_arn text,
    error_details text
) RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    IF function_error IS NOT NULL THEN
        RAISE EXCEPTION 'lambda invocation failed'
            USING DETAIL = format(
                '"%s" returned error "%s", details: "%s".',
                function_arn, function_error, error_details
            );
    END IF;
    RETURN true;
END;
$$;

-- LANGUAGE SQL keeps the user-facing arg name `payload` even though it
-- also names a RETURNS TABLE column (see the composite overload below).
CREATE OR REPLACE FUNCTION aws_lambda.invoke(
    function_name text,
    payload json,
    region text DEFAULT NULL,
    invocation_type text DEFAULT 'RequestResponse'
) RETURNS TABLE(
    status_code integer,
    payload json,
    executed_version text,
    log_result text
)
LANGUAGE SQL
AS $$
    SELECT r.status_code, r.payload, r.executed_version, r.log_result
    FROM aws_lambda._invoke_bridge($1, $2, $3, $4) r
    WHERE aws_lambda._check_function_error(r.function_error, r.function_arn, r.error_details);
$$;

