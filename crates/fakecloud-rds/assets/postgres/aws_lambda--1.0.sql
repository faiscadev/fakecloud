-- aws_lambda extension v1.0 (fakecloud)
-- Calls fakecloud Lambda invocations through a host bridge endpoint.

\echo Use "CREATE EXTENSION aws_lambda CASCADE" to load this file. \quit

CREATE FUNCTION aws_lambda.invoke(
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
try:
    with urllib.request.urlopen(req, timeout=300) as resp:
        raw = resp.read()
        http_status = resp.status
except urllib.error.HTTPError as e:
    raw = e.read()
    http_status = e.code
except Exception as e:
    plpy.error('aws_lambda: bridge call failed: {}'.format(e))

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
)]
$$;

-- LANGUAGE SQL keeps the user-facing arg name `payload` even though it
-- also names a RETURNS TABLE column. PL/pgSQL would reject that as a
-- duplicate identifier in the function namespace; SQL doesn't.
CREATE FUNCTION aws_lambda.invoke(
    function_name aws_commons._lambda_function_arn_1,
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
    SELECT * FROM aws_lambda.invoke(
        (function_name).function_name,
        payload,
        COALESCE(region, (function_name).region),
        invocation_type
    );
$$;
