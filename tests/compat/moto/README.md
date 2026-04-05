# Moto Compatibility Tests

Run [Moto](https://github.com/getmoto/moto)'s full test suite against FakeCloud to measure API compatibility.

## Quick Start

```bash
# Run all moto tests (takes a while)
./tests/compat/moto/run.sh

# Run only our implemented services
./tests/compat/moto/run.sh --implemented

# Run specific services
./tests/compat/moto/run.sh sqs sns
```

## How It Works

1. **setup.sh** clones moto and creates a Python venv with all dependencies
2. **run.sh** builds FakeCloud, starts the server, and runs moto's pytest suite
3. Tests use moto's `TEST_SERVER_MODE` which patches `boto3.client()` and `boto3.resource()` to point at `http://localhost:4566` instead of mocking internally
4. **report.py** parses per-service log files and generates `RESULTS.md`

### Key Environment Variables

| Variable | Value | Purpose |
|----------|-------|---------|
| `TEST_SERVER_MODE` | `true` | Makes `@mock_aws` patch boto3 to hit a real server |
| `TEST_SERVER_MODE_ENDPOINT` | `http://localhost:4566` | The server URL |
| `MOTO_CALL_RESET_API` | `false` | Disables moto's `/moto-api/reset` call (FakeCloud doesn't have it) |
| `AWS_DEFAULT_REGION` | `us-east-1` | Default region for tests |
| `AWS_ACCESS_KEY_ID` | `testing` | Dummy credentials |
| `AWS_SECRET_ACCESS_KEY` | `testing` | Dummy credentials |

## Interpreting Results

The `RESULTS.md` file contains a table with per-service results:

- **Passed**: Tests that ran successfully against FakeCloud
- **Failed**: Tests that got unexpected responses (missing actions, wrong formats, etc.)
- **Errors**: Tests that crashed (connection errors, import errors, etc.)
- **Skipped**: Tests skipped by pytest markers
- **Pass Rate**: passed / total

For services FakeCloud doesn't implement at all, expect 0% pass rate with mostly errors or failures. This is the baseline for tracking progress.

Detailed per-service logs are saved in `results/{service}.log`.

## File Structure

```
tests/compat/moto/
  setup.sh          # One-time setup (clone moto, create venv)
  run.sh            # Main runner script
  conftest.py       # Pytest conftest (merged into moto's test dir)
  report.py         # Report generator
  README.md         # This file
  RESULTS.md        # Generated compatibility report
  results/          # Per-service log files
    sqs.log
    sns.log
    ...
```

## Adding New Services

When you implement a new AWS service in FakeCloud:

1. Run just that service: `./tests/compat/moto/run.sh <service>`
2. Check `results/<service>.log` for specific test failures
3. Use failure details to guide your implementation
4. Re-run to track progress

## Customization

- **Moto location**: Set `MOTO_DIR` to use a different moto checkout
- **Per-test timeout**: Edit the `--timeout=30` flag in `run.sh`

## Troubleshooting

- **Server won't start**: Check if port 4566 is already in use
- **Import errors**: Run `./tests/compat/moto/setup.sh` to reinstall dependencies
- **Hanging tests**: The 30-second timeout should catch these; check the log files
