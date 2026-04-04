#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SERVER_BIN="$REPO_ROOT/target/release/fakecloud-server"
MOTO_DIR="${MOTO_DIR:-$HOME/.cache/fakecloud/moto}"
VENV_DIR="$MOTO_DIR/.venv"
RESULTS_DIR="$SCRIPT_DIR/results"
SERVER_PID=""
PORT=4566

QUICK_SERVICES="sqs sns events iam sts ssm"

# --- Argument parsing ---
SERVICES=()
QUICK=false

for arg in "$@"; do
    case "$arg" in
        --quick)
            QUICK=true
            ;;
        --help|-h)
            echo "Usage: $0 [--quick] [service...]"
            echo ""
            echo "Run Moto's test suite against FakeCloud."
            echo ""
            echo "Options:"
            echo "  --quick     Run only implemented services ($QUICK_SERVICES)"
            echo "  service...  Run specific services (e.g., sqs sns)"
            echo "  (none)      Run ALL moto test directories"
            echo ""
            echo "Examples:"
            echo "  $0                # run all services"
            echo "  $0 sqs sns        # run only SQS and SNS"
            echo "  $0 --quick        # run implemented services only"
            exit 0
            ;;
        *)
            SERVICES+=("$arg")
            ;;
    esac
done

if $QUICK; then
    SERVICES=($QUICK_SERVICES)
fi

# --- Cleanup ---
cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo ""
        echo "Stopping FakeCloud server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# --- Setup check ---
if [ ! -f "$VENV_DIR/bin/python" ] || [ ! -d "$MOTO_DIR/tests" ]; then
    echo "Moto not set up. Running setup.sh..."
    bash "$SCRIPT_DIR/setup.sh"
fi

# --- Build FakeCloud ---
echo "=== Building FakeCloud ==="
cd "$REPO_ROOT"
cargo build --release 2>&1

if [ ! -f "$SERVER_BIN" ]; then
    echo "ERROR: Server binary not found at $SERVER_BIN"
    exit 1
fi

# --- Kill any existing FakeCloud on our port ---
existing_pid=$(lsof -ti :"$PORT" 2>/dev/null || true)
if [ -n "$existing_pid" ]; then
    echo "Killing existing process on port $PORT (PID $existing_pid)..."
    kill "$existing_pid" 2>/dev/null || true
    sleep 1
fi

# --- Start FakeCloud ---
mkdir -p "$RESULTS_DIR"

echo ""
echo "=== Starting FakeCloud server on port $PORT ==="
"$SERVER_BIN" --log-level warn >"$RESULTS_DIR/server.log" 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for health check (server returns 400 on GET / which is fine - it means it's up)
for i in $(seq 1 30); do
    if curl -s -o /dev/null "http://localhost:$PORT/" 2>/dev/null; then
        echo "Server is ready."
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Server process died."
        cat "$RESULTS_DIR/server.log"
        exit 1
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Server did not start within 30 seconds."
        exit 1
    fi
    sleep 1
done

# --- Discover services ---
if [ ${#SERVICES[@]} -eq 0 ]; then
    echo ""
    echo "Discovering all moto test directories..."
    SERVICES=()
    for dir in "$MOTO_DIR"/tests/test_*; do
        if [ -d "$dir" ]; then
            svc=$(basename "$dir" | sed 's/^test_//')
            SERVICES+=("$svc")
        fi
    done
fi

echo ""
echo "=== Running tests for ${#SERVICES[@]} services ==="

# --- Set environment for TEST_SERVER_MODE ---
export TEST_SERVER_MODE=true
export TEST_SERVER_MODE_ENDPOINT="http://localhost:$PORT"
export MOTO_CALL_RESET_API=false
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
export AWS_SECURITY_TOKEN=testing
export AWS_SESSION_TOKEN=testing
# Disable docker-dependent features
export SKIP_REQUIRES_DOCKER=true
export MOTO_DOCKER_NETWORK_MODE=""

# Merge our conftest with moto's original conftest
MOTO_CONFTEST="$MOTO_DIR/tests/conftest.py"
MOTO_CONFTEST_ORIG="$MOTO_DIR/tests/conftest.py.orig"
if [ ! -f "$MOTO_CONFTEST_ORIG" ]; then
    cp "$MOTO_CONFTEST" "$MOTO_CONFTEST_ORIG"
fi
# Combine: our conftest first, then moto's original
cat "$SCRIPT_DIR/conftest.py" > "$MOTO_CONFTEST"
echo "" >> "$MOTO_CONFTEST"
echo "# --- Original moto conftest below ---" >> "$MOTO_CONFTEST"
cat "$MOTO_CONFTEST_ORIG" >> "$MOTO_CONFTEST"

# --- Run tests ---
PASSED_TOTAL=0
FAILED_TOTAL=0
SKIPPED_TOTAL=0
ERROR_TOTAL=0
SERVICES_RUN=0

for svc in "${SERVICES[@]}"; do
    test_dir="$MOTO_DIR/tests/test_$svc"
    if [ ! -d "$test_dir" ]; then
        echo "  SKIP $svc (no test directory)"
        continue
    fi

    SERVICES_RUN=$((SERVICES_RUN + 1))
    log_file="$RESULTS_DIR/${svc}.log"

    printf "  %-35s " "$svc"

    # Run pytest from the moto tests directory so conftest.py is picked up
    (cd "$MOTO_DIR/tests" && "$VENV_DIR/bin/python" -m pytest \
        "test_$svc/" \
        --timeout=30 \
        -q \
        --tb=short \
        --no-header \
        --ignore-glob="**/test_server.py" \
        2>&1) > "$log_file" || true

    # Parse results from log (strip ANSI codes)
    summary=$(tail -5 "$log_file" | sed 's/\x1b\[[0-9;]*m//g' | grep -E '(passed|failed|error|skipped|no tests ran)' | tail -1 || echo "")

    passed=$(echo "$summary" | grep -oE '[0-9]+ passed' | grep -oE '[0-9]+' || echo 0)
    failed=$(echo "$summary" | grep -oE '[0-9]+ failed' | grep -oE '[0-9]+' || echo 0)
    skipped=$(echo "$summary" | grep -oE '[0-9]+ skipped' | grep -oE '[0-9]+' || echo 0)
    errors=$(echo "$summary" | grep -oE '[0-9]+ errors?' | grep -oE '[0-9]+' || echo 0)

    [ -z "$passed" ] && passed=0
    [ -z "$failed" ] && failed=0
    [ -z "$skipped" ] && skipped=0
    [ -z "$errors" ] && errors=0

    total=$((passed + failed + skipped + errors))

    if [ "$total" -gt 0 ]; then
        rate=$((passed * 100 / total))
        echo "${passed}/${total} passed (${rate}%) | ${failed} failed, ${errors} errors, ${skipped} skipped"
    else
        echo "no tests collected"
    fi

    PASSED_TOTAL=$((PASSED_TOTAL + passed))
    FAILED_TOTAL=$((FAILED_TOTAL + failed))
    SKIPPED_TOTAL=$((SKIPPED_TOTAL + skipped))
    ERROR_TOTAL=$((ERROR_TOTAL + errors))
done

echo ""
echo "=== Summary ==="
GRAND_TOTAL=$((PASSED_TOTAL + FAILED_TOTAL + SKIPPED_TOTAL + ERROR_TOTAL))
if [ "$GRAND_TOTAL" -gt 0 ]; then
    RATE=$((PASSED_TOTAL * 100 / GRAND_TOTAL))
    echo "$PASSED_TOTAL passed / $GRAND_TOTAL total ($RATE%) across $SERVICES_RUN services"
    echo "$FAILED_TOTAL failed, $ERROR_TOTAL errors, $SKIPPED_TOTAL skipped"
else
    echo "No tests were executed."
fi

# --- Generate report ---
echo ""
echo "=== Generating report ==="
"$VENV_DIR/bin/python" "$SCRIPT_DIR/report.py"

echo ""
echo "Done. Results in $RESULTS_DIR/"
echo "Report at $SCRIPT_DIR/RESULTS.md"
