#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MOTO_DIR="${MOTO_DIR:-$HOME/.cache/fakecloud/moto}"
VENV_DIR="$MOTO_DIR/.venv"

echo "=== Moto Compatibility Test Setup ==="
echo "Moto dir: $MOTO_DIR"
echo "Venv dir: $VENV_DIR"

# Clone moto if not present
if [ -d "$MOTO_DIR/.git" ]; then
    echo "Moto already cloned, updating..."
    git -C "$MOTO_DIR" fetch --depth=1 origin main 2>/dev/null || true
    git -C "$MOTO_DIR" reset --hard origin/main 2>/dev/null || true
else
    echo "Cloning moto..."
    mkdir -p "$(dirname "$MOTO_DIR")"
    # Try /tmp/moto-tests first as a local cache
    if [ -d "/tmp/moto-tests/.git" ]; then
        echo "Using local moto cache from /tmp/moto-tests..."
        cp -a /tmp/moto-tests "$MOTO_DIR"
    else
        git clone --depth=1 https://github.com/getmoto/moto.git "$MOTO_DIR"
    fi
fi

# Create venv if not present
if [ -d "$VENV_DIR" ] && [ -f "$VENV_DIR/bin/python" ]; then
    echo "Venv already exists, skipping creation."
else
    echo "Creating Python venv..."
    python3 -m venv "$VENV_DIR"
fi

echo "Installing moto and test dependencies..."
"$VENV_DIR/bin/pip" install --quiet --upgrade pip
"$VENV_DIR/bin/pip" install --quiet -e "$MOTO_DIR[all,server]" 2>&1 | tail -1
"$VENV_DIR/bin/pip" install --quiet pytest pytest-timeout pytest-xdist requests flask freezegun 2>&1 | tail -1

echo ""
echo "Setup complete."
echo "Moto version: $("$VENV_DIR/bin/python" -c 'import moto; print(moto.__version__)')"
echo "Pytest version: $("$VENV_DIR/bin/python" -m pytest --version 2>&1 | head -1)"
