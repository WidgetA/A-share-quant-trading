#!/bin/bash

# === MODULE PURPOSE ===
# Quick dependency checker for THS SDK.
# Run this to diagnose missing library issues without full installation.

# === USAGE ===
# chmod +x scripts/check_ths_deps.sh
# ./scripts/check_ths_deps.sh /path/to/sdk

set -e

SDK_PATH="${1:-/opt/ths_sdk}"

# Detect architecture
if [[ "$(uname -m)" == "x86_64" ]]; then
    BIN_DIR="bin64"
else
    BIN_DIR="bin"
fi

echo "Checking THS SDK dependencies..."
echo "SDK Path: $SDK_PATH"
echo "Binary Dir: $BIN_DIR"
echo ""

check_lib() {
    local lib="$1"
    if [[ -f "$lib" ]]; then
        echo "=== $(basename "$lib") ==="
        ldd "$lib" 2>&1
        echo ""
    else
        echo "=== $(basename "$lib") - NOT FOUND ==="
        echo ""
    fi
}

check_lib "$SDK_PATH/$BIN_DIR/libShellExport.so"
check_lib "$SDK_PATH/$BIN_DIR/libFTDataInterface.so"
check_lib "$SDK_PATH/$BIN_DIR/hqdatafeed"

echo "=== Summary ==="
echo "Missing dependencies (if any):"
for lib in libShellExport.so libFTDataInterface.so hqdatafeed; do
    if [[ -f "$SDK_PATH/$BIN_DIR/$lib" ]]; then
        ldd "$SDK_PATH/$BIN_DIR/$lib" 2>&1 | grep "not found" || true
    fi
done
