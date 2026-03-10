#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

cargo build >/dev/null

bash "$ROOT_DIR/test_cli_roundtrip.sh"
bash "$ROOT_DIR/test_cli_patch.sh"
bash "$ROOT_DIR/test_cli_search.sh"
bash "$ROOT_DIR/test_cli_list.sh"
bash "$ROOT_DIR/test_cli_wal.sh"
bash "$ROOT_DIR/test_cli_log.sh"
bash "$ROOT_DIR/test_cli_verify.sh"
bash "$ROOT_DIR/test_cli_gc.sh"
bash "$ROOT_DIR/test_cli_batch.sh"
bash "$ROOT_DIR/test_cli_contract.sh"

echo "PASS: all CLI bash tests"
