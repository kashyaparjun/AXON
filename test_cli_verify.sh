#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

BIN="${AXON_BIN:-$ROOT_DIR/target/debug/axon}"
if [[ ! -x "$BIN" ]]; then
  cargo build >/dev/null
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

ARCHIVE="$TMP_DIR/verify.axon"
SRC="$TMP_DIR/input.txt"
printf 'verify-content' >"$SRC"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/readme.txt "$SRC" >/dev/null

VERIFY_JSON="$("$BIN" verify "$ARCHIVE")"
if ! grep -q '"ok":true' <<<"$VERIFY_JSON"; then
  echo "FAIL: expected verify ok=true"
  echo "$VERIFY_JSON"
  exit 1
fi
if ! grep -q '"wal_entry_count":1' <<<"$VERIFY_JSON"; then
  echo "FAIL: expected wal_entry_count=1 after single add"
  echo "$VERIFY_JSON"
  exit 1
fi

echo "PASS: CLI verify tests"
