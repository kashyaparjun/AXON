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

ARCHIVE="$TMP_DIR/wal.axon"
A="$TMP_DIR/a.txt"
B="$TMP_DIR/b.txt"
printf 'a' >"$A"
printf 'b' >"$B"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/a.txt "$A" >/dev/null
"$BIN" patch "$ARCHIVE" docs/a.txt "$B" >/dev/null
"$BIN" remove "$ARCHIVE" docs/a.txt >/dev/null

WAL_JSON="$("$BIN" wal "$ARCHIVE" --status)"
if ! grep -q '"entry_count":3' <<<"$WAL_JSON"; then
  echo "FAIL: expected wal entry_count=3 after add/patch/remove"
  echo "$WAL_JSON"
  exit 1
fi

if ! grep -q '"wal_size":' <<<"$WAL_JSON"; then
  echo "FAIL: expected wal_size in wal status output"
  echo "$WAL_JSON"
  exit 1
fi

echo "PASS: CLI wal tests"
