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

ARCHIVE="$TMP_DIR/contract.axon"
ARCHIVE_GC_A="$TMP_DIR/contract-gc-a.axon"
ARCHIVE_GC_B="$TMP_DIR/contract-gc-b.axon"
PLAN_EMPTY="$TMP_DIR/plan-empty.json"
A="$TMP_DIR/a.txt"
B="$TMP_DIR/b.txt"
ERR="$TMP_DIR/err.json"
OUT="$TMP_DIR/out.txt"

printf 'value-a' >"$A"
printf 'value-b' >"$B"
printf '{"mutations":[]}' >"$PLAN_EMPTY"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/a.txt "$A" >/dev/null
"$BIN" patch "$ARCHIVE" docs/a.txt "$B" >/dev/null

compact_json() {
  tr -d '[:space:]' <<<"$1"
}

assert_pretty_parity() {
  local name="$1"
  shift
  local compact pretty
  compact="$("$BIN" "$@")"
  pretty="$("$BIN" "$@" --pretty)"
  if [[ "$(compact_json "$compact")" != "$(compact_json "$pretty")" ]]; then
    echo "FAIL: pretty/json parity mismatch for $name"
    echo "compact=$compact"
    echo "pretty=$pretty"
    exit 1
  fi
}

assert_pretty_parity "info" info "$ARCHIVE"
assert_pretty_parity "peek" peek "$ARCHIVE"
assert_pretty_parity "search" search "$ARCHIVE" docs/
assert_pretty_parity "list" list "$ARCHIVE" --prefix docs/
assert_pretty_parity "wal-status" wal "$ARCHIVE" --status
assert_pretty_parity "verify" verify "$ARCHIVE"
assert_pretty_parity "log" log "$ARCHIVE" docs/a.txt
assert_pretty_parity "batch-empty" batch "$ARCHIVE" "$PLAN_EMPTY"

cp "$ARCHIVE" "$ARCHIVE_GC_A"
cp "$ARCHIVE" "$ARCHIVE_GC_B"
GC_COMPACT="$("$BIN" gc "$ARCHIVE_GC_A")"
GC_PRETTY="$("$BIN" gc "$ARCHIVE_GC_B" --pretty)"
if [[ "$(compact_json "$GC_COMPACT")" != "$(compact_json "$GC_PRETTY")" ]]; then
  echo "FAIL: pretty/json parity mismatch for gc"
  echo "$GC_COMPACT"
  echo "$GC_PRETTY"
  exit 1
fi

set +e
"$BIN" patch "$ARCHIVE" docs/a.txt "$A" --expected-version 1 >/dev/null 2>"$ERR"
STATUS_PATCH=$?
set -e
if [[ $STATUS_PATCH -ne 24 ]]; then
  echo "FAIL: expected patch conflict exit code 24, got $STATUS_PATCH"
  cat "$ERR"
  exit 1
fi
if ! grep -q '"code":"ERR_CONFLICT"' "$ERR"; then
  echo "FAIL: expected ERR_CONFLICT code for stale expected-version patch"
  cat "$ERR"
  exit 1
fi
if ! grep -q '"exit_code":24' "$ERR"; then
  echo "FAIL: expected exit_code=24 in conflict payload"
  cat "$ERR"
  exit 1
fi

set +e
"$BIN" remove "$ARCHIVE" docs/a.txt --expected-version 1 >/dev/null 2>"$ERR"
STATUS_REMOVE=$?
set -e
if [[ $STATUS_REMOVE -ne 24 ]]; then
  echo "FAIL: expected remove conflict exit code 24, got $STATUS_REMOVE"
  cat "$ERR"
  exit 1
fi
if ! grep -q '"code":"ERR_CONFLICT"' "$ERR"; then
  echo "FAIL: expected ERR_CONFLICT code for stale expected-version remove"
  cat "$ERR"
  exit 1
fi

set +e
"$BIN" read "$ARCHIVE" missing.txt -o "$OUT" >/dev/null 2>"$ERR"
STATUS_NOT_FOUND=$?
set -e
if [[ $STATUS_NOT_FOUND -ne 23 ]]; then
  echo "FAIL: expected read missing exit code 23, got $STATUS_NOT_FOUND"
  cat "$ERR"
  exit 1
fi
if ! grep -q '"code":"ERR_NOT_FOUND"' "$ERR"; then
  echo "FAIL: expected ERR_NOT_FOUND code for missing read"
  cat "$ERR"
  exit 1
fi

echo "PASS: CLI contract tests"
