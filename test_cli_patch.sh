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

ARCHIVE="$TMP_DIR/patch.axon"
V1="$TMP_DIR/v1.txt"
V2="$TMP_DIR/v2.txt"
OUT="$TMP_DIR/out.txt"
MISS="$TMP_DIR/missing.err"
MISS_REMOVE="$TMP_DIR/missing-remove.err"
REMOVED_READ="$TMP_DIR/removed-read.err"
CONFLICT_PATCH="$TMP_DIR/conflict-patch.err"
CONFLICT_REMOVE="$TMP_DIR/conflict-remove.err"

printf 'old-value' >"$V1"
printf 'new-value' >"$V2"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" config/app.env "$V1" >/dev/null
"$BIN" patch "$ARCHIVE" config/app.env "$V2" >/dev/null
"$BIN" read "$ARCHIVE" config/app.env -o "$OUT" >/dev/null

if ! cmp -s "$V2" "$OUT"; then
  echo "FAIL: patched content mismatch"
  exit 1
fi

PEEK_JSON="$("$BIN" peek "$ARCHIVE")"
if ! grep -q '"version":2' <<<"$PEEK_JSON"; then
  echo "FAIL: expected version=2 after one patch"
  echo "$PEEK_JSON"
  exit 1
fi

if ! grep -q '"history_block_ids":\["' <<<"$PEEK_JSON"; then
  echo "FAIL: expected non-empty history_block_ids after patch"
  echo "$PEEK_JSON"
  exit 1
fi

if "$BIN" patch "$ARCHIVE" config/app.env "$V1" --expected-version 1 >/dev/null 2>"$CONFLICT_PATCH"; then
  echo "FAIL: patch with stale expected-version unexpectedly succeeded"
  exit 1
fi

if ! grep -qi "conflict" "$CONFLICT_PATCH"; then
  echo "FAIL: stale expected-version patch did not emit expected conflict"
  cat "$CONFLICT_PATCH"
  exit 1
fi

if "$BIN" remove "$ARCHIVE" config/app.env --expected-version 1 >/dev/null 2>"$CONFLICT_REMOVE"; then
  echo "FAIL: remove with stale expected-version unexpectedly succeeded"
  exit 1
fi

if ! grep -qi "conflict" "$CONFLICT_REMOVE"; then
  echo "FAIL: stale expected-version remove did not emit expected conflict"
  cat "$CONFLICT_REMOVE"
  exit 1
fi

"$BIN" remove "$ARCHIVE" config/app.env >/dev/null

PEEK_AFTER_REMOVE="$("$BIN" peek "$ARCHIVE")"
if ! grep -q '"tombstoned":true' <<<"$PEEK_AFTER_REMOVE"; then
  echo "FAIL: expected tombstoned=true after remove"
  echo "$PEEK_AFTER_REMOVE"
  exit 1
fi

if "$BIN" read "$ARCHIVE" config/app.env -o "$OUT" >/dev/null 2>"$REMOVED_READ"; then
  echo "FAIL: reading removed file unexpectedly succeeded"
  exit 1
fi

if ! grep -qi "not found" "$REMOVED_READ"; then
  echo "FAIL: removed-file read did not emit expected error"
  cat "$REMOVED_READ"
  exit 1
fi

if "$BIN" patch "$ARCHIVE" missing/file.txt "$V2" >/dev/null 2>"$MISS"; then
  echo "FAIL: patching missing file unexpectedly succeeded"
  exit 1
fi

if ! grep -qi "not found" "$MISS"; then
  echo "FAIL: missing-file patch did not emit expected error"
  cat "$MISS"
  exit 1
fi

if "$BIN" remove "$ARCHIVE" missing/file.txt >/dev/null 2>"$MISS_REMOVE"; then
  echo "FAIL: removing missing file unexpectedly succeeded"
  exit 1
fi

if ! grep -qi "not found" "$MISS_REMOVE"; then
  echo "FAIL: missing-file remove did not emit expected error"
  cat "$MISS_REMOVE"
  exit 1
fi

echo "PASS: CLI patch tests"
