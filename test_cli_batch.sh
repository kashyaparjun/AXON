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

ARCHIVE="$TMP_DIR/batch.axon"
A="$TMP_DIR/a.txt"
B="$TMP_DIR/b.txt"
PLAN_OK="$TMP_DIR/plan-ok.json"
PLAN_BAD="$TMP_DIR/plan-bad.json"
ERR="$TMP_DIR/batch.err"
OUT="$TMP_DIR/out.txt"

printf 'a' >"$A"
printf 'b' >"$B"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/a.txt "$A" >/dev/null

cat >"$PLAN_OK" <<JSON
{"mutations":[
  {"op":"patch","path":"docs/a.txt","source":"$B","expected_version":1},
  {"op":"add","path":"docs/new.txt","source":"$B"},
  {"op":"remove","path":"docs/a.txt","expected_version":2}
]}
JSON

"$BIN" batch "$ARCHIVE" "$PLAN_OK" >/dev/null
"$BIN" read "$ARCHIVE" docs/new.txt -o "$OUT" >/dev/null
if ! cmp -s "$B" "$OUT"; then
  echo "FAIL: batch add/patch output mismatch"
  exit 1
fi

cat >"$PLAN_BAD" <<JSON
{"mutations":[
  {"op":"patch","path":"docs/new.txt","source":"$A","expected_version":99},
  {"op":"add","path":"docs/never.txt","source":"$A"}
]}
JSON

if "$BIN" batch "$ARCHIVE" "$PLAN_BAD" >/dev/null 2>"$ERR"; then
  echo "FAIL: conflicting batch unexpectedly succeeded"
  exit 1
fi

if ! grep -qi "conflict" "$ERR"; then
  echo "FAIL: conflicting batch did not emit conflict"
  cat "$ERR"
  exit 1
fi

if "$BIN" read "$ARCHIVE" docs/never.txt -o "$OUT" >/dev/null 2>/dev/null; then
  echo "FAIL: failed batch wrote partial changes"
  exit 1
fi

echo "PASS: CLI batch tests"
