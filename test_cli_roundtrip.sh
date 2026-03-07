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

ARCHIVE="$TMP_DIR/roundtrip.axon"
INPUT="$TMP_DIR/input.txt"
OUTPUT="$TMP_DIR/output.txt"
printf 'hello-from-cli-roundtrip' >"$INPUT"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/readme.txt "$INPUT" >/dev/null
"$BIN" read "$ARCHIVE" docs/readme.txt -o "$OUTPUT" >/dev/null

if ! cmp -s "$INPUT" "$OUTPUT"; then
  echo "FAIL: read output did not match input bytes"
  exit 1
fi

INFO_JSON="$("$BIN" info "$ARCHIVE")"
if ! grep -q '"total_files":1' <<<"$INFO_JSON"; then
  echo "FAIL: info output missing expected total_files=1"
  echo "$INFO_JSON"
  exit 1
fi

PEEK_JSON="$("$BIN" peek "$ARCHIVE")"
if ! grep -q '"path":"docs/readme.txt"' <<<"$PEEK_JSON"; then
  echo "FAIL: peek output missing added file path"
  echo "$PEEK_JSON"
  exit 1
fi

DUP_ERR="$TMP_DIR/dup.err"
if "$BIN" add "$ARCHIVE" docs/readme.txt "$INPUT" >/dev/null 2>"$DUP_ERR"; then
  echo "FAIL: duplicate add unexpectedly succeeded"
  exit 1
fi

if ! grep -qi "entry already exists" "$DUP_ERR"; then
  echo "FAIL: duplicate add did not emit expected error"
  cat "$DUP_ERR"
  exit 1
fi

echo "PASS: CLI roundtrip tests"
