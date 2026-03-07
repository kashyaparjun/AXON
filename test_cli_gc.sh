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

ARCHIVE="$TMP_DIR/gc.axon"
A="$TMP_DIR/a.txt"
B="$TMP_DIR/b.txt"
OUT="$TMP_DIR/out.txt"
printf 'a' >"$A"
printf 'b' >"$B"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/a.txt "$A" >/dev/null
"$BIN" patch "$ARCHIVE" docs/a.txt "$B" >/dev/null

BEFORE="$("$BIN" wal "$ARCHIVE" --status)"
if ! grep -Eq '"entry_count":[2-9]' <<<"$BEFORE"; then
  echo "FAIL: expected wal entry_count >= 2 before gc"
  echo "$BEFORE"
  exit 1
fi

GC_JSON="$("$BIN" gc "$ARCHIVE")"
if ! grep -q '"ok":true' <<<"$GC_JSON"; then
  echo "FAIL: expected gc ok=true"
  echo "$GC_JSON"
  exit 1
fi

AFTER="$("$BIN" wal "$ARCHIVE" --status)"
if ! grep -q '"entry_count":0' <<<"$AFTER"; then
  echo "FAIL: expected wal entry_count=0 after gc"
  echo "$AFTER"
  exit 1
fi

"$BIN" read "$ARCHIVE" docs/a.txt -o "$OUT" >/dev/null
if ! cmp -s "$B" "$OUT"; then
  echo "FAIL: expected read content preserved after gc"
  exit 1
fi

echo "PASS: CLI gc tests"
