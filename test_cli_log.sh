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

ARCHIVE="$TMP_DIR/log.axon"
V1="$TMP_DIR/v1.txt"
V2="$TMP_DIR/v2.txt"
V3="$TMP_DIR/v3.txt"
OUT="$TMP_DIR/out.txt"
LATEST_ERR="$TMP_DIR/latest.err"
TOMBSTONE_ERR="$TMP_DIR/tombstone.err"

printf 'value-v1' >"$V1"
printf 'value-v2' >"$V2"
printf 'value-v3' >"$V3"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/a.txt "$V1" >/dev/null
"$BIN" patch "$ARCHIVE" docs/a.txt "$V2" >/dev/null
"$BIN" patch "$ARCHIVE" docs/a.txt "$V3" >/dev/null
"$BIN" remove "$ARCHIVE" docs/a.txt >/dev/null

"$BIN" read "$ARCHIVE" docs/a.txt --version 1 -o "$OUT" >/dev/null
if ! cmp -s "$V1" "$OUT"; then
  echo "FAIL: version=1 read mismatch"
  exit 1
fi

"$BIN" read "$ARCHIVE" docs/a.txt --version 2 -o "$OUT" >/dev/null
if ! cmp -s "$V2" "$OUT"; then
  echo "FAIL: version=2 read mismatch"
  exit 1
fi

"$BIN" read "$ARCHIVE" docs/a.txt --version 3 -o "$OUT" >/dev/null
if ! cmp -s "$V3" "$OUT"; then
  echo "FAIL: version=3 read mismatch"
  exit 1
fi

if "$BIN" read "$ARCHIVE" docs/a.txt -o "$OUT" >/dev/null 2>"$LATEST_ERR"; then
  echo "FAIL: latest read unexpectedly succeeded for tombstoned file"
  exit 1
fi
if ! grep -qi "not found" "$LATEST_ERR"; then
  echo "FAIL: latest tombstoned read did not emit expected error"
  cat "$LATEST_ERR"
  exit 1
fi

if "$BIN" read "$ARCHIVE" docs/a.txt --version 4 -o "$OUT" >/dev/null 2>"$TOMBSTONE_ERR"; then
  echo "FAIL: tombstoned version read unexpectedly succeeded"
  exit 1
fi
if ! grep -qi "not found" "$TOMBSTONE_ERR"; then
  echo "FAIL: tombstoned version read did not emit expected error"
  cat "$TOMBSTONE_ERR"
  exit 1
fi

LOG_JSON="$("$BIN" log "$ARCHIVE" docs/a.txt)"
if ! grep -q '"current_version":4' <<<"$LOG_JSON"; then
  echo "FAIL: log missing current_version=4"
  echo "$LOG_JSON"
  exit 1
fi
if ! grep -q '"tombstoned":true' <<<"$LOG_JSON"; then
  echo "FAIL: log missing tombstoned=true"
  echo "$LOG_JSON"
  exit 1
fi
if ! grep -q '"version":1' <<<"$LOG_JSON"; then
  echo "FAIL: log missing version 1 record"
  echo "$LOG_JSON"
  exit 1
fi
if ! grep -q '"version":4' <<<"$LOG_JSON"; then
  echo "FAIL: log missing version 4 record"
  echo "$LOG_JSON"
  exit 1
fi

echo "PASS: CLI log/version tests"
