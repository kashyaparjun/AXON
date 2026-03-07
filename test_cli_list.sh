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

ARCHIVE="$TMP_DIR/list.axon"
A="$TMP_DIR/a.txt"
B="$TMP_DIR/b.txt"
C="$TMP_DIR/c.txt"
printf 'a' >"$A"
printf 'b' >"$B"
printf 'c' >"$C"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/a.txt "$A" >/dev/null
"$BIN" add "$ARCHIVE" docs/b.txt "$B" >/dev/null
"$BIN" add "$ARCHIVE" src/main.rs "$C" >/dev/null
"$BIN" remove "$ARCHIVE" docs/b.txt >/dev/null

LIST_ACTIVE="$("$BIN" list "$ARCHIVE" --prefix docs/)"
if ! grep -q '"count":1' <<<"$LIST_ACTIVE"; then
  echo "FAIL: expected active list count=1"
  echo "$LIST_ACTIVE"
  exit 1
fi
if ! grep -q '"path":"docs/a.txt"' <<<"$LIST_ACTIVE"; then
  echo "FAIL: expected docs/a.txt in active list"
  echo "$LIST_ACTIVE"
  exit 1
fi
if grep -q '"path":"docs/b.txt"' <<<"$LIST_ACTIVE"; then
  echo "FAIL: did not expect tombstoned docs/b.txt in active list"
  echo "$LIST_ACTIVE"
  exit 1
fi

LIST_ALL="$("$BIN" list "$ARCHIVE" --prefix docs/ --include-tombstoned)"
if ! grep -q '"count":2' <<<"$LIST_ALL"; then
  echo "FAIL: expected include-tombstoned list count=2"
  echo "$LIST_ALL"
  exit 1
fi

LIST_PAGED="$("$BIN" list "$ARCHIVE" --include-tombstoned --offset 1 --limit 1)"
if ! grep -q '"count":1' <<<"$LIST_PAGED"; then
  echo "FAIL: expected paged list count=1"
  echo "$LIST_PAGED"
  exit 1
fi

echo "PASS: CLI list tests"
