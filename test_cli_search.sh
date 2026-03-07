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

ARCHIVE="$TMP_DIR/search.axon"
A="$TMP_DIR/a.txt"
B="$TMP_DIR/b.txt"
printf 'a' >"$A"
printf 'b' >"$B"

"$BIN" init "$ARCHIVE" --force >/dev/null
"$BIN" add "$ARCHIVE" docs/a.txt "$A" >/dev/null
"$BIN" add "$ARCHIVE" docs/b.txt "$B" >/dev/null
"$BIN" remove "$ARCHIVE" docs/b.txt >/dev/null

SEARCH_ACTIVE="$("$BIN" search "$ARCHIVE" docs/)"
if ! grep -q '"count":1' <<<"$SEARCH_ACTIVE"; then
  echo "FAIL: expected active search count=1"
  echo "$SEARCH_ACTIVE"
  exit 1
fi
if ! grep -q '"path":"docs/a.txt"' <<<"$SEARCH_ACTIVE"; then
  echo "FAIL: expected docs/a.txt in active search"
  echo "$SEARCH_ACTIVE"
  exit 1
fi
if grep -q '"path":"docs/b.txt"' <<<"$SEARCH_ACTIVE"; then
  echo "FAIL: did not expect tombstoned docs/b.txt in active search"
  echo "$SEARCH_ACTIVE"
  exit 1
fi

SEARCH_ALL="$("$BIN" search "$ARCHIVE" docs/ --include-tombstoned)"
if ! grep -q '"count":2' <<<"$SEARCH_ALL"; then
  echo "FAIL: expected include-tombstoned search count=2"
  echo "$SEARCH_ALL"
  exit 1
fi
if ! grep -q '"path":"docs/b.txt"' <<<"$SEARCH_ALL"; then
  echo "FAIL: expected docs/b.txt in include-tombstoned search"
  echo "$SEARCH_ALL"
  exit 1
fi

echo "PASS: CLI search tests"
