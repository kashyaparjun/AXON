# AXON Project Plan (v0.1 Target)

This plan turns the AXON specification into an incremental implementation roadmap with clear acceptance criteria.

## 1. Objectives

- Deliver a usable `axon` CLI that can create, inspect, read, and update AXON archives.
- Preserve core v0.1 guarantees: inspectability, surgical reads/writes, append-only mutation model, and bounded reconstruction cost.
- Keep implementation test-driven and safe for future format evolution.

## 2. Current Baseline (Updated)

Implemented:
- Rust project + CLI skeleton
- Header encode/decode with checksum validation
- Empty archive initialization (`init`)
- Basic metadata inspection (`info`) and root manifest read (`peek`)
- Manifest-only file path query (`search`)
- Manifest listing ergonomics with prefix/pagination (`list`)
- Binary root/shard manifest encoding (v1) with JSON fallback migration path
- Shard-aware manifest query reads (root + shard manifests)
- Deterministic path routing into shard manifests on commit for larger file sets
- WAL v1 binary codec and persisted mutation entries for add/patch/remove
- WAL status inspection command (`wal --status`)
- Atomic batch mutation command with OCC checks (`batch`)
- `verify` command with pointer-bounds and decode prechecks
- `gc` checkpoint scaffold that compacts WAL into fresh snapshots
- Block format + block index format primitives
- `add`, `read`, and `patch` for current-version file content
- `remove` tombstone semantics with append-only snapshot commits
- Append-only mutation path:
  - append block (if not deduplicated),
  - append index snapshot,
  - append manifest snapshot,
  - update header pointers
- Block deduplication by content hash (BLAKE3)
- Per-file versioning + history (`version`, `history_block_ids`) in manifest entries
- Manifest tombstone tracking (`tombstoned`) with read/patch exclusion
- Pre-WAL expected-version guardrails on `patch`/`remove` mutations
- Unit tests covering header validation, add/read/patch behavior, dedupe, append growth, and patch error paths
- Top-level bash CLI tests with runner (`run_cli_tests.sh`) validating roundtrip, patch, remove, and failure paths

Not implemented:
- Lock table and multi-process writer locking
- Encryption
- Production-hardening for GC/checkpoint/compaction

## 3. Delivery Phases

## Phase A: Storage Core (Completed)

Scope:
- Define block header and block record IO
- Implement BASE block write/read
- Implement block index structure + binary search lookup
- Add `axon add` and `axon read` for current-version content
- Use append-only snapshots for mutation safety before WAL/OCC

Acceptance criteria:
- Add/read files without full unpack
- `read` resolves data via block index lookup
- Corrupt/missing block detection is explicit and tested
- Repeated writes do not truncate archive; deduplication works for identical content

Status:
- Complete for baseline goals.
- Block IDs now use BLAKE3 to match the v0.1 target direction.

## Phase B: Manifest and Sharding (Completed)

Scope:
- Introduce shard manifest model (done)
- Route file paths to shards (done)
- Implement path listing/query over manifests (done: `search`/`list` over root + referenced shard manifests)
- Move from bootstrap JSON manifest toward stable binary encoding (done: binary v1 + read fallback)

Acceptance criteria:
- `peek` and `search` run without touching data blocks (done: root/shard manifest-only reads)
- Large file trees remain responsive with on-demand shard load (done: shard descriptor-driven reads)
- Manifest format versioning is documented and tested (done: manifest v1 codec tests + compatibility fallback)

## Phase C: Mutation Semantics (Pre-WAL then WAL + OCC) (Completed)

Scope (step 1: pre-WAL):
- Add `patch` command for replacing file content via new BASE blocks (done)
- Introduce per-file version counters and history in manifest (done)
- Add `remove` tombstone semantics in manifest (done)

Scope (step 2: WAL/OCC):
- Append-only WAL entry format (done)
- OCC commit protocol using expected version (done: per-file + atomic batch checks)
- `wal --status` and replay at open (done)
- Reader view = base state + committed WAL entries

Acceptance criteria:
- `patch` updates current content and increments versions deterministically (done)
- Version metadata remains consistent across repeated updates (done)
- Concurrent update conflicts are detected correctly (done for single-writer OCC model)
- Successful writes are durable via WAL before manifest/index fold-in (done for current write flow)
- WAL replay reconstructs latest visible state after restart (done)

## Phase D: Delta and Version History (Completed)

Scope:
- Delta block support (COPY/INSERT stream) (done)
- Version history tracking per file (done)
- Depth cap enforcement (max chain depth 8) (done)
- `log` and versioned `read` (done)

Acceptance criteria:
- Reconstruct current and prior versions through bounded delta chain depth (done)
- Delta/Base decisions are deterministic and test-covered (done for patch path)
- History metadata remains consistent after repeated patches (done)

## Phase E: Maintenance and Integrity

Scope:
- `gc` checkpoint: fold WAL into manifests/index
- Orphan reachability analysis
- Tombstone and optional compact rewrite
- `verify` checksum/integrity command

Acceptance criteria:
- GC is safe and idempotent
- Orphaned blocks are correctly detected/reclaimed
- Verification catches header/index/block inconsistencies

## Phase F: Encryption and Access Tiers

Scope:
- Per-file AES-256-GCM content encryption
- Key input via env/flag/file
- Mixed encrypted/plain files in same archive

Acceptance criteria:
- Encrypted content cannot be read without key
- Manifest inspectability remains intact
- Auth tag failures are detected and surfaced cleanly

## 4. Cross-Cutting Workstreams

- Error model and exit codes: stable, machine-readable JSON errors.
- Compatibility policy: strict header/version checks with clear upgrade path.
- Observability: operation stats in CLI output (`bytes_read`, `blocks_touched`, timings).
- Fuzzing and robustness: parsers for header, index, WAL, and delta streams.

## 5. Test Strategy

- Unit tests: binary codecs, manifest parsing, index lookup, delta apply.
- Integration tests: end-to-end CLI flows (`init -> add -> read -> patch -> remove -> gc`).
- Concurrency tests: OCC conflicts, lock TTL expiry, WAL replay under contention.
- Fault-injection tests: partial writes, corrupt checksum, interrupted GC.
- CLI bash tests: top-level scripts that execute the compiled `axon` binary (`run_cli_tests.sh`).

Minimum quality gates per phase:
- `cargo fmt`
- `cargo clippy -- -D warnings`
- `cargo test`

## 6. Immediate Sprint (Next 1-2 Iterations, Updated)

1. Define WAL entry binary format and append/replay primitives.
   Status: done for add/patch/remove entry append.
2. Add `wal --status` and replay-on-open scaffolding.
   Status: done.
3. Add optimistic concurrency commit checks over expected versions across batched mutations.
   Status: done (`batch` applies all-or-nothing with expected-version guards).
4. Add `verify` command prechecks for header/index/manifest pointer bounds.
   Status: done (includes WAL/index/manifest decode validation).
5. Start GC checkpoint scaffolding to fold WAL into manifest/index snapshots.
   Status: done (checkpoint rewrites reachable blocks/snapshots and clears folded WAL).

## 7. Definition of Done (v0.1)

- CLI commands from spec are either implemented or explicitly marked unsupported with actionable errors.
- Archive can be safely mutated and recovered through WAL replay.
- Read/write behavior is deterministic under concurrent writers (OCC model).
- Integrity verification and GC are production-safe on multi-file archives.
- Format and behavior are documented with examples and test fixtures.
