# AXON (Implementation Foundation)

AXON = Agent eXchange Object Notation.

This repository now contains a runnable Rust foundation for the AXON v0.1 draft format.

Execution plan: see [PROJECT_PLAN.md](./PROJECT_PLAN.md).
Project whitepaper: see [WHITEPAPER.md](./WHITEPAPER.md).

Implemented in this baseline:
- CLI entrypoint (`axon`)
- Binary AXON header codec (fixed 128-byte layout + CRC32 validation)
- Empty archive creation (`axon init`)
- Header inspection (`axon info`)
- Root manifest read (`axon peek`)
- File add/read/patch flow with BASE blocks + block index (`axon add`, `axon read`, `axon patch`)
- Versioned file reads (`axon read --version`)
- File remove tombstones in manifest (`axon remove`)
- Manifest-only path search (`axon search`)
- Manifest file listing with prefix/pagination (`axon list`)
- WAL persistence + status inspection (`axon wal --status`)
- Atomic batch mutations with OCC checks (`axon batch`)
- Structural archive verification prechecks (`axon verify`)
- GC checkpoint compaction with optional tombstone pruning (`axon gc`)
- Per-file version history inspection (`axon log`)
- Unit and CLI integration tests

## Quick Start

```bash
cargo run -- init demo.axon
cargo run -- add demo.axon src/main.rs ./src/main.rs
cargo run -- patch demo.axon src/main.rs ./src/main.rs
cargo run -- patch demo.axon src/main.rs ./src/main.rs --expected-version 2
cargo run -- remove demo.axon src/main.rs
cargo run -- remove demo.axon src/main.rs --expected-version 3
cargo run -- read demo.axon src/main.rs
cargo run -- read demo.axon src/main.rs --version 2
cargo run -- log demo.axon src/main.rs --pretty
cargo run -- search demo.axon src/
cargo run -- list demo.axon --prefix src/ --limit 20
cargo run -- wal demo.axon --status --pretty
cargo run -- verify demo.axon --pretty
cargo run -- gc demo.axon --pretty
cargo run -- gc demo.axon --prune-tombstones --pretty
cargo run -- batch demo.axon ./mutations.json --pretty
cargo run -- info demo.axon --pretty
cargo run -- peek demo.axon --pretty
```

## CLI Bash Tests

```bash
bash ./run_cli_tests.sh
```

## Current Notes

- Root and shard manifests are encoded in a binary v1 layout (magic + versioned fields).
- Root/shard manifest reads support JSON fallback for older bootstrap archives.
- `search`/`list` read from root manifest plus referenced uncompressed shard manifests.
- Commits route larger file sets into deterministic shard buckets; small sets remain in root manifest.
- Mutations append WAL entries (`add`/`patch`/`remove`), with `wal --status` for inspection.
- Read/query state is resolved as base manifest + WAL replay (single-writer semantics).
- `batch` applies multiple mutations atomically with OCC expected-version checks.
- `verify` performs deep consistency checks across header/WAL/index/manifest/block references.
- `gc` checkpoints current reachable data into a fresh snapshot, folds WAL entries, and can prune tombstoned manifest entries.
- WAL and OCC are implemented for current single-writer semantics.
- Sidecar lock-table writer coordination with TTL is implemented for mutating commands.
- CLI failures now emit machine-readable JSON errors with stable symbolic codes and exit codes.
- Encryption is not implemented yet.
- Delta blocks are implemented for patch/read flows with deterministic BASE/DELTA selection and depth caps.
- Versioned reads and per-file history logs are implemented (`read --version`, `log`).
- Current write path is append-only with snapshot updates for block index + root manifest, followed by header update.
- Manifest tracks per-file `version`, `history_block_ids`, and `tombstoned` state.
- `patch` and `remove` support optional `--expected-version` precondition checks.
- Block IDs use BLAKE3 for content hashing and deduplication.
- This baseline is intended to anchor incremental, test-driven development toward the full spec.

## Next Implementation Milestones

1. Add encryption and key-management flows.
2. Expand observability (operation metrics, richer diagnostics).
3. Add compatibility/fuzzing coverage for evolving format versions.
