# AnvilDB

A long-horizon build for a formally-informed distributed database.

This repository starts with a deliberately small vertical slice:
- In-memory log and state machine
- Raft-like leader replication with follower catch-up mechanics
- Deterministic tests for replication behavior
- Initial TLA+ model skeleton and safety invariants list

## Quick start

```bash
cd /path/to/anvildb
make test
make run
```

## Current scope (Phase 7 kickoff complete)

- Dynamic leader transitions via manual election trigger (`start_election`)
- Logical-time scheduler with automatic election timeouts (`tick` / `tick_many`)
- Heartbeat cadence to suppress unnecessary elections
- In-memory state machine with WAL-backed durable log replay
- Per-follower replication state (`nextIndex`/`matchIndex`)
- Conflict repair and quorum-gated commit advancement
- RequestVote vote-grant/reject semantics with term checks
- WAL append/load/truncate/replay prototype in `db-storage`
- Durable cluster bootstrap via `new_durable(...)` with on-disk replay
- Durable term/vote metadata for restart-safe election semantics
- Snapshot create/install flow with leader log-prefix compaction
- Deterministic election backoff/jitter for repeated failed elections
- In-process API service scaffold with explicit request/response envelopes
- Transport abstraction with `InProcessTransport`, envelope-level gRPC adapter, and byte-wire gRPC server/client adapter
- Node-addressed request routing with follower-to-leader redirect behavior
- Read consistency modes (`Eventual`, `Linearizable`) exposed at API level
- Linearizable reads gated by explicit quorum heartbeat-ack lease checks with safety-capped lease windows
- Proposal timeout handling and pending-log backpressure policy checks
- SQL parser + logical planner crate (`db-sql`) for `INSERT`/`DELETE`/`UPDATE`/`SELECT` on `kv`
- SQL logical plan tree (`Scan`/`Filter`/`Projection`) with lowering into executable KV operations
- SQL request path in API service with projection/predicate execution and structured SQL responses
- MVCC scaffolding types in `db-sql` (`VersionChain`, `MvccCatalog`, transaction/version timestamps)
- MVCC-backed server read/write execution with commit-index timestamp visibility
- MVCC historical reads for lagging followers and automatic resync for out-of-band cluster writes
- Phase 7 deterministic chaos harness with partitions, delayed delivery, crash/restart, elections, snapshots, and deferred fault scheduling
- Model-vs-implementation invariant checks over commit monotonicity, term monotonicity, and snapshot agreement
- Expanded TLA+ Raft model with delayed AppendEntries delivery, partition/reconnect, packet drop, follower divergence, and crash/restart transitions
- CI-gated Phase 7 verification loop (`phase7_chaos` + bounded TLC invariants via `spec/kv_raft_ci.cfg`)
- Byte-wire gRPC interoperability/conformance coverage: chunked payload handling, corruption/truncation rejection, and cross-transport parity checks

## Planned next scope

1. Run higher-bounds/nightly model and chaos suites for deeper fault-space exploration
2. Add operational deployment/testing guidance for real network integration
3. Expand workload-level correctness benchmarks for SQL + MVCC behavior under faults

See `docs/ROADMAP.md`, `docs/ARCHITECTURE.md`, and `spec/kv_raft.tla`.
