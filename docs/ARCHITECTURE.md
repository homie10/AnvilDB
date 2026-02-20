# Architecture Plan

## System layers

1. `db-core`: shared protocol/data types and error surface
2. `db-storage`: log and state machine interfaces + memory implementation
3. `db-raft`: replication semantics and commit/apply pipeline
4. `server`: runtime entrypoint + API/transport facade (in-process + byte-wire gRPC paths)
5. Transport adapters: `Transport` trait with in-process, envelope-level gRPC, and byte-wire gRPC implementations
6. `db-sql`: SQL parser + logical planner for the execution surface

## Correctness strategy

- Keep implementation states small and explicit.
- Encode invariants in unit and integration tests.
- Maintain an executable TLA+ model for protocol-level properties.
- Use differential checks between model assumptions and code behavior.
- Gate linearizable reads on explicit quorum heartbeat-ack lease evidence with bounded lease windows.

## Milestones

1. Baseline replication semantics (complete)
2. Log matching and conflict resolution (complete)
3. Leader election and term transitions (complete)
4. Durable storage, restart safety, and snapshot install (complete)
5. Client/API layer + consistency controls (complete; envelope transport, consistency modes, timeout/backpressure added)
6. SQL and query execution surface (complete; parser/planner/lowering + MVCC-backed API execution integrated)
7. Fault harness + formal/model-checking expansion (expanded; deterministic chaos harness plus CI-gated TLC invariants)
