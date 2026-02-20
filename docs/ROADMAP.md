# Project Roadmap

## Vision

Build a correctness-first distributed database with a formally modeled control plane, deterministic storage semantics, and fault-oriented validation.

## Current status (as of 2026-02-20)

- Stage: **Phase 7 verification expansion complete (model + CI/nightly gates in place)**
- Working baseline:
  - Multi-crate Rust workspace (`db-core`, `db-storage`, `db-raft`, `server`)
  - In-memory command log + state machine
  - Leader/follower/candidate roles
  - Raft-style append replication with follower catch-up (`nextIndex`/`matchIndex`)
  - Quorum-based commit advancement
  - `RequestVote` election flow with term-aware vote rejection
  - Logical-time `tick` scheduler with automatic election timeout handling
  - Heartbeat cadence for leader liveness signaling
  - Simulated follower disconnect/reconnect behavior
  - WAL prototype with binary record encoding + deterministic replay tests
  - WAL wired into `db-raft` append/replication/commit paths
  - Durable bootstrap constructor that replays committed state from disk
  - Durable term/vote metadata persistence and recovery for election safety
  - Snapshot creation + install flow with WAL-backed restart recovery
  - Deterministic election backoff/jitter policy for repeated failed elections
  - Restart-election and timeout+partition edge-case integration tests
  - TLA+ election-aware model with commit monotonic/election-link safety invariants
  - In-process API/transport scaffold with envelope-based request/response handling
  - Node addressing + redirect semantics for non-leader write/linearizable-read targets
  - API-level consistency controls (`Eventual` vs `Linearizable` reads)
  - Explicit quorum heartbeat-ack lease checks for linearizable reads with safety-capped lease windows
  - Transport abstraction with in-process transport, envelope-level gRPC adapter, and byte-wire gRPC server/client adapter
  - API-level timeout and pending-log backpressure controls with coverage tests
  - New `db-sql` crate with parser + logical plan mapping for key/value SQL subset
  - SQL request surface in `server` API that routes plans through raft read/write flows
  - MVCC-backed execution for API reads and SQL selects/mutations with commit-index visibility
  - MVCC resync path when cluster state advances outside API-proposed write flows
  - Deterministic Phase 7 chaos harness over partition, delayed-delivery, snapshot, election, and restart faults, including deferred fault scheduling
  - Model-vs-implementation checks driven by committed-log replay against live leader snapshot state
  - Invariant guards for commit monotonicity, term monotonicity, and commit-index/log-bounds safety
  - Expanded TLA+ fault model with delayed AppendEntries delivery queue and partition/drop/crash transitions
  - CI workflow gate for Phase 7 checks (deterministic chaos test + bounded TLC invariant run)
  - Nightly higher-bounds verification workflow (expanded chaos seeds/steps + higher-bounds TLC config)
  - Byte-wire gRPC interoperability/conformance tests for chunked request handling and corruption/truncation rejection

## Milestones

1. Phase 1: Baseline vertical slice (Complete)
- Goal: Prove end-to-end replicated apply flow exists.
- Delivered: In-memory log/state machine, deterministic tests, runnable demo.

2. Phase 2: Raft log matching + replication state (Complete)
- Goal: Make replication behavior closer to real Raft.
- Delivered:
  - Per-follower replication pointers (`nextIndex`, `matchIndex`)
  - Append conflict detection and repair on followers
  - Leader commit index advancement gated by quorum and term
  - Partition simulation via disconnect/reconnect API
 
3. Phase 3: Leader election and term transitions (Complete)
- Goal: Support realistic leadership changes and voting safety.
- Delivered so far:
  - Explicit election entrypoint (`start_election`)
  - `Candidate` role transitions
  - RequestVote validation for log freshness and single-vote-per-term behavior
  - Timeout-driven election triggering via logical ticks
  - Periodic heartbeats that reset follower election timers
  - Tests for majority win, split vote, and stale-term rejection
  - Tests for timeout failover and heartbeat stability
  - Timeout + partition interaction edge-case tests
  - Deterministic backoff/jitter policy for repeated failed elections

4. Phase 4: Durable storage + recovery (Complete)
- Goal: Make state survive process crash/restart safely.
- Delivered so far:
  - Binary WAL record format with append/load/truncate operations
  - Deterministic WAL replay helper into log + state machine
  - Corruption detection for non-contiguous log index sequence
  - Unit tests for round-trip, truncate/recover, and corruption rejection
  - Commit-index metadata persistence and recovery validation
  - Term/vote metadata persistence and durable reload path
  - WAL integration into raft append, follower conflict repair, and commit advancement
  - Restart tests for committed/uncommitted entries and restart-election behavior
  - Snapshot creation/install flow with log-prefix compaction + restart safety tests

5. Phase 5: Client/API layer + consistency controls (Complete)
- External transport (likely gRPC)
- Linearizable reads (heartbeat/lease strategy)
- Timeouts, backpressure, and overload protection
- Delivered so far:
  - Request/response envelope API in `server` crate
  - Node-addressed routing + leader redirect behavior
  - Lease-based linearizable-read gating with quorum heartbeat-ack proof and stale-lease safety checks
  - Transport trait with `InProcessTransport`, `GrpcTransportAdapter`, and `GrpcWireTransportAdapter`
  - Proposal timeout and overload/backpressure tests around replication lag

6. Phase 6: SQL execution surface (Complete)
- SQL parser and logical planning
- MVCC transaction semantics
- Index and compaction strategy
- Delivered so far:
  - `db-sql` parser for `INSERT`, `DELETE`, `UPDATE`, `SELECT` over the `kv` table
  - Projection list + predicate operator support (`=` and `!=`) for SQL filtering paths
  - Logical plan tree (`Scan`/`Filter`/`Projection`) plus lowering to executable KV plans
  - `ClientRequest::Sql` path in API layer with structured SQL result envelopes
  - MVCC scaffolding types (`VersionChain`, `MvccCatalog`, transaction/version timestamps) + tests
  - MVCC integration in live server read/write execution using commit index as visibility timestamp
  - MVCC-based stale follower reads and out-of-band-write resynchronization tests

7. Phase 7: Verification and chaos (Kickoff complete, in progress)
- Expand TLA+ spec from election skeleton to richer log-matching/fault behaviors
- Model-vs-implementation invariant checks
- Added: commit monotonic and election/commit-link invariants in model skeleton
- Delivered kickoff: deterministic fault harness for partitions, crash/restart, delayed reconnect, elections, and snapshots
- Delivered kickoff: per-step invariant assertions and leader-state differential checks against a replayed reference model
- Delivered expansion: queue-based delayed-delivery TLA+ transitions with partition/heal/drop and crash-restart paths
- Delivered expansion: CI gate that executes `phase7_chaos` plus bounded TLC invariants via `spec/kv_raft_ci.cfg`
- Delivered expansion: generalized deferred fault scheduler with scripted election/snapshot/restart scenarios
- Delivered expansion: byte-wire gRPC round-trip path and lease hardening coverage for stale leader and clock-regression edges
- Delivered expansion: byte-wire conformance checks for fragmented payload streams and malformed payload rejection paths
- Delivered expansion: nightly higher-bounds verification workflow for chaos harness depth and TLC search-space exploration

## Definition of done for the next checkpoint (Phase 7 kickoff slice)

- Done: SQL parser/planner/lowering bridge with API integration
- Done: MVCC scaffolding wired into server read/write execution paths
- Done: Deterministic chaos harness with restart/partition/delay coverage and convergence validation
- Done: Model-vs-implementation verification loop with invariant checks after each injected fault action
- Done: Expanded TLA+ model with delayed-delivery/partition/drop/crash transitions and richer committed-prefix invariants
- Done: CI-gated verification loop for Phase 7 (`phase7_chaos` + TLC model check)
- Done: Production-oriented wire conformance coverage over byte-wire gRPC path (fragmented/chunked and malformed payload cases)
- Next: Add deployment-facing network integration guidance and expand workload-level correctness benchmarks
