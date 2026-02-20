use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use db_core::{Command, DbError, Index, NodeId, Term};
use db_raft::{RaftCluster, Role};

const PHASE7_NODE_IDS: [NodeId; 5] = [1, 2, 3, 4, 5];
const PHASE7_KEYS: [&str; 6] = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
const PHASE7_VALUES: [&str; 7] = ["one", "two", "three", "four", "five", "six", "seven"];

#[derive(Debug, Clone)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn choose_index(&mut self, upper_exclusive: usize) -> usize {
        (self.next_u64() as usize) % upper_exclusive
    }

    fn choose_u64(&mut self, min_inclusive: u64, max_inclusive: u64) -> u64 {
        min_inclusive + (self.next_u64() % (max_inclusive - min_inclusive + 1))
    }
}

#[derive(Debug, Clone)]
enum ChaosAction {
    Tick(u64),
    Put { key: String, value: String },
    Delete { key: String },
    Disconnect(NodeId),
    Reconnect(NodeId),
    DelayDelivery { node_id: NodeId, ticks: u64 },
    StartElection(NodeId),
    ScheduleElection { node_id: NodeId, after_ticks: u64 },
    Snapshot,
    ScheduleSnapshot { after_ticks: u64 },
    Restart,
    ScheduleRestart { after_ticks: u64 },
}

impl ChaosAction {
    fn describe(&self) -> String {
        match self {
            Self::Tick(ticks) => format!("tick_many({ticks})"),
            Self::Put { key, value } => format!("put({key}={value})"),
            Self::Delete { key } => format!("delete({key})"),
            Self::Disconnect(node_id) => format!("disconnect({node_id})"),
            Self::Reconnect(node_id) => format!("reconnect({node_id})"),
            Self::DelayDelivery { node_id, ticks } => {
                format!("delay_delivery(node={node_id}, ticks={ticks})")
            }
            Self::StartElection(node_id) => format!("start_election({node_id})"),
            Self::ScheduleElection {
                node_id,
                after_ticks,
            } => {
                format!("schedule_election(node={node_id}, after_ticks={after_ticks})")
            }
            Self::Snapshot => "create_leader_snapshot()".to_owned(),
            Self::ScheduleSnapshot { after_ticks } => {
                format!("schedule_snapshot(after_ticks={after_ticks})")
            }
            Self::Restart => "restart_cluster()".to_owned(),
            Self::ScheduleRestart { after_ticks } => {
                format!("schedule_restart(after_ticks={after_ticks})")
            }
        }
    }
}

#[derive(Debug, Clone)]
enum ScheduledFault {
    Reconnect(NodeId),
    StartElection(NodeId),
    Snapshot,
    Restart,
}

impl ScheduledFault {
    fn describe(&self) -> String {
        match self {
            Self::Reconnect(node_id) => format!("scheduled_reconnect({node_id})"),
            Self::StartElection(node_id) => format!("scheduled_start_election({node_id})"),
            Self::Snapshot => "scheduled_snapshot()".to_owned(),
            Self::Restart => "scheduled_restart()".to_owned(),
        }
    }
}

#[derive(Debug)]
struct Phase7Harness {
    node_ids: Vec<NodeId>,
    wal_dir: PathBuf,
    cluster: RaftCluster,
    rng: Lcg,
    scheduled_faults: BTreeMap<u64, Vec<ScheduledFault>>,
    previous_terms: BTreeMap<NodeId, Term>,
    previous_commit_indexes: BTreeMap<NodeId, Index>,
    model_state: BTreeMap<String, String>,
    model_applied_index: Index,
}

impl Phase7Harness {
    fn new(seed: u64, suffix: &str) -> Self {
        let wal_dir = temp_wal_dir(suffix);
        let node_ids = PHASE7_NODE_IDS.to_vec();
        let cluster = RaftCluster::new_durable(node_ids.clone(), &wal_dir)
            .expect("durable cluster should build");
        let mut previous_terms = BTreeMap::new();
        let mut previous_commit_indexes = BTreeMap::new();

        for node_id in &node_ids {
            let node = cluster.node(*node_id).expect("node should exist");
            previous_terms.insert(*node_id, node.current_term());
            previous_commit_indexes.insert(*node_id, node.commit_index());
        }

        let leader_id = cluster.leader_id();
        let leader = cluster.node(leader_id).expect("leader should exist");
        let model_state = leader.snapshot();
        let model_applied_index = leader.commit_index();

        Self {
            node_ids,
            wal_dir,
            cluster,
            rng: Lcg::new(seed),
            scheduled_faults: BTreeMap::new(),
            previous_terms,
            previous_commit_indexes,
            model_state,
            model_applied_index,
        }
    }

    fn run_steps(&mut self, steps: usize) {
        for step in 0..steps {
            let action = self.next_action();
            self.apply_and_assert(step, &action);
        }
    }

    fn run_scripted_actions(&mut self, start_step: usize, actions: &[ChaosAction]) -> usize {
        let mut step = start_step;
        for action in actions {
            self.apply_and_assert(step, action);
            step += 1;
        }
        step
    }

    fn apply_and_assert(&mut self, step: usize, action: &ChaosAction) {
        self.apply_action(action);
        self.assert_invariants(step, action);
    }

    fn finalize_and_assert_convergence(&mut self) {
        self.scheduled_faults.clear();

        for node_id in self.node_ids.clone() {
            self.cluster
                .reconnect(node_id)
                .expect("reconnect during convergence should succeed");
        }

        self.ensure_active_leader();

        for _ in 0..24 {
            self.cluster
                .tick()
                .expect("tick during convergence should succeed");
            match self.cluster.heartbeat_round() {
                Ok(_) => {}
                Err(DbError::LeaderMissing(_)) => {
                    self.ensure_active_leader();
                }
                Err(error) => panic!("heartbeat during convergence should succeed: {error:?}"),
            }
        }

        self.ensure_active_leader();

        let leader_id = self.cluster.leader_id();
        let leader = self.cluster.node(leader_id).expect("leader should exist");
        let leader_commit = leader.commit_index();
        let leader_snapshot = leader.snapshot();

        for node_id in self.node_ids.clone() {
            let node = self.cluster.node(node_id).expect("node should exist");
            assert_eq!(
                node.commit_index(),
                leader_commit,
                "node {node_id} did not converge to leader commit index {leader_commit}"
            );
            assert_eq!(
                node.snapshot(),
                leader_snapshot,
                "node {node_id} snapshot diverged from converged leader state"
            );
        }
    }

    fn next_action(&mut self) -> ChaosAction {
        let roll = self.rng.next_u64() % 100;
        if roll < 22 {
            return ChaosAction::Tick(self.rng.choose_u64(1, 4));
        }

        if roll < 40 {
            if !self.has_active_leader() {
                return ChaosAction::Tick(1);
            }
            return ChaosAction::Put {
                key: PHASE7_KEYS[self.rng.choose_index(PHASE7_KEYS.len())].to_owned(),
                value: PHASE7_VALUES[self.rng.choose_index(PHASE7_VALUES.len())].to_owned(),
            };
        }

        if roll < 48 {
            if !self.has_active_leader() {
                return ChaosAction::Tick(1);
            }
            return ChaosAction::Delete {
                key: PHASE7_KEYS[self.rng.choose_index(PHASE7_KEYS.len())].to_owned(),
            };
        }

        if roll < 58 {
            let candidates = self.connected_nodes();
            if candidates.is_empty() {
                return ChaosAction::Tick(1);
            }
            return ChaosAction::Disconnect(candidates[self.rng.choose_index(candidates.len())]);
        }

        if roll < 66 {
            let candidates = self.disconnected_nodes();
            if candidates.is_empty() {
                return ChaosAction::Tick(1);
            }
            return ChaosAction::Reconnect(candidates[self.rng.choose_index(candidates.len())]);
        }

        if roll < 74 {
            let candidates = self.connected_nodes();
            if candidates.is_empty() {
                return ChaosAction::Tick(1);
            }

            return ChaosAction::DelayDelivery {
                node_id: candidates[self.rng.choose_index(candidates.len())],
                ticks: self.rng.choose_u64(2, 6),
            };
        }

        if roll < 82 {
            let leader_id = self.cluster.leader_id();
            let candidates: Vec<NodeId> = self
                .node_ids
                .iter()
                .copied()
                .filter(|node_id| *node_id != leader_id)
                .collect();

            if candidates.is_empty() {
                return ChaosAction::Tick(1);
            }

            return ChaosAction::StartElection(candidates[self.rng.choose_index(candidates.len())]);
        }

        if roll < 89 {
            let leader_id = self.cluster.leader_id();
            let candidates: Vec<NodeId> = self
                .node_ids
                .iter()
                .copied()
                .filter(|node_id| *node_id != leader_id)
                .collect();

            if candidates.is_empty() {
                return ChaosAction::Tick(1);
            }

            return ChaosAction::ScheduleElection {
                node_id: candidates[self.rng.choose_index(candidates.len())],
                after_ticks: self.rng.choose_u64(1, 5),
            };
        }

        if roll < 93 {
            return ChaosAction::Snapshot;
        }

        if roll < 96 {
            return ChaosAction::ScheduleSnapshot {
                after_ticks: self.rng.choose_u64(1, 4),
            };
        }

        if roll < 98 {
            return ChaosAction::ScheduleRestart {
                after_ticks: self.rng.choose_u64(2, 6),
            };
        }

        ChaosAction::Restart
    }

    fn connected_nodes(&self) -> Vec<NodeId> {
        self.node_ids
            .iter()
            .copied()
            .filter(|node_id| {
                self.cluster
                    .is_connected_to_leader(*node_id)
                    .expect("connection state should be readable")
            })
            .collect()
    }

    fn disconnected_nodes(&self) -> Vec<NodeId> {
        self.node_ids
            .iter()
            .copied()
            .filter(|node_id| {
                !self
                    .cluster
                    .is_connected_to_leader(*node_id)
                    .expect("connection state should be readable")
            })
            .collect()
    }

    fn has_active_leader(&self) -> bool {
        self.node_ids.iter().copied().any(|node_id| {
            self.cluster
                .node(node_id)
                .is_ok_and(|node| node.role() == Role::Leader)
        })
    }

    fn ensure_active_leader(&mut self) {
        if self.has_active_leader() {
            return;
        }

        for node_id in self.node_ids.clone() {
            let outcome = self
                .cluster
                .start_election(node_id)
                .expect("election during convergence should execute");
            if outcome.elected && self.has_active_leader() {
                return;
            }
        }

        self.cluster
            .tick_many(6)
            .expect("ticks for leader recovery should execute");

        if !self.has_active_leader() {
            panic!("failed to recover an active leader");
        }
    }

    fn apply_action(&mut self, action: &ChaosAction) {
        match action {
            ChaosAction::Tick(ticks) => {
                for _ in 0..*ticks {
                    self.cluster.tick().expect("tick should succeed");
                    self.run_due_scheduled_faults();
                }
            }
            ChaosAction::Put { key, value } => {
                let result = self.cluster.propose(Command::Put {
                    key: key.clone(),
                    value: value.clone(),
                });
                match result {
                    Ok(_) => {}
                    Err(DbError::LeaderMissing(_)) => {
                        self.cluster
                            .tick()
                            .expect("tick after leaderless write should succeed");
                        self.run_due_scheduled_faults();
                    }
                    Err(error) => panic!("put proposal should execute: {error:?}"),
                }
            }
            ChaosAction::Delete { key } => {
                let result = self.cluster.propose(Command::Delete { key: key.clone() });
                match result {
                    Ok(_) => {}
                    Err(DbError::LeaderMissing(_)) => {
                        self.cluster
                            .tick()
                            .expect("tick after leaderless delete should succeed");
                        self.run_due_scheduled_faults();
                    }
                    Err(error) => panic!("delete proposal should execute: {error:?}"),
                }
            }
            ChaosAction::Disconnect(node_id) => {
                self.cluster
                    .disconnect(*node_id)
                    .expect("disconnect should succeed");
                self.clear_scheduled_reconnect(*node_id);
            }
            ChaosAction::Reconnect(node_id) => {
                self.cluster
                    .reconnect(*node_id)
                    .expect("reconnect should succeed");
                self.clear_scheduled_reconnect(*node_id);
            }
            ChaosAction::DelayDelivery { node_id, ticks } => {
                self.cluster
                    .disconnect(*node_id)
                    .expect("disconnect for delay should succeed");
                self.clear_scheduled_reconnect(*node_id);
                self.schedule_fault(*ticks, ScheduledFault::Reconnect(*node_id));
            }
            ChaosAction::StartElection(node_id) => {
                let _ = self
                    .cluster
                    .start_election(*node_id)
                    .expect("election should execute");
            }
            ChaosAction::ScheduleElection {
                node_id,
                after_ticks,
            } => {
                self.schedule_fault(*after_ticks, ScheduledFault::StartElection(*node_id));
            }
            ChaosAction::Snapshot => {
                let _ = self
                    .cluster
                    .create_leader_snapshot()
                    .expect("snapshot attempt should execute");
            }
            ChaosAction::ScheduleSnapshot { after_ticks } => {
                self.schedule_fault(*after_ticks, ScheduledFault::Snapshot);
            }
            ChaosAction::Restart => {
                self.cluster = RaftCluster::new_durable(self.node_ids.clone(), &self.wal_dir)
                    .expect("restart recovery should succeed");
            }
            ChaosAction::ScheduleRestart { after_ticks } => {
                self.schedule_fault(*after_ticks, ScheduledFault::Restart);
            }
        }
    }

    fn schedule_fault(&mut self, after_ticks: u64, fault: ScheduledFault) {
        let delay = after_ticks.max(1);
        let due_tick = self.cluster.clock_ticks().saturating_add(delay);
        self.scheduled_faults
            .entry(due_tick)
            .or_default()
            .push(fault);
    }

    fn run_due_scheduled_faults(&mut self) {
        loop {
            let due_tick = self.cluster.clock_ticks();
            let Some(faults) = self.scheduled_faults.remove(&due_tick) else {
                break;
            };

            for fault in faults {
                self.apply_scheduled_fault(fault);
            }
        }
    }

    fn apply_scheduled_fault(&mut self, fault: ScheduledFault) {
        let description = fault.describe();
        match fault {
            ScheduledFault::Reconnect(node_id) => {
                self.cluster
                    .reconnect(node_id)
                    .unwrap_or_else(|error| panic!("{description} should execute: {error:?}"));
            }
            ScheduledFault::StartElection(node_id) => {
                let _ = self
                    .cluster
                    .start_election(node_id)
                    .unwrap_or_else(|error| panic!("{description} should execute: {error:?}"));
            }
            ScheduledFault::Snapshot => {
                let _ = self
                    .cluster
                    .create_leader_snapshot()
                    .unwrap_or_else(|error| panic!("{description} should execute: {error:?}"));
            }
            ScheduledFault::Restart => {
                self.cluster = RaftCluster::new_durable(self.node_ids.clone(), &self.wal_dir)
                    .unwrap_or_else(|error| panic!("{description} should execute: {error:?}"));
            }
        }
    }

    fn clear_scheduled_reconnect(&mut self, node_id: NodeId) {
        let mut empty_ticks = Vec::new();

        for (tick, faults) in self.scheduled_faults.iter_mut() {
            faults
                .retain(|fault| !matches!(fault, ScheduledFault::Reconnect(id) if *id == node_id));
            if faults.is_empty() {
                empty_ticks.push(*tick);
            }
        }

        for tick in empty_ticks {
            self.scheduled_faults.remove(&tick);
        }
    }

    fn assert_invariants(&mut self, step: usize, action: &ChaosAction) {
        let leader_id = self.cluster.leader_id();
        let mut leaders = Vec::new();
        for node_id in self.node_ids.clone() {
            let node = self.cluster.node(node_id).expect("node should exist");
            if node.role() == Role::Leader {
                leaders.push(node_id);
            }
        }

        assert!(
            leaders.len() <= 1,
            "step {step} action {}: multiple leaders observed: {:?}",
            action.describe(),
            leaders
        );

        if let Some(active_leader_id) = leaders.first().copied() {
            assert_eq!(
                active_leader_id,
                leader_id,
                "step {step} action {}: leader_id {} does not match active leader {}",
                action.describe(),
                leader_id,
                active_leader_id
            );
        }

        for node_id in self.node_ids.clone() {
            let node = self.cluster.node(node_id).expect("node should exist");
            let term = node.current_term();
            let commit = node.commit_index();
            let base = node.log_base_index();
            let last_index = base + (node.log_len() as u64);

            assert!(
                commit >= base,
                "step {step} action {}: node {node_id} has commit_index {commit} behind log base {base}",
                action.describe()
            );
            assert!(
                commit <= last_index,
                "step {step} action {}: node {node_id} has commit_index {commit} beyond log end {last_index}",
                action.describe()
            );

            let previous_term = self.previous_terms.get(&node_id).copied().unwrap_or(0);
            assert!(
                term >= previous_term,
                "step {step} action {}: node {node_id} term regressed from {previous_term} to {term}",
                action.describe()
            );
            self.previous_terms.insert(node_id, term);

            let previous_commit = self
                .previous_commit_indexes
                .get(&node_id)
                .copied()
                .unwrap_or(0);
            assert!(
                commit >= previous_commit,
                "step {step} action {}: node {node_id} commit index regressed from {previous_commit} to {commit}",
                action.describe()
            );
            self.previous_commit_indexes.insert(node_id, commit);
        }

        self.reconcile_model_with_leader(step, action);
    }

    fn reconcile_model_with_leader(&mut self, step: usize, action: &ChaosAction) {
        let leader_id = self.cluster.leader_id();
        let (leader_commit_index, leader_snapshot, committed_delta, missing_entry_index) = {
            let leader = self.cluster.node(leader_id).expect("leader should exist");
            let leader_commit_index = leader.commit_index();
            let leader_snapshot = leader.snapshot();

            let mut committed_delta = Vec::new();
            let mut missing_entry_index = None;
            if leader_commit_index > self.model_applied_index {
                for index in self.model_applied_index + 1..=leader_commit_index {
                    match leader.entry(index) {
                        Some(entry) => committed_delta.push(entry.command.clone()),
                        None => {
                            missing_entry_index = Some(index);
                            break;
                        }
                    }
                }
            }

            (
                leader_commit_index,
                leader_snapshot,
                committed_delta,
                missing_entry_index,
            )
        };

        if leader_commit_index < self.model_applied_index {
            self.model_state = leader_snapshot.clone();
            self.model_applied_index = leader_commit_index;
        } else if let Some(missing_index) = missing_entry_index {
            self.model_state = leader_snapshot.clone();
            self.model_applied_index = leader_commit_index;
            assert!(
                missing_index <= leader_commit_index,
                "step {step} action {}: inconsistent missing-entry marker {missing_index}",
                action.describe()
            );
        } else {
            for command in committed_delta {
                apply_model_command(&mut self.model_state, &command);
            }
            self.model_applied_index = leader_commit_index;
        }

        assert_eq!(
            self.model_state,
            leader_snapshot,
            "step {step} action {}: model diverged from leader snapshot at commit index {}",
            action.describe(),
            leader_commit_index
        );

        for node_id in self.node_ids.clone() {
            let node = self.cluster.node(node_id).expect("node should exist");
            if node.commit_index() == self.model_applied_index {
                assert_eq!(
                    node.snapshot(),
                    self.model_state,
                    "step {step} action {}: node {node_id} snapshot diverged at commit {}",
                    action.describe(),
                    self.model_applied_index
                );
            }
        }
    }
}

fn apply_model_command(model: &mut BTreeMap<String, String>, command: &Command) {
    match command {
        Command::Put { key, value } => {
            model.insert(key.clone(), value.clone());
        }
        Command::Delete { key } => {
            model.remove(key);
        }
    }
}

fn temp_wal_dir(suffix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after UNIX_EPOCH")
        .as_nanos();

    std::env::temp_dir().join(format!(
        "anvildb-phase7-chaos-{suffix}-{}-{nonce}",
        std::process::id()
    ))
}

fn cleanup_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

#[test]
fn phase7_chaos_harness_preserves_invariants_and_converges() {
    let seeds = [0xA11CE_u64, 0xC0FFEE_u64, 0xDEADBEEF_u64];

    for seed in seeds {
        let mut harness = Phase7Harness::new(seed, &format!("seed-{seed}"));
        harness.run_steps(220);
        harness.finalize_and_assert_convergence();

        let wal_dir = harness.wal_dir.clone();
        drop(harness);
        cleanup_dir(&wal_dir);
    }
}

#[test]
fn phase7_scripted_fault_schedule_preserves_invariants_and_converges() {
    let mut harness = Phase7Harness::new(0xFACEB00C_u64, "scripted-schedule");
    let scripted = [
        ChaosAction::Put {
            key: "alpha".to_owned(),
            value: "one".to_owned(),
        },
        ChaosAction::Put {
            key: "beta".to_owned(),
            value: "two".to_owned(),
        },
        ChaosAction::ScheduleElection {
            node_id: 2,
            after_ticks: 2,
        },
        ChaosAction::Disconnect(1),
        ChaosAction::DelayDelivery {
            node_id: 1,
            ticks: 5,
        },
        ChaosAction::ScheduleSnapshot { after_ticks: 3 },
        ChaosAction::ScheduleRestart { after_ticks: 4 },
        ChaosAction::Tick(6),
        ChaosAction::Reconnect(1),
        ChaosAction::Tick(4),
        ChaosAction::Disconnect(4),
        ChaosAction::Disconnect(5),
        ChaosAction::StartElection(3),
        ChaosAction::Put {
            key: "gamma".to_owned(),
            value: "three".to_owned(),
        },
        ChaosAction::Reconnect(4),
        ChaosAction::Reconnect(5),
        ChaosAction::Tick(8),
        ChaosAction::Snapshot,
        ChaosAction::Restart,
        ChaosAction::Tick(6),
    ];

    let next_step = harness.run_scripted_actions(0, &scripted);
    harness.run_steps(next_step + 140);
    harness.finalize_and_assert_convergence();

    let wal_dir = harness.wal_dir.clone();
    drop(harness);
    cleanup_dir(&wal_dir);
}

#[test]
fn phase7_overlapping_deferred_faults_still_converge() {
    let mut harness = Phase7Harness::new(0xBAD5EED_u64, "overlapping-schedule");
    let scripted = [
        ChaosAction::Put {
            key: "delta".to_owned(),
            value: "four".to_owned(),
        },
        ChaosAction::DelayDelivery {
            node_id: 2,
            ticks: 3,
        },
        ChaosAction::DelayDelivery {
            node_id: 3,
            ticks: 5,
        },
        ChaosAction::ScheduleElection {
            node_id: 4,
            after_ticks: 1,
        },
        ChaosAction::ScheduleElection {
            node_id: 5,
            after_ticks: 2,
        },
        ChaosAction::ScheduleSnapshot { after_ticks: 4 },
        ChaosAction::ScheduleRestart { after_ticks: 6 },
        ChaosAction::Tick(7),
        ChaosAction::Reconnect(2),
        ChaosAction::Reconnect(3),
        ChaosAction::Tick(10),
    ];

    let next_step = harness.run_scripted_actions(0, &scripted);
    harness.run_steps(next_step + 120);
    harness.finalize_and_assert_convergence();

    let wal_dir = harness.wal_dir.clone();
    drop(harness);
    cleanup_dir(&wal_dir);
}
