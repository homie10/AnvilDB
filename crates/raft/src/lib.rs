use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use db_core::{Command, DbError, DbResult, Index, LogEntry, NodeId, Term};
use db_storage::{
    apply_entry, recover_node_from_wal, InMemoryStateMachine, MemLog, PersistentRaftMeta,
    SnapshotData, Wal,
};

const DEFAULT_HEARTBEAT_INTERVAL_TICKS: u64 = 2;
const DEFAULT_ELECTION_TIMEOUT_BASE_TICKS: u64 = 6;
const DEFAULT_ELECTION_TIMEOUT_SPREAD_TICKS: u64 = 5;
const MAX_ELECTION_BACKOFF_EXPONENT: u32 = 4;
pub const MIN_ELECTION_TIMEOUT_TICKS: u64 = DEFAULT_ELECTION_TIMEOUT_BASE_TICKS;

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::Io(format!("{context}: {error}"))
}

fn election_jitter_for(node_id: NodeId, term: Term) -> u64 {
    let spread = DEFAULT_ELECTION_TIMEOUT_SPREAD_TICKS.max(1);
    (node_id.wrapping_mul(37) + term.wrapping_mul(17)) % spread
}

fn election_timeout_for(node_id: NodeId, term: Term, backoff_rounds: u32) -> u64 {
    let exp = backoff_rounds.min(MAX_ELECTION_BACKOFF_EXPONENT);
    let backoff_multiplier = 1_u64 << exp;
    let base = DEFAULT_ELECTION_TIMEOUT_BASE_TICKS.saturating_mul(backoff_multiplier);
    base.saturating_add(election_jitter_for(node_id, term))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone)]
struct RequestVote {
    term: Term,
    candidate_id: NodeId,
    last_log_index: Index,
    last_log_term: Term,
}

#[derive(Debug, Clone, Copy)]
struct VoteResponse {
    term: Term,
    vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct RaftNode {
    id: NodeId,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    commit_index: Index,
    last_applied: Index,
    election_elapsed: u64,
    election_timeout_ticks: u64,
    heartbeat_elapsed: u64,
    election_backoff_rounds: u32,
    log: MemLog,
    state_machine: InMemoryStateMachine,
    snapshot: Option<SnapshotData>,
    wal: Option<Wal>,
}

impl RaftNode {
    fn new(id: NodeId, role: Role, current_term: Term) -> Self {
        Self::new_internal(
            id,
            role,
            current_term,
            None,
            None,
            MemLog::default(),
            InMemoryStateMachine::default(),
            None,
            0,
        )
    }

    fn new_with_wal(id: NodeId, role: Role, current_term: Term, wal: Wal) -> DbResult<Self> {
        let (log, state_machine, commit_index) = recover_node_from_wal(&wal)?;
        let snapshot = wal.load_snapshot()?;
        let PersistentRaftMeta {
            current_term: persisted_term,
            voted_for,
        } = wal.load_raft_meta()?;
        let recovered_term = current_term.max(log.last_term()).max(persisted_term);
        let recovered_vote = if persisted_term == recovered_term {
            voted_for
        } else {
            None
        };
        Ok(Self::new_internal(
            id,
            role,
            recovered_term,
            recovered_vote,
            Some(wal),
            log,
            state_machine,
            snapshot,
            commit_index,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn new_internal(
        id: NodeId,
        role: Role,
        current_term: Term,
        voted_for: Option<NodeId>,
        wal: Option<Wal>,
        log: MemLog,
        state_machine: InMemoryStateMachine,
        snapshot: Option<SnapshotData>,
        commit_index: Index,
    ) -> Self {
        Self {
            id,
            role,
            current_term,
            voted_for,
            commit_index,
            last_applied: commit_index,
            election_elapsed: 0,
            election_timeout_ticks: election_timeout_for(id, current_term, 0),
            heartbeat_elapsed: 0,
            election_backoff_rounds: 0,
            log,
            state_machine,
            snapshot,
            wal,
        }
    }

    fn persist_term_vote_metadata(&self) -> DbResult<()> {
        if let Some(wal) = &self.wal {
            wal.persist_raft_meta(self.current_term, self.voted_for)?;
        }
        Ok(())
    }

    fn reset_timeout_with_current_backoff(&mut self) {
        self.election_timeout_ticks =
            election_timeout_for(self.id, self.current_term, self.election_backoff_rounds);
    }

    fn clear_election_backoff(&mut self) {
        self.election_backoff_rounds = 0;
        self.reset_timeout_with_current_backoff();
    }

    fn increase_election_backoff(&mut self) {
        self.election_backoff_rounds = self
            .election_backoff_rounds
            .saturating_add(1)
            .min(MAX_ELECTION_BACKOFF_EXPONENT);
        self.reset_timeout_with_current_backoff();
    }

    fn append_local_entry(&mut self, command: Command, term: Term) -> DbResult<LogEntry> {
        let entry = LogEntry {
            term,
            index: self.log.last_index() + 1,
            command,
        };

        if let Some(wal) = &self.wal {
            wal.append(&entry)?;
        }
        self.log.append(entry.clone());
        Ok(entry)
    }

    fn append_entries_from_leader(
        &mut self,
        leader_term: Term,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: &[LogEntry],
        leader_commit: Index,
    ) -> DbResult<bool> {
        if leader_term < self.current_term {
            return Ok(false);
        }

        if leader_term > self.current_term || self.role == Role::Candidate {
            self.become_follower(leader_term)?;
        }

        if prev_log_index > self.log.last_index() {
            return Ok(false);
        }

        if prev_log_index > 0 && self.log.term_at(prev_log_index) != Some(prev_log_term) {
            return Ok(false);
        }

        let mut first_new_entry_offset = entries.len();
        let mut truncate_from_index = None;

        for (offset, incoming) in entries.iter().enumerate() {
            match self.log.get(incoming.index) {
                Some(existing) if existing.term != incoming.term => {
                    truncate_from_index = Some(incoming.index);
                    first_new_entry_offset = offset;
                    break;
                }
                Some(_) => {}
                None => {
                    first_new_entry_offset = offset;
                    break;
                }
            }
        }

        if let Some(truncate_index) = truncate_from_index {
            if let Some(wal) = &self.wal {
                wal.truncate_to(truncate_index.saturating_sub(1))?;
            }
            self.log.truncate_from(truncate_index);
        }

        if first_new_entry_offset < entries.len() {
            let new_entries = &entries[first_new_entry_offset..];
            if let Some(wal) = &self.wal {
                wal.append_batch(new_entries)?;
            }
            self.log.append_entries(new_entries.iter().cloned());
        }

        let new_commit = leader_commit.min(self.log.last_index());
        self.commit_to(new_commit)?;
        self.election_elapsed = 0;

        Ok(true)
    }

    fn request_vote(&mut self, request: &RequestVote) -> DbResult<VoteResponse> {
        if request.term < self.current_term {
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }

        if request.term > self.current_term {
            self.become_follower(request.term)?;
        }

        let local_last_term = self.log.last_term();
        let local_last_index = self.log.last_index();

        let candidate_up_to_date = request.last_log_term > local_last_term
            || (request.last_log_term == local_last_term
                && request.last_log_index >= local_last_index);

        let can_vote_for_candidate = self.voted_for.is_none()
            || self.voted_for == Some(request.candidate_id)
            || self.id == request.candidate_id;

        let vote_granted = candidate_up_to_date && can_vote_for_candidate;

        if vote_granted {
            self.voted_for = Some(request.candidate_id);
            self.election_elapsed = 0;
            self.clear_election_backoff();
            self.persist_term_vote_metadata()?;
        }

        Ok(VoteResponse {
            term: self.current_term,
            vote_granted,
        })
    }

    fn become_follower(&mut self, new_term: Term) -> DbResult<()> {
        let advanced_term = new_term > self.current_term;

        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = None;
        }

        self.role = Role::Follower;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.clear_election_backoff();

        if advanced_term {
            self.persist_term_vote_metadata()?;
        }

        Ok(())
    }

    fn become_candidate(&mut self) -> DbResult<()> {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.reset_timeout_with_current_backoff();
        self.persist_term_vote_metadata()
    }

    fn become_leader(&mut self) -> DbResult<()> {
        self.role = Role::Leader;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.clear_election_backoff();
        Ok(())
    }

    fn should_send_heartbeat(&mut self, heartbeat_interval_ticks: u64) -> bool {
        if self.role != Role::Leader {
            return false;
        }

        self.heartbeat_elapsed += 1;
        if self.heartbeat_elapsed >= heartbeat_interval_ticks {
            self.heartbeat_elapsed = 0;
            return true;
        }

        false
    }

    fn has_election_timeout(&mut self) -> bool {
        if self.role == Role::Leader {
            return false;
        }

        self.election_elapsed += 1;
        self.election_elapsed >= self.election_timeout_ticks
    }

    fn commit_to(&mut self, commit_index: Index) -> DbResult<()> {
        if commit_index <= self.commit_index {
            return Ok(());
        }

        if let Some(wal) = &self.wal {
            wal.persist_commit_index(commit_index)?;
        }

        self.commit_index = commit_index;
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            apply_entry(&self.log, &mut self.state_machine, next)?;
            self.last_applied = next;
        }

        Ok(())
    }

    fn create_snapshot(&mut self) -> DbResult<Option<SnapshotData>> {
        if self.commit_index == 0 {
            return Ok(None);
        }

        let last_included_index = self.commit_index;
        let last_included_term = self
            .log
            .term_at(last_included_index)
            .ok_or(DbError::MissingLogEntry(last_included_index))?;

        let snapshot = SnapshotData {
            last_included_index,
            last_included_term,
            state: self.state_machine.snapshot(),
        };

        if let Some(wal) = &self.wal {
            wal.persist_snapshot(&snapshot)?;
            wal.truncate_prefix(last_included_index)?;
            wal.persist_commit_index(self.commit_index)?;
        }

        self.log
            .install_snapshot(last_included_index, last_included_term);
        self.snapshot = Some(snapshot.clone());

        Ok(Some(snapshot))
    }

    fn install_snapshot_from_leader(
        &mut self,
        leader_term: Term,
        snapshot: &SnapshotData,
        leader_commit: Index,
    ) -> DbResult<bool> {
        if leader_term < self.current_term {
            return Ok(false);
        }

        if leader_term > self.current_term || self.role == Role::Candidate {
            self.become_follower(leader_term)?;
        }

        if self
            .snapshot
            .as_ref()
            .is_some_and(|local| local.last_included_index >= snapshot.last_included_index)
        {
            return Ok(true);
        }

        if let Some(wal) = &self.wal {
            wal.persist_snapshot(snapshot)?;
            wal.truncate_prefix(snapshot.last_included_index)?;
        }

        self.state_machine
            .replace_with_snapshot(snapshot.state.clone());
        self.log
            .install_snapshot(snapshot.last_included_index, snapshot.last_included_term);
        self.snapshot = Some(snapshot.clone());

        if self.commit_index < snapshot.last_included_index {
            if let Some(wal) = &self.wal {
                wal.persist_commit_index(snapshot.last_included_index)?;
            }
            self.commit_index = snapshot.last_included_index;
        }
        self.last_applied = self.last_applied.max(snapshot.last_included_index);

        let new_commit = leader_commit.min(self.log.last_index());
        self.commit_to(new_commit)?;
        self.election_elapsed = 0;

        Ok(true)
    }

    fn last_index(&self) -> Index {
        self.log.last_index()
    }

    fn last_term(&self) -> Term {
        self.log.last_term()
    }

    fn term_at(&self, index: Index) -> Option<Term> {
        self.log.term_at(index)
    }

    fn entries_from(&self, index: Index) -> Vec<LogEntry> {
        self.log.entries_from(index)
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn commit_index(&self) -> Index {
        self.commit_index
    }

    pub fn log_len(&self) -> usize {
        self.log.len()
    }

    pub fn log_base_index(&self) -> Index {
        self.log.base_index()
    }

    pub fn entry(&self, index: Index) -> Option<&LogEntry> {
        self.log.get(index)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.state_machine.get(key)
    }

    pub fn snapshot(&self) -> BTreeMap<String, String> {
        self.state_machine.snapshot()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalOutcome {
    pub index: Index,
    pub committed_nodes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionOutcome {
    pub term: Term,
    pub votes_received: usize,
    pub elected: bool,
    pub leader_id: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatRoundOutcome {
    pub term: Term,
    pub leader_id: NodeId,
    pub acknowledged_nodes: usize,
    pub quorum: usize,
}

#[derive(Debug)]
pub struct RaftCluster {
    nodes: BTreeMap<NodeId, RaftNode>,
    leader_id: NodeId,
    current_term: Term,
    next_index: BTreeMap<NodeId, Index>,
    match_index: BTreeMap<NodeId, Index>,
    connected_to_leader: BTreeSet<NodeId>,
    heartbeat_interval_ticks: u64,
    clock_ticks: u64,
}

impl RaftCluster {
    pub fn new(node_ids: Vec<NodeId>) -> DbResult<Self> {
        Self::build(node_ids, None)
    }

    pub fn new_durable(node_ids: Vec<NodeId>, wal_dir: impl AsRef<Path>) -> DbResult<Self> {
        Self::build(node_ids, Some(wal_dir.as_ref()))
    }

    fn build(node_ids: Vec<NodeId>, wal_dir: Option<&Path>) -> DbResult<Self> {
        if node_ids.len() < 3 {
            return Err(DbError::ClusterTooSmall);
        }

        let bootstrap_order = node_ids.clone();

        let mut seen = BTreeSet::new();
        for node_id in &node_ids {
            if !seen.insert(*node_id) {
                return Err(DbError::DuplicateNode(*node_id));
            }
        }

        if let Some(dir) = wal_dir {
            fs::create_dir_all(dir)
                .map_err(|error| io_error("failed to create WAL directory", error))?;
        }

        let mut current_term = 1;
        let mut nodes = BTreeMap::new();
        let mut next_index = BTreeMap::new();
        let mut match_index = BTreeMap::new();
        let mut connected_to_leader = BTreeSet::new();

        for node_id in node_ids {
            let node = if let Some(dir) = wal_dir {
                let wal_path = dir.join(format!("node-{node_id}.wal"));
                let wal = Wal::open(wal_path)?;
                RaftNode::new_with_wal(node_id, Role::Follower, current_term, wal)?
            } else {
                RaftNode::new(node_id, Role::Follower, current_term)
            };

            current_term = current_term.max(node.current_term());

            nodes.insert(node_id, node);
            connected_to_leader.insert(node_id);
        }

        for node in nodes.values_mut() {
            if node.current_term < current_term {
                node.current_term = current_term;
                node.voted_for = None;
                node.persist_term_vote_metadata()?;
            }
        }

        // After restart, prefer the node with the most up-to-date committed/log state
        // as bootstrap leader to avoid regressions in visible committed state.
        let mut leader_id = bootstrap_order[0];
        for node_id in bootstrap_order.iter().copied().skip(1) {
            let node = nodes.get(&node_id).ok_or(DbError::NodeNotFound(node_id))?;
            let leader = nodes
                .get(&leader_id)
                .ok_or(DbError::LeaderMissing(leader_id))?;

            let node_rank = (node.commit_index(), node.last_index());
            let leader_rank = (leader.commit_index(), leader.last_index());
            if node_rank > leader_rank {
                leader_id = node_id;
            }
        }

        for (node_id, node) in nodes.iter_mut() {
            node.clear_election_backoff();
            node.role = if *node_id == leader_id {
                Role::Leader
            } else {
                Role::Follower
            };
        }

        let leader_last_index = nodes
            .get(&leader_id)
            .ok_or(DbError::LeaderMissing(leader_id))?
            .last_index();

        for node_id in nodes.keys().copied() {
            next_index.insert(node_id, leader_last_index + 1);
            let matched = if node_id == leader_id {
                leader_last_index
            } else {
                0
            };
            match_index.insert(node_id, matched);
        }

        Ok(Self {
            nodes,
            leader_id,
            current_term,
            next_index,
            match_index,
            connected_to_leader,
            heartbeat_interval_ticks: DEFAULT_HEARTBEAT_INTERVAL_TICKS,
            clock_ticks: 0,
        })
    }

    fn quorum_size(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    fn follower_ids(&self) -> Vec<NodeId> {
        self.nodes
            .keys()
            .copied()
            .filter(|node_id| *node_id != self.leader_id)
            .collect()
    }

    fn leader_available_for_replication(&self) -> DbResult<bool> {
        let leader = self
            .nodes
            .get(&self.leader_id)
            .ok_or(DbError::LeaderMissing(self.leader_id))?;

        Ok(leader.role() == Role::Leader && self.connected_to_leader.contains(&self.leader_id))
    }

    fn reset_replication_state(&mut self) -> DbResult<()> {
        let leader_last_index = self
            .nodes
            .get(&self.leader_id)
            .ok_or(DbError::LeaderMissing(self.leader_id))?
            .last_index();

        for node_id in self.node_ids() {
            if node_id == self.leader_id {
                self.match_index.insert(node_id, leader_last_index);
                self.next_index.insert(node_id, leader_last_index + 1);
            } else {
                self.match_index.insert(node_id, 0);
                self.next_index.insert(node_id, leader_last_index + 1);
            }
        }

        Ok(())
    }

    fn can_exchange_votes(&self, candidate_id: NodeId, voter_id: NodeId) -> bool {
        if candidate_id == voter_id {
            return true;
        }

        self.connected_to_leader.contains(&candidate_id)
            && self.connected_to_leader.contains(&voter_id)
    }

    fn replicate_to_follower(&mut self, follower_id: NodeId) -> DbResult<bool> {
        if follower_id == self.leader_id {
            return Ok(true);
        }

        if !self.connected_to_leader.contains(&follower_id) {
            return Ok(false);
        }

        if !self.leader_available_for_replication()? {
            return Ok(false);
        }

        if !self.nodes.contains_key(&follower_id) {
            return Err(DbError::NodeNotFound(follower_id));
        }

        loop {
            let next_index = *self.next_index.get(&follower_id).unwrap_or(&1);
            let prev_log_index = next_index.saturating_sub(1);

            let (leader_term, leader_base_index, leader_last_index, leader_snapshot, leader_commit) = {
                let leader = self
                    .nodes
                    .get(&self.leader_id)
                    .ok_or(DbError::LeaderMissing(self.leader_id))?;

                (
                    leader.current_term(),
                    leader.log_base_index(),
                    leader.last_index(),
                    leader.snapshot.clone(),
                    leader.commit_index(),
                )
            };

            if next_index > leader_last_index + 1 {
                self.next_index.insert(follower_id, leader_last_index + 1);
                continue;
            }

            if next_index <= leader_base_index {
                let snapshot =
                    leader_snapshot.ok_or(DbError::MissingLogEntry(leader_base_index))?;

                let installed = {
                    let follower = self
                        .nodes
                        .get_mut(&follower_id)
                        .ok_or(DbError::NodeNotFound(follower_id))?;
                    follower.install_snapshot_from_leader(leader_term, &snapshot, leader_commit)?
                };

                if !installed {
                    return Ok(false);
                }

                self.match_index
                    .insert(follower_id, snapshot.last_included_index);
                self.next_index
                    .insert(follower_id, snapshot.last_included_index + 1);
                continue;
            }

            let (prev_log_term, entries) = {
                let leader = self
                    .nodes
                    .get(&self.leader_id)
                    .ok_or(DbError::LeaderMissing(self.leader_id))?;

                let prev_log_term = if prev_log_index == 0 {
                    Some(0)
                } else {
                    leader.term_at(prev_log_index)
                };

                (prev_log_term, leader.entries_from(next_index))
            };

            let Some(prev_log_term) = prev_log_term else {
                if next_index <= 1 {
                    return Ok(false);
                }
                self.next_index.insert(follower_id, next_index - 1);
                continue;
            };

            let success = {
                let follower = self
                    .nodes
                    .get_mut(&follower_id)
                    .ok_or(DbError::NodeNotFound(follower_id))?;

                follower.append_entries_from_leader(
                    leader_term,
                    prev_log_index,
                    prev_log_term,
                    &entries,
                    leader_commit,
                )?
            };

            if success {
                let follower_last_index = self
                    .nodes
                    .get(&follower_id)
                    .ok_or(DbError::NodeNotFound(follower_id))?
                    .last_index();

                self.match_index.insert(follower_id, follower_last_index);
                self.next_index.insert(follower_id, follower_last_index + 1);
                return Ok(true);
            }

            if next_index <= 1 {
                return Ok(false);
            }

            self.next_index.insert(follower_id, next_index - 1);
        }
    }

    fn send_heartbeats(&mut self) -> DbResult<()> {
        let _ = self.heartbeat_round()?;
        Ok(())
    }

    fn advance_leader_commit_index(&mut self) -> DbResult<()> {
        let (leader_last_index, leader_term, current_commit) = {
            let leader = self
                .nodes
                .get(&self.leader_id)
                .ok_or(DbError::LeaderMissing(self.leader_id))?;
            (
                leader.last_index(),
                leader.current_term(),
                leader.commit_index(),
            )
        };

        let quorum = self.quorum_size();
        let mut new_commit = current_commit;

        for candidate in (current_commit + 1..=leader_last_index).rev() {
            let replicated_nodes = self
                .match_index
                .values()
                .filter(|&&match_index| match_index >= candidate)
                .count();

            if replicated_nodes < quorum {
                continue;
            }

            let candidate_term = self
                .nodes
                .get(&self.leader_id)
                .ok_or(DbError::LeaderMissing(self.leader_id))?
                .term_at(candidate)
                .ok_or(DbError::MissingLogEntry(candidate))?;

            if candidate_term == leader_term {
                new_commit = candidate;
                break;
            }
        }

        if new_commit > current_commit {
            let leader = self
                .nodes
                .get_mut(&self.leader_id)
                .ok_or(DbError::LeaderMissing(self.leader_id))?;

            leader.commit_to(new_commit)?;
        }

        Ok(())
    }

    fn replicate_commit_index(&mut self) -> DbResult<()> {
        for follower_id in self.follower_ids() {
            if self.connected_to_leader.contains(&follower_id) {
                self.replicate_to_follower(follower_id)?;
            }
        }
        Ok(())
    }

    pub fn start_election(&mut self, candidate_id: NodeId) -> DbResult<ElectionOutcome> {
        if !self.nodes.contains_key(&candidate_id) {
            return Err(DbError::NodeNotFound(candidate_id));
        }

        let (term, last_log_index, last_log_term) = {
            let candidate = self
                .nodes
                .get_mut(&candidate_id)
                .ok_or(DbError::NodeNotFound(candidate_id))?;
            candidate.become_candidate()?;
            (
                candidate.current_term(),
                candidate.last_index(),
                candidate.last_term(),
            )
        };

        self.current_term = self.current_term.max(term);

        let request = RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        };

        let mut votes_received = 1;

        for voter_id in self.node_ids() {
            if voter_id == candidate_id {
                continue;
            }

            if !self.can_exchange_votes(candidate_id, voter_id) {
                continue;
            }

            let response = {
                let voter = self
                    .nodes
                    .get_mut(&voter_id)
                    .ok_or(DbError::NodeNotFound(voter_id))?;
                voter.request_vote(&request)?
            };

            if response.term > term {
                let candidate = self
                    .nodes
                    .get_mut(&candidate_id)
                    .ok_or(DbError::NodeNotFound(candidate_id))?;
                candidate.become_follower(response.term)?;
                self.current_term = response.term;

                return Ok(ElectionOutcome {
                    term: response.term,
                    votes_received,
                    elected: false,
                    leader_id: self.leader_id,
                });
            }

            if response.vote_granted {
                votes_received += 1;
            }
        }

        if votes_received >= self.quorum_size() {
            self.leader_id = candidate_id;
            self.current_term = term;
            self.connected_to_leader.insert(candidate_id);

            for (node_id, node) in self.nodes.iter_mut() {
                if *node_id == candidate_id {
                    node.become_leader()?;
                } else {
                    node.become_follower(term)?;
                }
            }

            self.reset_replication_state()?;
            self.send_heartbeats()?;

            return Ok(ElectionOutcome {
                term,
                votes_received,
                elected: true,
                leader_id: self.leader_id,
            });
        }

        if let Some(candidate) = self.nodes.get_mut(&candidate_id) {
            candidate.increase_election_backoff();
            candidate.election_elapsed = 0;
        }

        Ok(ElectionOutcome {
            term,
            votes_received,
            elected: false,
            leader_id: self.leader_id,
        })
    }

    pub fn tick(&mut self) -> DbResult<()> {
        self.clock_ticks += 1;

        let should_heartbeat = {
            let leader = self
                .nodes
                .get_mut(&self.leader_id)
                .ok_or(DbError::LeaderMissing(self.leader_id))?;

            if !self.connected_to_leader.contains(&self.leader_id) {
                false
            } else {
                leader.should_send_heartbeat(self.heartbeat_interval_ticks)
            }
        };

        if should_heartbeat {
            self.send_heartbeats()?;
        }

        let mut timed_out_nodes = Vec::new();
        for node_id in self.node_ids() {
            let timed_out = {
                let node = self
                    .nodes
                    .get_mut(&node_id)
                    .ok_or(DbError::NodeNotFound(node_id))?;
                node.has_election_timeout()
            };

            if timed_out {
                timed_out_nodes.push(node_id);
            }
        }

        if let Some(candidate_id) = timed_out_nodes.into_iter().next() {
            let outcome = self.start_election(candidate_id)?;
            if outcome.elected {
                self.send_heartbeats()?;
            }
        }

        Ok(())
    }

    pub fn tick_many(&mut self, ticks: u64) -> DbResult<()> {
        for _ in 0..ticks {
            self.tick()?;
        }
        Ok(())
    }

    pub fn heartbeat_round(&mut self) -> DbResult<HeartbeatRoundOutcome> {
        let leader_id = self.leader_id;
        let term = {
            let leader = self
                .nodes
                .get(&leader_id)
                .ok_or(DbError::LeaderMissing(leader_id))?;
            if leader.role() != Role::Leader {
                return Err(DbError::LeaderMissing(leader_id));
            }
            leader.current_term()
        };

        let mut acknowledged_nodes = 0usize;
        if self.connected_to_leader.contains(&leader_id) {
            acknowledged_nodes += 1;
        }

        for follower_id in self.follower_ids() {
            if !self.connected_to_leader.contains(&follower_id) {
                continue;
            }
            if self.replicate_to_follower(follower_id)? {
                acknowledged_nodes += 1;
            }
        }

        Ok(HeartbeatRoundOutcome {
            term,
            leader_id,
            acknowledged_nodes,
            quorum: self.quorum_size(),
        })
    }

    pub fn disconnect(&mut self, node_id: NodeId) -> DbResult<()> {
        if !self.nodes.contains_key(&node_id) {
            return Err(DbError::NodeNotFound(node_id));
        }

        self.connected_to_leader.remove(&node_id);

        Ok(())
    }

    pub fn reconnect(&mut self, node_id: NodeId) -> DbResult<()> {
        if !self.nodes.contains_key(&node_id) {
            return Err(DbError::NodeNotFound(node_id));
        }

        self.connected_to_leader.insert(node_id);

        if node_id == self.leader_id {
            self.send_heartbeats()?;
        } else {
            self.replicate_to_follower(node_id)?;
            self.replicate_commit_index()?;
        }

        Ok(())
    }

    pub fn is_connected_to_leader(&self, node_id: NodeId) -> DbResult<bool> {
        if !self.nodes.contains_key(&node_id) {
            return Err(DbError::NodeNotFound(node_id));
        }
        Ok(self.connected_to_leader.contains(&node_id))
    }

    pub fn leader_id(&self) -> NodeId {
        self.leader_id
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn clock_ticks(&self) -> u64 {
        self.clock_ticks
    }

    pub fn min_election_timeout_ticks(&self) -> u64 {
        MIN_ELECTION_TIMEOUT_TICKS
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn node(&self, node_id: NodeId) -> DbResult<&RaftNode> {
        self.nodes
            .get(&node_id)
            .ok_or(DbError::NodeNotFound(node_id))
    }

    pub fn propose(&mut self, command: Command) -> DbResult<ProposalOutcome> {
        let leader_term = {
            let leader = self
                .nodes
                .get(&self.leader_id)
                .ok_or(DbError::LeaderMissing(self.leader_id))?;
            if leader.role() != Role::Leader {
                return Err(DbError::LeaderMissing(self.leader_id));
            }
            leader.current_term()
        };

        let appended_index = {
            let leader = self
                .nodes
                .get_mut(&self.leader_id)
                .ok_or(DbError::LeaderMissing(self.leader_id))?;
            leader.append_local_entry(command, leader_term)?.index
        };

        self.match_index.insert(self.leader_id, appended_index);
        self.next_index.insert(self.leader_id, appended_index + 1);

        for follower_id in self.follower_ids() {
            self.replicate_to_follower(follower_id)?;
        }

        self.advance_leader_commit_index()?;
        self.replicate_commit_index()?;

        let committed_nodes = self
            .nodes
            .values()
            .filter(|node| node.commit_index() >= appended_index)
            .count();

        Ok(ProposalOutcome {
            index: appended_index,
            committed_nodes,
        })
    }

    pub fn leader_snapshot(&self) -> DbResult<BTreeMap<String, String>> {
        let leader = self
            .nodes
            .get(&self.leader_id)
            .ok_or(DbError::LeaderMissing(self.leader_id))?;
        Ok(leader.snapshot())
    }

    pub fn create_leader_snapshot(&mut self) -> DbResult<Option<Index>> {
        let snapshot = {
            let leader = self
                .nodes
                .get_mut(&self.leader_id)
                .ok_or(DbError::LeaderMissing(self.leader_id))?;
            leader.create_snapshot()?
        };

        let Some(snapshot) = snapshot else {
            return Ok(None);
        };

        let leader_last_index = self
            .nodes
            .get(&self.leader_id)
            .ok_or(DbError::LeaderMissing(self.leader_id))?
            .last_index();

        self.match_index.insert(self.leader_id, leader_last_index);
        self.next_index
            .insert(self.leader_id, leader_last_index + 1);

        Ok(Some(snapshot.last_included_index))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use db_core::{Command, DbError, LogEntry};

    use super::{
        RaftCluster, Role, DEFAULT_ELECTION_TIMEOUT_BASE_TICKS,
        DEFAULT_ELECTION_TIMEOUT_SPREAD_TICKS, MAX_ELECTION_BACKOFF_EXPONENT,
    };

    fn temp_wal_dir(suffix: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();

        std::env::temp_dir().join(format!(
            "anvildb-raft-wal-{suffix}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn cleanup_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn rejects_clusters_smaller_than_three_nodes() {
        let result = RaftCluster::new(vec![1, 2]);
        assert!(matches!(result, Err(DbError::ClusterTooSmall)));
    }

    #[test]
    fn elects_first_node_as_leader_in_phase_one() {
        let cluster = RaftCluster::new(vec![3, 4, 5]).expect("cluster should build");

        let leader = cluster
            .node(cluster.leader_id())
            .expect("leader node must exist");
        assert_eq!(leader.role(), Role::Leader);
    }

    #[test]
    fn candidate_with_majority_becomes_leader() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        let outcome = cluster.start_election(2).expect("election should execute");
        assert!(outcome.elected);
        assert_eq!(outcome.leader_id, 2);
        assert_eq!(cluster.leader_id(), 2);
        assert_eq!(cluster.current_term(), 2);

        let leader = cluster.node(2).expect("node 2 should exist");
        assert_eq!(leader.role(), Role::Leader);
        assert_eq!(leader.current_term(), 2);

        cluster
            .propose(Command::Put {
                key: "leader".to_owned(),
                value: "node-2".to_owned(),
            })
            .expect("new leader should accept proposals");

        for node_id in cluster.node_ids() {
            let node = cluster.node(node_id).expect("node should exist");
            assert_eq!(node.get("leader"), Some("node-2"));
        }
    }

    #[test]
    fn split_vote_leaves_existing_leader_in_place() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");

        {
            let node_1 = cluster.nodes.get_mut(&1).expect("node 1 should exist");
            node_1.current_term = 2;
            node_1.voted_for = Some(1);

            let node_3 = cluster.nodes.get_mut(&3).expect("node 3 should exist");
            node_3.current_term = 2;
            node_3.voted_for = Some(3);

            let node_4 = cluster.nodes.get_mut(&4).expect("node 4 should exist");
            node_4.current_term = 2;
            node_4.voted_for = Some(4);

            let node_5 = cluster.nodes.get_mut(&5).expect("node 5 should exist");
            node_5.current_term = 2;
            node_5.voted_for = Some(5);
        }

        let outcome = cluster.start_election(2).expect("election should execute");

        assert!(!outcome.elected);
        assert_eq!(outcome.votes_received, 1);
        assert_eq!(cluster.leader_id(), 1);
        assert_eq!(
            cluster.node(2).expect("node 2 should exist").role(),
            Role::Candidate
        );
        assert_eq!(
            cluster.node(1).expect("node 1 should exist").role(),
            Role::Leader
        );
    }

    #[test]
    fn stale_term_rejection_forces_candidate_to_step_down() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        {
            let node_3 = cluster.nodes.get_mut(&3).expect("node 3 should exist");
            node_3.current_term = 5;
            node_3.voted_for = Some(3);
        }

        let outcome = cluster.start_election(2).expect("election should execute");

        assert!(!outcome.elected);
        assert_eq!(outcome.term, 5);

        let candidate = cluster.node(2).expect("node 2 should exist");
        assert_eq!(candidate.role(), Role::Follower);
        assert_eq!(candidate.current_term(), 5);
    }

    #[test]
    fn heartbeats_prevent_unnecessary_elections() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        cluster.tick_many(50).expect("ticks should execute");

        assert_eq!(cluster.leader_id(), 1);
        assert_eq!(cluster.current_term(), 1);
        assert_eq!(
            cluster.node(1).expect("node 1 should exist").role(),
            Role::Leader
        );
        assert_eq!(
            cluster.node(2).expect("node 2 should exist").role(),
            Role::Follower
        );
        assert_eq!(
            cluster.node(3).expect("node 3 should exist").role(),
            Role::Follower
        );
        assert_eq!(cluster.clock_ticks(), 50);
    }

    #[test]
    fn follower_times_out_and_becomes_leader_when_leader_isolated() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        cluster
            .disconnect(1)
            .expect("leader disconnect should succeed");
        cluster.tick_many(20).expect("ticks should execute");

        assert_ne!(cluster.leader_id(), 1);
        let new_leader_id = cluster.leader_id();
        assert_eq!(
            cluster
                .node(new_leader_id)
                .expect("new leader should exist")
                .role(),
            Role::Leader
        );
        assert!(cluster.current_term() > 1);

        let outcome = cluster
            .propose(Command::Put {
                key: "failover".to_owned(),
                value: "ok".to_owned(),
            })
            .expect("new leader should be able to commit with quorum");

        assert_eq!(outcome.committed_nodes, 2);
        assert_eq!(
            cluster
                .node(new_leader_id)
                .expect("new leader should exist")
                .get("failover"),
            Some("ok")
        );
    }

    #[test]
    fn heartbeat_round_reports_acknowledged_nodes() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");

        let initial = cluster.heartbeat_round().expect("heartbeat should succeed");
        assert_eq!(initial.leader_id, 1);
        assert_eq!(initial.quorum, 3);
        assert_eq!(initial.acknowledged_nodes, 5);

        cluster.disconnect(4).expect("node 4 should disconnect");
        cluster.disconnect(5).expect("node 5 should disconnect");

        let degraded = cluster.heartbeat_round().expect("heartbeat should succeed");
        assert_eq!(degraded.quorum, 3);
        assert_eq!(degraded.acknowledged_nodes, 3);

        cluster.disconnect(3).expect("node 3 should disconnect");

        let below_quorum = cluster.heartbeat_round().expect("heartbeat should succeed");
        assert_eq!(below_quorum.quorum, 3);
        assert_eq!(below_quorum.acknowledged_nodes, 2);
    }

    #[test]
    fn durable_cluster_recovers_committed_state_after_restart() {
        let wal_dir = temp_wal_dir("committed-restart");

        {
            let mut cluster =
                RaftCluster::new_durable(vec![1, 2, 3], &wal_dir).expect("cluster should build");

            cluster
                .propose(Command::Put {
                    key: "a".to_owned(),
                    value: "1".to_owned(),
                })
                .expect("first proposal should commit");
            cluster
                .propose(Command::Put {
                    key: "b".to_owned(),
                    value: "2".to_owned(),
                })
                .expect("second proposal should commit");
            cluster
                .propose(Command::Delete {
                    key: "a".to_owned(),
                })
                .expect("delete proposal should commit");

            for node_id in cluster.node_ids() {
                let node = cluster.node(node_id).expect("node should exist");
                assert_eq!(node.commit_index(), 3);
            }
        }

        let mut recovered =
            RaftCluster::new_durable(vec![1, 2, 3], &wal_dir).expect("recovery should succeed");

        for node_id in recovered.node_ids() {
            let node = recovered.node(node_id).expect("node should exist");
            assert_eq!(node.log_len(), 3);
            assert_eq!(node.commit_index(), 3);
            assert_eq!(node.get("a"), None);
            assert_eq!(node.get("b"), Some("2"));
        }

        recovered
            .propose(Command::Put {
                key: "after-restart".to_owned(),
                value: "ok".to_owned(),
            })
            .expect("recovered cluster should keep committing entries");

        cleanup_dir(&wal_dir);
    }

    #[test]
    fn durable_restart_keeps_uncommitted_entries_unapplied() {
        let wal_dir = temp_wal_dir("uncommitted-restart");

        {
            let mut cluster = RaftCluster::new_durable(vec![1, 2, 3, 4, 5], &wal_dir)
                .expect("cluster should build");

            cluster.disconnect(2).expect("node 2 should disconnect");
            cluster.disconnect(3).expect("node 3 should disconnect");
            cluster.disconnect(4).expect("node 4 should disconnect");

            let outcome = cluster
                .propose(Command::Put {
                    key: "x".to_owned(),
                    value: "uncommitted".to_owned(),
                })
                .expect("proposal should append without quorum");
            assert_eq!(outcome.committed_nodes, 0);
            assert_eq!(cluster.node(1).expect("leader exists").commit_index(), 0);
            assert_eq!(cluster.node(1).expect("leader exists").get("x"), None);
        }

        let recovered = RaftCluster::new_durable(vec![1, 2, 3, 4, 5], &wal_dir)
            .expect("recovery should succeed");

        let leader = recovered.node(1).expect("leader should exist");
        assert_eq!(leader.log_len(), 1);
        assert_eq!(leader.commit_index(), 0);
        assert_eq!(leader.get("x"), None);

        let follower_five = recovered.node(5).expect("follower should exist");
        assert_eq!(follower_five.log_len(), 1);
        assert_eq!(follower_five.commit_index(), 0);
        assert_eq!(follower_five.get("x"), None);

        cleanup_dir(&wal_dir);
    }

    #[test]
    fn durable_restart_preserves_term_vote_metadata_for_election_safety() {
        let wal_dir = temp_wal_dir("term-vote-restart");

        {
            let mut cluster =
                RaftCluster::new_durable(vec![1, 2, 3, 4], &wal_dir).expect("cluster should build");

            cluster.disconnect(1).expect("node 1 should disconnect");
            cluster.disconnect(4).expect("node 4 should disconnect");

            let outcome = cluster.start_election(2).expect("election should execute");
            assert!(!outcome.elected);
            assert_eq!(outcome.term, 2);
            assert_eq!(outcome.votes_received, 2);
        }

        let mut recovered =
            RaftCluster::new_durable(vec![1, 2, 3, 4], &wal_dir).expect("recovery should succeed");

        assert_eq!(recovered.current_term(), 2);
        for node_id in recovered.node_ids() {
            let node = recovered.node(node_id).expect("node should exist");
            assert_eq!(node.current_term(), 2);
        }

        {
            let node_four = recovered.nodes.get_mut(&4).expect("node 4 should exist");
            node_four.current_term = 1;
        }

        let same_term_outcome = recovered
            .start_election(4)
            .expect("election should execute");
        assert!(!same_term_outcome.elected);
        assert_eq!(same_term_outcome.term, 2);
        assert_eq!(same_term_outcome.votes_received, 2);
        assert_eq!(recovered.leader_id(), 1);

        cleanup_dir(&wal_dir);
    }

    #[test]
    fn durable_snapshot_compaction_survives_restart() {
        let wal_dir = temp_wal_dir("snapshot-restart");

        {
            let mut cluster =
                RaftCluster::new_durable(vec![1, 2, 3], &wal_dir).expect("cluster should build");

            for index in 1..=6 {
                cluster
                    .propose(Command::Put {
                        key: format!("k{index}"),
                        value: format!("v{index}"),
                    })
                    .expect("proposal should commit");
            }

            let snapshot_index = cluster
                .create_leader_snapshot()
                .expect("snapshot creation should succeed")
                .expect("snapshot should be created");
            assert_eq!(snapshot_index, 6);

            let leader = cluster.node(1).expect("leader should exist");
            assert_eq!(leader.log_base_index(), 6);
            assert_eq!(leader.log_len(), 0);

            cluster
                .propose(Command::Put {
                    key: "after".to_owned(),
                    value: "snapshot".to_owned(),
                })
                .expect("proposal after snapshot should commit");
        }

        let recovered =
            RaftCluster::new_durable(vec![1, 2, 3], &wal_dir).expect("recovery should succeed");

        let recovered_leader = recovered.node(1).expect("leader should exist");
        assert_eq!(recovered_leader.log_base_index(), 6);
        assert_eq!(recovered_leader.log_len(), 1);
        assert_eq!(recovered_leader.commit_index(), 7);
        assert_eq!(recovered_leader.get("k1"), Some("v1"));
        assert_eq!(recovered_leader.get("k6"), Some("v6"));
        assert_eq!(recovered_leader.get("after"), Some("snapshot"));

        cleanup_dir(&wal_dir);
    }

    #[test]
    fn reconnecting_lagging_follower_installs_snapshot_after_compaction() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        cluster.disconnect(3).expect("node 3 should disconnect");

        for index in 1..=5 {
            cluster
                .propose(Command::Put {
                    key: format!("k{index}"),
                    value: format!("v{index}"),
                })
                .expect("proposal should commit");
        }

        let snapshot_index = cluster
            .create_leader_snapshot()
            .expect("snapshot creation should succeed")
            .expect("snapshot should be created");
        assert_eq!(snapshot_index, 5);

        cluster
            .propose(Command::Put {
                key: "k6".to_owned(),
                value: "v6".to_owned(),
            })
            .expect("post-snapshot proposal should commit");

        cluster.reconnect(3).expect("node 3 should reconnect");

        let follower = cluster.node(3).expect("node 3 should exist");
        assert_eq!(follower.log_base_index(), 5);
        assert_eq!(follower.commit_index(), 6);
        assert_eq!(follower.get("k1"), Some("v1"));
        assert_eq!(follower.get("k5"), Some("v5"));
        assert_eq!(follower.get("k6"), Some("v6"));
    }

    #[test]
    fn repeated_failed_elections_increase_backoff_and_success_resets_it() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");

        cluster.disconnect(3).expect("node 3 should disconnect");
        cluster.disconnect(4).expect("node 4 should disconnect");
        cluster.disconnect(5).expect("node 5 should disconnect");

        let initial_timeout = cluster
            .nodes
            .get(&2)
            .expect("node 2 should exist")
            .election_timeout_ticks;

        let first = cluster.start_election(2).expect("election should execute");
        assert!(!first.elected);

        let timeout_after_first = cluster
            .nodes
            .get(&2)
            .expect("node 2 should exist")
            .election_timeout_ticks;
        assert!(timeout_after_first > initial_timeout);

        let second = cluster.start_election(2).expect("election should execute");
        assert!(!second.elected);

        let timeout_after_second = cluster
            .nodes
            .get(&2)
            .expect("node 2 should exist")
            .election_timeout_ticks;
        assert!(timeout_after_second >= timeout_after_first);

        for _ in 0..10 {
            let _ = cluster.start_election(2).expect("election should execute");
        }

        let candidate = cluster.nodes.get(&2).expect("node 2 should exist");
        assert_eq!(
            candidate.election_backoff_rounds,
            MAX_ELECTION_BACKOFF_EXPONENT
        );
        let max_base =
            DEFAULT_ELECTION_TIMEOUT_BASE_TICKS * (1_u64 << MAX_ELECTION_BACKOFF_EXPONENT);
        assert!(candidate.election_timeout_ticks >= max_base);
        assert!(
            candidate.election_timeout_ticks
                < max_base + DEFAULT_ELECTION_TIMEOUT_SPREAD_TICKS.max(1)
        );

        cluster.reconnect(3).expect("node 3 should reconnect");
        cluster.reconnect(4).expect("node 4 should reconnect");
        let success = cluster.start_election(2).expect("election should execute");
        assert!(success.elected);
        assert_eq!(
            cluster
                .nodes
                .get(&2)
                .expect("node 2 should exist")
                .election_backoff_rounds,
            0
        );
    }

    #[test]
    fn minority_timeout_storm_does_not_unseat_stable_leader() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");

        cluster.disconnect(3).expect("node 3 should disconnect");
        cluster.disconnect(4).expect("node 4 should disconnect");
        cluster.disconnect(5).expect("node 5 should disconnect");

        cluster.tick_many(40).expect("ticks should execute");

        assert_eq!(cluster.leader_id(), 1);
        assert_eq!(
            cluster.node(1).expect("leader should exist").role(),
            Role::Leader
        );
        assert_eq!(
            cluster.node(1).expect("leader should exist").current_term(),
            1
        );

        let outcome = cluster
            .propose(Command::Put {
                key: "majority".to_owned(),
                value: "ok".to_owned(),
            })
            .expect("majority side should still commit");

        assert_eq!(outcome.committed_nodes, 0);
        assert_eq!(
            cluster
                .node(1)
                .expect("node 1 should exist")
                .get("majority"),
            None
        );
        assert_eq!(
            cluster
                .node(2)
                .expect("node 2 should exist")
                .get("majority"),
            None
        );
        assert_eq!(
            cluster
                .node(3)
                .expect("node 3 should exist")
                .get("majority"),
            None
        );
        assert_eq!(
            cluster
                .node(4)
                .expect("node 4 should exist")
                .get("majority"),
            None
        );
        assert_eq!(
            cluster
                .node(5)
                .expect("node 5 should exist")
                .get("majority"),
            None
        );
    }

    #[test]
    fn majority_partition_timeout_elects_new_leader_and_reconciles_old_leader() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");

        cluster.disconnect(1).expect("node 1 should disconnect");
        cluster.disconnect(5).expect("node 5 should disconnect");

        cluster.tick_many(10).expect("ticks should execute");

        let new_leader_id = cluster.leader_id();
        assert_ne!(new_leader_id, 1);
        assert!([2, 3, 4].contains(&new_leader_id));
        assert_eq!(
            cluster
                .node(new_leader_id)
                .expect("new leader should exist")
                .role(),
            Role::Leader
        );

        let outcome = cluster
            .propose(Command::Put {
                key: "partition".to_owned(),
                value: "majority".to_owned(),
            })
            .expect("majority partition should commit");
        assert_eq!(outcome.committed_nodes, 3);
        assert_eq!(
            cluster
                .node(1)
                .expect("node 1 should exist")
                .get("partition"),
            None
        );

        cluster.reconnect(1).expect("node 1 should reconnect");

        let recovered_old_leader = cluster.node(1).expect("node 1 should exist");
        assert_eq!(recovered_old_leader.get("partition"), Some("majority"));
        assert_eq!(recovered_old_leader.commit_index(), 1);
    }

    #[test]
    fn replicates_put_to_all_nodes() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        let outcome = cluster
            .propose(Command::Put {
                key: "x".to_owned(),
                value: "1".to_owned(),
            })
            .expect("proposal should commit");

        assert_eq!(outcome.committed_nodes, 3);

        for node_id in cluster.node_ids() {
            let node = cluster.node(node_id).expect("node should exist");
            assert_eq!(node.get("x"), Some("1"));
            assert_eq!(node.log_len(), 1);
            assert_eq!(node.commit_index(), 1);
        }
    }

    #[test]
    fn replicates_delete_to_all_nodes() {
        let mut cluster = RaftCluster::new(vec![7, 8, 9]).expect("cluster should build");

        cluster
            .propose(Command::Put {
                key: "doomed".to_owned(),
                value: "value".to_owned(),
            })
            .expect("put should commit");

        cluster
            .propose(Command::Delete {
                key: "doomed".to_owned(),
            })
            .expect("delete should commit");

        for node_id in cluster.node_ids() {
            let node = cluster.node(node_id).expect("node should exist");
            assert_eq!(node.get("doomed"), None);
            assert_eq!(node.log_len(), 2);
            assert_eq!(node.commit_index(), 2);
        }
    }

    #[test]
    fn does_not_commit_without_quorum() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");

        cluster.disconnect(2).expect("node 2 should disconnect");
        cluster.disconnect(3).expect("node 3 should disconnect");
        cluster.disconnect(4).expect("node 4 should disconnect");

        let outcome = cluster
            .propose(Command::Put {
                key: "x".to_owned(),
                value: "uncommitted".to_owned(),
            })
            .expect("proposal should be appended");

        assert_eq!(outcome.index, 1);
        assert_eq!(outcome.committed_nodes, 0);

        let leader = cluster.node(1).expect("leader exists");
        assert_eq!(leader.log_len(), 1);
        assert_eq!(leader.commit_index(), 0);
        assert_eq!(leader.get("x"), None);
    }

    #[test]
    fn reconnect_catches_up_lagging_follower() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        cluster.disconnect(3).expect("node 3 should disconnect");
        assert!(!cluster
            .is_connected_to_leader(3)
            .expect("connection state should be readable"));

        cluster
            .propose(Command::Put {
                key: "a".to_owned(),
                value: "1".to_owned(),
            })
            .expect("first proposal should commit");

        cluster
            .propose(Command::Put {
                key: "b".to_owned(),
                value: "2".to_owned(),
            })
            .expect("second proposal should commit");

        let lagging = cluster.node(3).expect("node 3 exists");
        assert_eq!(lagging.log_len(), 0);
        assert_eq!(lagging.commit_index(), 0);

        cluster.reconnect(3).expect("node 3 should reconnect");

        let recovered = cluster.node(3).expect("node 3 exists");
        assert_eq!(recovered.log_len(), 2);
        assert_eq!(recovered.commit_index(), 2);
        assert_eq!(recovered.get("a"), Some("1"));
        assert_eq!(recovered.get("b"), Some("2"));
    }

    #[test]
    fn conflicting_follower_entry_is_repaired_on_reconnect() {
        let mut cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");

        cluster
            .propose(Command::Put {
                key: "seed".to_owned(),
                value: "ok".to_owned(),
            })
            .expect("seed proposal should commit");

        cluster.disconnect(3).expect("node 3 should disconnect");

        cluster
            .propose(Command::Put {
                key: "target".to_owned(),
                value: "authoritative".to_owned(),
            })
            .expect("authoritative proposal should commit");

        {
            let follower = cluster.nodes.get_mut(&3).expect("node 3 should exist");
            follower.log.truncate_from(2);
            follower.log.append(LogEntry {
                term: 99,
                index: 2,
                command: Command::Put {
                    key: "target".to_owned(),
                    value: "stale".to_owned(),
                },
            });
        }

        cluster.reconnect(3).expect("node 3 should reconnect");

        let repaired = cluster.node(3).expect("node 3 exists");
        assert_eq!(repaired.log_len(), 2);
        assert_eq!(repaired.commit_index(), 2);
        assert_eq!(repaired.get("target"), Some("authoritative"));
    }
}
