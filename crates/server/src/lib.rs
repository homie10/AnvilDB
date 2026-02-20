use std::collections::BTreeMap;
use std::fmt;

use db_core::{Command, DbResult, Index, NodeId, Term};
use db_raft::{HeartbeatRoundOutcome, RaftCluster};
use db_sql::{
    column_headers, evaluate_predicate, lower_plan_for_kv, plan_sql, project_row, ColumnName,
    KvExecutionPlan, MvccCatalog, Predicate, Timestamp,
};

const DEFAULT_PROPOSAL_TIMEOUT_TICKS: u64 = 8;
const DEFAULT_READ_TIMEOUT_TICKS: u64 = 4;
const DEFAULT_READ_LEASE_TICKS: u64 = 3;
const DEFAULT_MAX_PENDING_ENTRIES: u64 = 64;
const DEFAULT_GRPC_METHOD_PATH: &str = "/anvildb.v1.Api/Handle";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAddress {
    pub node_id: NodeId,
    pub endpoint: String,
}

impl NodeAddress {
    pub fn new(node_id: NodeId, endpoint: impl Into<String>) -> Self {
        Self {
            node_id,
            endpoint: endpoint.into(),
        }
    }
}

fn default_node_address(node_id: NodeId) -> NodeAddress {
    NodeAddress::new(node_id, format!("node-{node_id}.anvildb.internal"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    Eventual,
    Linearizable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {
    Write(Command),
    Read {
        key: String,
        consistency: ReadConsistency,
    },
    Sql {
        query: String,
        read_consistency: ReadConsistency,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEnvelope {
    pub request_id: u64,
    pub from: NodeAddress,
    pub to: NodeAddress,
    pub timeout_ticks: Option<u64>,
    pub request: ClientRequest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutOperation {
    Proposal,
    LinearizableRead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResponse {
    WriteCommitted {
        index: Index,
        committed_nodes: usize,
    },
    ReadResult {
        value: Option<String>,
        consistency: ReadConsistency,
    },
    Redirect {
        leader: NodeAddress,
    },
    TimedOut {
        operation: TimeoutOperation,
        index: Option<Index>,
        observed_nodes: usize,
    },
    Overloaded {
        pending_entries: u64,
        limit: u64,
    },
    RouteError {
        message: String,
    },
    InvalidQuery {
        message: String,
    },
    SqlRows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        consistency: ReadConsistency,
    },
    SqlWriteResult {
        affected_rows: usize,
        last_index: Option<Index>,
        committed_nodes: usize,
    },
    InternalError {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseEnvelope {
    pub request_id: u64,
    pub from: NodeAddress,
    pub to: NodeAddress,
    pub response: ClientResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportConfig {
    pub proposal_timeout_ticks: u64,
    pub read_timeout_ticks: u64,
    pub read_lease_ticks: u64,
    pub max_pending_entries: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            proposal_timeout_ticks: DEFAULT_PROPOSAL_TIMEOUT_TICKS,
            read_timeout_ticks: DEFAULT_READ_TIMEOUT_TICKS,
            read_lease_ticks: DEFAULT_READ_LEASE_TICKS,
            max_pending_entries: DEFAULT_MAX_PENDING_ENTRIES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadLease {
    leader_id: NodeId,
    term: Term,
    issued_at_tick: u64,
    expires_at_tick: u64,
    acknowledged_nodes: usize,
}

#[derive(Debug)]
pub struct ApiService {
    cluster: RaftCluster,
    addresses: BTreeMap<NodeId, NodeAddress>,
    config: TransportConfig,
    read_lease: Option<ReadLease>,
    mvcc: MvccCatalog,
    mvcc_last_applied_index: Index,
}

impl ApiService {
    pub fn new(cluster: RaftCluster) -> Self {
        Self::with_config(cluster, TransportConfig::default())
    }

    pub fn with_config(cluster: RaftCluster, config: TransportConfig) -> Self {
        let config = Self::harden_transport_config(&cluster, config);
        let mut addresses = BTreeMap::new();
        for node_id in cluster.node_ids() {
            addresses.insert(node_id, default_node_address(node_id));
        }

        let (mvcc, mvcc_last_applied_index) = Self::bootstrap_mvcc_from_cluster(&cluster);

        Self {
            cluster,
            addresses,
            config,
            read_lease: None,
            mvcc,
            mvcc_last_applied_index,
        }
    }

    pub fn cluster(&self) -> &RaftCluster {
        &self.cluster
    }

    pub fn cluster_mut(&mut self) -> &mut RaftCluster {
        &mut self.cluster
    }

    pub fn in_process_transport(&mut self) -> InProcessTransport<'_> {
        InProcessTransport::new(self)
    }

    pub fn address_for(&self, node_id: NodeId) -> Option<&NodeAddress> {
        self.addresses.get(&node_id)
    }

    pub fn leader_address(&self) -> NodeAddress {
        self.addresses
            .get(&self.cluster.leader_id())
            .cloned()
            .unwrap_or_else(|| default_node_address(self.cluster.leader_id()))
    }

    pub fn handle(&mut self, envelope: RequestEnvelope) -> ResponseEnvelope {
        let from = self
            .address_for(envelope.to.node_id)
            .cloned()
            .unwrap_or_else(|| envelope.to.clone());

        if !self.is_known_address(&envelope.to) {
            return ResponseEnvelope {
                request_id: envelope.request_id,
                from,
                to: envelope.from,
                response: ClientResponse::RouteError {
                    message: format!(
                        "unknown destination {} ({})",
                        envelope.to.node_id, envelope.to.endpoint
                    ),
                },
            };
        }

        let response = match envelope.request {
            ClientRequest::Write(command) => {
                self.handle_write(envelope.to.node_id, command, envelope.timeout_ticks)
            }
            ClientRequest::Read { key, consistency } => self.handle_read(
                envelope.to.node_id,
                key,
                consistency,
                envelope.timeout_ticks,
            ),
            ClientRequest::Sql {
                query,
                read_consistency,
            } => self.handle_sql(
                envelope.to.node_id,
                query,
                read_consistency,
                envelope.timeout_ticks,
            ),
        };

        ResponseEnvelope {
            request_id: envelope.request_id,
            from,
            to: envelope.from,
            response,
        }
    }

    fn is_known_address(&self, destination: &NodeAddress) -> bool {
        self.addresses
            .get(&destination.node_id)
            .is_some_and(|known| known == destination)
    }

    fn bootstrap_mvcc_from_cluster(cluster: &RaftCluster) -> (MvccCatalog, Index) {
        let mut mvcc = MvccCatalog::default();

        let leader_id = cluster.leader_id();
        let Ok(leader) = cluster.node(leader_id) else {
            return (mvcc, 0);
        };

        let commit_index = leader.commit_index();
        for (key, value) in leader.snapshot() {
            mvcc.apply_committed_put(key, value, 0, 0);
        }

        (mvcc, commit_index)
    }

    fn harden_transport_config(
        cluster: &RaftCluster,
        mut config: TransportConfig,
    ) -> TransportConfig {
        // Keep lease windows below election timeout so stale leaders cannot
        // indefinitely serve linearizable reads with old quorum evidence.
        let safe_lease_cap = cluster.min_election_timeout_ticks().saturating_sub(1);
        config.read_lease_ticks = config.read_lease_ticks.min(safe_lease_cap);
        config
    }

    fn quorum_size(&self) -> usize {
        (self.cluster.node_ids().len() / 2) + 1
    }

    fn proposal_timeout(&self, requested_timeout: Option<u64>) -> u64 {
        requested_timeout.unwrap_or(self.config.proposal_timeout_ticks)
    }

    fn read_timeout(&self, requested_timeout: Option<u64>) -> u64 {
        requested_timeout.unwrap_or(self.config.read_timeout_ticks)
    }

    fn committed_nodes_at(&self, index: Index) -> DbResult<usize> {
        let mut committed_nodes = 0usize;
        for node_id in self.cluster.node_ids() {
            let node = self.cluster.node(node_id)?;
            if node.commit_index() >= index {
                committed_nodes += 1;
            }
        }
        Ok(committed_nodes)
    }

    fn pending_entries(&self) -> DbResult<u64> {
        let leader = self.cluster.node(self.cluster.leader_id())?;
        let last_index = leader.log_base_index() + (leader.log_len() as u64);
        Ok(last_index.saturating_sub(leader.commit_index()))
    }

    fn invalidate_stale_read_lease(&mut self) {
        let Some(lease) = self.read_lease else {
            return;
        };

        let now = self.cluster.clock_ticks();
        let lease_still_valid = lease.leader_id == self.cluster.leader_id()
            && lease.term == self.cluster.current_term()
            && lease.acknowledged_nodes >= self.quorum_size()
            && now >= lease.issued_at_tick
            && now <= lease.expires_at_tick;

        if !lease_still_valid {
            self.read_lease = None;
        }
    }

    fn active_read_lease(&mut self) -> Option<ReadLease> {
        self.invalidate_stale_read_lease();
        self.read_lease
    }

    fn record_read_lease(&mut self, heartbeat: HeartbeatRoundOutcome) {
        if self.config.read_lease_ticks == 0 || heartbeat.acknowledged_nodes < heartbeat.quorum {
            self.read_lease = None;
            return;
        }

        let issued_at_tick = self.cluster.clock_ticks();
        let expires_at_tick = self
            .cluster
            .clock_ticks()
            .saturating_add(self.config.read_lease_ticks);

        self.read_lease = Some(ReadLease {
            leader_id: heartbeat.leader_id,
            term: heartbeat.term,
            issued_at_tick,
            expires_at_tick,
            acknowledged_nodes: heartbeat.acknowledged_nodes,
        });
    }

    fn apply_mvcc_committed_command(&mut self, command: &Command, committed_index: Index) {
        let timestamp = committed_index as Timestamp;
        let writer_txn = committed_index;
        match command {
            Command::Put { key, value } => {
                self.mvcc
                    .apply_committed_put(key.clone(), value.clone(), timestamp, writer_txn)
            }
            Command::Delete { key } => {
                self.mvcc
                    .apply_committed_delete(key.clone(), timestamp, writer_txn);
            }
        }

        self.mvcc_last_applied_index = self.mvcc_last_applied_index.max(committed_index);
    }

    fn rebuild_mvcc_from_leader_snapshot(
        &mut self,
        timestamp: Timestamp,
    ) -> Result<(), ClientResponse> {
        let leader_id = self.cluster.leader_id();
        let leader =
            self.cluster
                .node(leader_id)
                .map_err(|error| ClientResponse::InternalError {
                    message: error.to_string(),
                })?;

        let mut rebuilt = MvccCatalog::default();
        for (key, value) in leader.snapshot() {
            rebuilt.apply_committed_put(key, value, timestamp, 0);
        }
        self.mvcc = rebuilt;
        self.mvcc_last_applied_index = timestamp as Index;
        Ok(())
    }

    fn ensure_mvcc_through(&mut self, required_index: Index) -> Result<(), ClientResponse> {
        if required_index > self.mvcc_last_applied_index {
            self.rebuild_mvcc_from_leader_snapshot(required_index as Timestamp)?;
        }
        Ok(())
    }

    fn read_timestamp_for_request(
        &mut self,
        destination_node_id: NodeId,
        consistency: ReadConsistency,
        timeout_ticks: Option<u64>,
    ) -> Result<Timestamp, ClientResponse> {
        match consistency {
            ReadConsistency::Eventual => self
                .cluster
                .node(destination_node_id)
                .map(|node| node.commit_index() as Timestamp)
                .map_err(|error| ClientResponse::InternalError {
                    message: error.to_string(),
                }),
            ReadConsistency::Linearizable => {
                let leader_id =
                    self.ensure_linearizable_read_lease(destination_node_id, timeout_ticks)?;
                self.cluster
                    .node(leader_id)
                    .map(|node| node.commit_index() as Timestamp)
                    .map_err(|error| ClientResponse::InternalError {
                        message: error.to_string(),
                    })
            }
        }
    }

    fn handle_write(
        &mut self,
        destination_node_id: NodeId,
        command: Command,
        timeout_ticks: Option<u64>,
    ) -> ClientResponse {
        self.invalidate_stale_read_lease();
        let committed_command = command.clone();

        let leader_id = self.cluster.leader_id();
        if destination_node_id != leader_id {
            return ClientResponse::Redirect {
                leader: self.leader_address(),
            };
        }

        let pending_entries = match self.pending_entries() {
            Ok(value) => value,
            Err(error) => {
                return ClientResponse::InternalError {
                    message: error.to_string(),
                };
            }
        };

        if pending_entries >= self.config.max_pending_entries {
            return ClientResponse::Overloaded {
                pending_entries,
                limit: self.config.max_pending_entries,
            };
        }

        let proposal = match self.cluster.propose(command) {
            Ok(outcome) => outcome,
            Err(error) => {
                return ClientResponse::InternalError {
                    message: error.to_string(),
                };
            }
        };

        let quorum = self.quorum_size();
        if proposal.committed_nodes >= quorum {
            self.apply_mvcc_committed_command(&committed_command, proposal.index);
            return ClientResponse::WriteCommitted {
                index: proposal.index,
                committed_nodes: proposal.committed_nodes,
            };
        }

        let mut committed_nodes = proposal.committed_nodes;
        for _ in 0..self.proposal_timeout(timeout_ticks) {
            if let Err(error) = self.cluster.tick() {
                return ClientResponse::InternalError {
                    message: error.to_string(),
                };
            }

            committed_nodes = match self.committed_nodes_at(proposal.index) {
                Ok(value) => value,
                Err(error) => {
                    return ClientResponse::InternalError {
                        message: error.to_string(),
                    };
                }
            };

            if committed_nodes >= quorum {
                self.apply_mvcc_committed_command(&committed_command, proposal.index);
                return ClientResponse::WriteCommitted {
                    index: proposal.index,
                    committed_nodes,
                };
            }
        }

        ClientResponse::TimedOut {
            operation: TimeoutOperation::Proposal,
            index: Some(proposal.index),
            observed_nodes: committed_nodes,
        }
    }

    fn handle_read(
        &mut self,
        destination_node_id: NodeId,
        key: String,
        consistency: ReadConsistency,
        timeout_ticks: Option<u64>,
    ) -> ClientResponse {
        let read_ts = match self.read_timestamp_for_request(
            destination_node_id,
            consistency,
            timeout_ticks,
        ) {
            Ok(read_ts) => read_ts,
            Err(response) => return response,
        };
        if let Err(response) = self.ensure_mvcc_through(read_ts as Index) {
            return response;
        }

        let value = self.mvcc.read_visible(&key, read_ts).map(str::to_owned);
        ClientResponse::ReadResult { value, consistency }
    }

    fn handle_sql(
        &mut self,
        destination_node_id: NodeId,
        query: String,
        read_consistency: ReadConsistency,
        timeout_ticks: Option<u64>,
    ) -> ClientResponse {
        let logical_plan = match plan_sql(&query) {
            Ok(plan) => plan,
            Err(error) => {
                return ClientResponse::InvalidQuery {
                    message: error.to_string(),
                };
            }
        };

        let execution_plan = match lower_plan_for_kv(&logical_plan) {
            Ok(plan) => plan,
            Err(error) => {
                return ClientResponse::InvalidQuery {
                    message: error.to_string(),
                };
            }
        };

        match execution_plan {
            KvExecutionPlan::Select { columns, predicate } => self.execute_sql_select(
                destination_node_id,
                columns,
                predicate,
                read_consistency,
                timeout_ticks,
            ),
            KvExecutionPlan::Insert { key, value } => self.execute_sql_single_write(
                destination_node_id,
                Command::Put { key, value },
                timeout_ticks,
            ),
            KvExecutionPlan::Delete { predicate } => self.execute_sql_predicate_mutation(
                destination_node_id,
                predicate,
                None,
                timeout_ticks,
            ),
            KvExecutionPlan::Update { value, predicate } => self.execute_sql_predicate_mutation(
                destination_node_id,
                predicate,
                Some(value),
                timeout_ticks,
            ),
        }
    }

    fn ensure_linearizable_read_lease(
        &mut self,
        destination_node_id: NodeId,
        timeout_ticks: Option<u64>,
    ) -> Result<NodeId, ClientResponse> {
        let leader_id = self.cluster.leader_id();
        if destination_node_id != leader_id {
            return Err(ClientResponse::Redirect {
                leader: self.leader_address(),
            });
        }

        if let Some(lease) = self.active_read_lease() {
            return Ok(lease.leader_id);
        }

        let timeout = self.read_timeout(timeout_ticks);
        let mut observed_nodes = 0usize;

        for attempt in 0..=timeout {
            if destination_node_id != self.cluster.leader_id() {
                return Err(ClientResponse::Redirect {
                    leader: self.leader_address(),
                });
            }

            let heartbeat =
                self.cluster
                    .heartbeat_round()
                    .map_err(|error| ClientResponse::InternalError {
                        message: error.to_string(),
                    })?;
            observed_nodes = heartbeat.acknowledged_nodes;

            if heartbeat.acknowledged_nodes >= heartbeat.quorum {
                self.record_read_lease(heartbeat);
                return Ok(heartbeat.leader_id);
            }

            if attempt < timeout {
                self.cluster
                    .tick()
                    .map_err(|error| ClientResponse::InternalError {
                        message: error.to_string(),
                    })?;
                self.invalidate_stale_read_lease();
            }
        }

        Err(ClientResponse::TimedOut {
            operation: TimeoutOperation::LinearizableRead,
            index: None,
            observed_nodes,
        })
    }

    fn execute_sql_select(
        &mut self,
        destination_node_id: NodeId,
        columns: Vec<ColumnName>,
        predicate: Option<Predicate>,
        read_consistency: ReadConsistency,
        timeout_ticks: Option<u64>,
    ) -> ClientResponse {
        let read_ts = match self.read_timestamp_for_request(
            destination_node_id,
            read_consistency,
            timeout_ticks,
        ) {
            Ok(read_ts) => read_ts,
            Err(response) => return response,
        };
        if let Err(response) = self.ensure_mvcc_through(read_ts as Index) {
            return response;
        }

        let mut rows = Vec::new();
        for (key, value) in self.mvcc.visible_rows(read_ts) {
            if predicate
                .as_ref()
                .is_some_and(|predicate| !evaluate_predicate(&key, &value, predicate))
            {
                continue;
            }

            rows.push(project_row(&key, &value, &columns));
        }

        ClientResponse::SqlRows {
            columns: column_headers(&columns),
            rows,
            consistency: read_consistency,
        }
    }

    fn execute_sql_single_write(
        &mut self,
        destination_node_id: NodeId,
        command: Command,
        timeout_ticks: Option<u64>,
    ) -> ClientResponse {
        match self.handle_write(destination_node_id, command, timeout_ticks) {
            ClientResponse::WriteCommitted {
                index,
                committed_nodes,
            } => ClientResponse::SqlWriteResult {
                affected_rows: 1,
                last_index: Some(index),
                committed_nodes,
            },
            other => other,
        }
    }

    fn execute_sql_predicate_mutation(
        &mut self,
        destination_node_id: NodeId,
        predicate: Predicate,
        updated_value: Option<String>,
        timeout_ticks: Option<u64>,
    ) -> ClientResponse {
        let leader_id = self.cluster.leader_id();
        if destination_node_id != leader_id {
            return ClientResponse::Redirect {
                leader: self.leader_address(),
            };
        }

        let leader_commit_index = match self.cluster.node(leader_id) {
            Ok(node) => node.commit_index(),
            Err(error) => {
                return ClientResponse::InternalError {
                    message: error.to_string(),
                };
            }
        };
        if let Err(response) = self.ensure_mvcc_through(leader_commit_index) {
            return response;
        }

        let matching_keys: Vec<String> = self
            .mvcc
            .visible_rows(leader_commit_index as Timestamp)
            .iter()
            .filter_map(|(key, value)| {
                if evaluate_predicate(key, value, &predicate) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        if matching_keys.is_empty() {
            return ClientResponse::SqlWriteResult {
                affected_rows: 0,
                last_index: None,
                committed_nodes: 0,
            };
        }

        let mut affected_rows = 0usize;
        let mut last_index = None;
        let mut committed_nodes = 0usize;

        for key in matching_keys {
            let command = if let Some(value) = &updated_value {
                Command::Put {
                    key,
                    value: value.clone(),
                }
            } else {
                Command::Delete { key }
            };

            match self.handle_write(leader_id, command, timeout_ticks) {
                ClientResponse::WriteCommitted {
                    index,
                    committed_nodes: committed,
                } => {
                    affected_rows += 1;
                    last_index = Some(index);
                    committed_nodes = committed;
                }
                other => return other,
            }
        }

        ClientResponse::SqlWriteResult {
            affected_rows,
            last_index,
            committed_nodes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    Remote(String),
}

pub trait Transport {
    fn round_trip(&mut self, request: RequestEnvelope) -> Result<ResponseEnvelope, TransportError>;
}

pub struct InProcessTransport<'a> {
    service: &'a mut ApiService,
}

impl<'a> InProcessTransport<'a> {
    pub fn new(service: &'a mut ApiService) -> Self {
        Self { service }
    }
}

impl Transport for InProcessTransport<'_> {
    fn round_trip(&mut self, request: RequestEnvelope) -> Result<ResponseEnvelope, TransportError> {
        Ok(self.service.handle(request))
    }
}

pub trait GrpcEnvelopeClient {
    fn unary_call(
        &mut self,
        method_path: &str,
        request: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, TransportError>;
}

pub struct GrpcTransportAdapter<C> {
    client: C,
    method_path: String,
}

impl<C> GrpcTransportAdapter<C> {
    pub fn new(client: C) -> Self {
        Self {
            client,
            method_path: DEFAULT_GRPC_METHOD_PATH.to_owned(),
        }
    }

    pub fn with_method_path(mut self, method_path: impl Into<String>) -> Self {
        self.method_path = method_path.into();
        self
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }
}

impl<C: GrpcEnvelopeClient> Transport for GrpcTransportAdapter<C> {
    fn round_trip(&mut self, request: RequestEnvelope) -> Result<ResponseEnvelope, TransportError> {
        self.client.unary_call(&self.method_path, &request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrpcCodecError {
    InvalidPayload(String),
    UnsupportedMethodPath(String),
}

impl fmt::Display for GrpcCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayload(message) => write!(f, "invalid gRPC payload: {message}"),
            Self::UnsupportedMethodPath(path) => {
                write!(f, "unsupported gRPC method path: {path}")
            }
        }
    }
}

#[derive(Debug, Default)]
struct BinaryEncoder {
    bytes: Vec<u8>,
}

impl BinaryEncoder {
    fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_len(&mut self, value: usize, field: &str) -> Result<(), GrpcCodecError> {
        let value = u64::try_from(value).map_err(|_| {
            GrpcCodecError::InvalidPayload(format!(
                "field {field} length {value} does not fit in u64"
            ))
        })?;
        self.write_u64(value);
        Ok(())
    }

    fn write_string(&mut self, value: &str, field: &str) -> Result<(), GrpcCodecError> {
        self.write_len(value.len(), field)?;
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn write_option_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.write_u8(1);
                self.write_u64(value);
            }
            None => self.write_u8(0),
        }
    }

    fn write_option_string(
        &mut self,
        value: Option<&str>,
        field: &str,
    ) -> Result<(), GrpcCodecError> {
        match value {
            Some(value) => {
                self.write_u8(1);
                self.write_string(value, field)?;
            }
            None => self.write_u8(0),
        }
        Ok(())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug)]
struct BinaryDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> BinaryDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.cursor)
    }

    fn ensure_remaining(&self, size: usize, field: &str) -> Result<(), GrpcCodecError> {
        if self.remaining() < size {
            return Err(GrpcCodecError::InvalidPayload(format!(
                "field {field} expected {size} bytes but {} remain",
                self.remaining()
            )));
        }
        Ok(())
    }

    fn read_u8(&mut self, field: &str) -> Result<u8, GrpcCodecError> {
        self.ensure_remaining(1, field)?;
        let value = self.bytes[self.cursor];
        self.cursor += 1;
        Ok(value)
    }

    fn read_u64(&mut self, field: &str) -> Result<u64, GrpcCodecError> {
        self.ensure_remaining(8, field)?;
        let mut raw = [0_u8; 8];
        raw.copy_from_slice(&self.bytes[self.cursor..self.cursor + 8]);
        self.cursor += 8;
        Ok(u64::from_le_bytes(raw))
    }

    fn read_len(&mut self, field: &str) -> Result<usize, GrpcCodecError> {
        let value = self.read_u64(field)?;
        usize::try_from(value).map_err(|_| {
            GrpcCodecError::InvalidPayload(format!(
                "field {field} length {value} does not fit in usize"
            ))
        })
    }

    fn read_string(&mut self, field: &str) -> Result<String, GrpcCodecError> {
        let len = self.read_len(field)?;
        self.ensure_remaining(len, field)?;
        let bytes = &self.bytes[self.cursor..self.cursor + len];
        self.cursor += len;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            GrpcCodecError::InvalidPayload(format!("field {field} is not utf8: {error}"))
        })
    }

    fn read_option_u64(&mut self, field: &str) -> Result<Option<u64>, GrpcCodecError> {
        match self.read_u8(field)? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64(field)?)),
            flag => Err(GrpcCodecError::InvalidPayload(format!(
                "field {field} has invalid option flag {flag}"
            ))),
        }
    }

    fn read_option_string(&mut self, field: &str) -> Result<Option<String>, GrpcCodecError> {
        match self.read_u8(field)? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string(field)?)),
            flag => Err(GrpcCodecError::InvalidPayload(format!(
                "field {field} has invalid option flag {flag}"
            ))),
        }
    }

    fn finish(self) -> Result<(), GrpcCodecError> {
        if self.cursor == self.bytes.len() {
            return Ok(());
        }

        Err(GrpcCodecError::InvalidPayload(format!(
            "{} trailing bytes remain",
            self.bytes.len() - self.cursor
        )))
    }
}

struct GrpcWireCodec;

impl GrpcWireCodec {
    fn encode_node_address(
        encoder: &mut BinaryEncoder,
        address: &NodeAddress,
    ) -> Result<(), GrpcCodecError> {
        encoder.write_u64(address.node_id);
        encoder.write_string(&address.endpoint, "node_address.endpoint")
    }

    fn decode_node_address(decoder: &mut BinaryDecoder<'_>) -> Result<NodeAddress, GrpcCodecError> {
        let node_id = decoder.read_u64("node_address.node_id")?;
        let endpoint = decoder.read_string("node_address.endpoint")?;
        Ok(NodeAddress { node_id, endpoint })
    }

    fn encode_read_consistency(encoder: &mut BinaryEncoder, consistency: ReadConsistency) {
        let tag = match consistency {
            ReadConsistency::Eventual => 0_u8,
            ReadConsistency::Linearizable => 1_u8,
        };
        encoder.write_u8(tag);
    }

    fn decode_read_consistency(
        decoder: &mut BinaryDecoder<'_>,
    ) -> Result<ReadConsistency, GrpcCodecError> {
        match decoder.read_u8("read_consistency")? {
            0 => Ok(ReadConsistency::Eventual),
            1 => Ok(ReadConsistency::Linearizable),
            tag => Err(GrpcCodecError::InvalidPayload(format!(
                "invalid read consistency tag {tag}"
            ))),
        }
    }

    fn encode_timeout_operation(encoder: &mut BinaryEncoder, operation: TimeoutOperation) {
        let tag = match operation {
            TimeoutOperation::Proposal => 0_u8,
            TimeoutOperation::LinearizableRead => 1_u8,
        };
        encoder.write_u8(tag);
    }

    fn decode_timeout_operation(
        decoder: &mut BinaryDecoder<'_>,
    ) -> Result<TimeoutOperation, GrpcCodecError> {
        match decoder.read_u8("timeout_operation")? {
            0 => Ok(TimeoutOperation::Proposal),
            1 => Ok(TimeoutOperation::LinearizableRead),
            tag => Err(GrpcCodecError::InvalidPayload(format!(
                "invalid timeout operation tag {tag}"
            ))),
        }
    }

    fn encode_command(
        encoder: &mut BinaryEncoder,
        command: &Command,
    ) -> Result<(), GrpcCodecError> {
        match command {
            Command::Put { key, value } => {
                encoder.write_u8(0);
                encoder.write_string(key, "command.put.key")?;
                encoder.write_string(value, "command.put.value")?;
            }
            Command::Delete { key } => {
                encoder.write_u8(1);
                encoder.write_string(key, "command.delete.key")?;
            }
        }
        Ok(())
    }

    fn decode_command(decoder: &mut BinaryDecoder<'_>) -> Result<Command, GrpcCodecError> {
        match decoder.read_u8("command.tag")? {
            0 => Ok(Command::Put {
                key: decoder.read_string("command.put.key")?,
                value: decoder.read_string("command.put.value")?,
            }),
            1 => Ok(Command::Delete {
                key: decoder.read_string("command.delete.key")?,
            }),
            tag => Err(GrpcCodecError::InvalidPayload(format!(
                "invalid command tag {tag}"
            ))),
        }
    }

    fn encode_request(request: &RequestEnvelope) -> Result<Vec<u8>, GrpcCodecError> {
        let mut encoder = BinaryEncoder::default();
        encoder.write_u64(request.request_id);
        Self::encode_node_address(&mut encoder, &request.from)?;
        Self::encode_node_address(&mut encoder, &request.to)?;
        encoder.write_option_u64(request.timeout_ticks);

        match &request.request {
            ClientRequest::Write(command) => {
                encoder.write_u8(0);
                Self::encode_command(&mut encoder, command)?;
            }
            ClientRequest::Read { key, consistency } => {
                encoder.write_u8(1);
                encoder.write_string(key, "request.read.key")?;
                Self::encode_read_consistency(&mut encoder, *consistency);
            }
            ClientRequest::Sql {
                query,
                read_consistency,
            } => {
                encoder.write_u8(2);
                encoder.write_string(query, "request.sql.query")?;
                Self::encode_read_consistency(&mut encoder, *read_consistency);
            }
        }

        Ok(encoder.into_bytes())
    }

    fn decode_request(bytes: &[u8]) -> Result<RequestEnvelope, GrpcCodecError> {
        let mut decoder = BinaryDecoder::new(bytes);
        let request_id = decoder.read_u64("request.request_id")?;
        let from = Self::decode_node_address(&mut decoder)?;
        let to = Self::decode_node_address(&mut decoder)?;
        let timeout_ticks = decoder.read_option_u64("request.timeout_ticks")?;
        let request = match decoder.read_u8("request.tag")? {
            0 => ClientRequest::Write(Self::decode_command(&mut decoder)?),
            1 => ClientRequest::Read {
                key: decoder.read_string("request.read.key")?,
                consistency: Self::decode_read_consistency(&mut decoder)?,
            },
            2 => ClientRequest::Sql {
                query: decoder.read_string("request.sql.query")?,
                read_consistency: Self::decode_read_consistency(&mut decoder)?,
            },
            tag => {
                return Err(GrpcCodecError::InvalidPayload(format!(
                    "invalid request tag {tag}"
                )));
            }
        };

        decoder.finish()?;
        Ok(RequestEnvelope {
            request_id,
            from,
            to,
            timeout_ticks,
            request,
        })
    }

    fn encode_response(response: &ResponseEnvelope) -> Result<Vec<u8>, GrpcCodecError> {
        let mut encoder = BinaryEncoder::default();
        encoder.write_u64(response.request_id);
        Self::encode_node_address(&mut encoder, &response.from)?;
        Self::encode_node_address(&mut encoder, &response.to)?;

        match &response.response {
            ClientResponse::WriteCommitted {
                index,
                committed_nodes,
            } => {
                encoder.write_u8(0);
                encoder.write_u64(*index);
                encoder.write_u64(u64::try_from(*committed_nodes).map_err(|_| {
                    GrpcCodecError::InvalidPayload(format!(
                        "committed_nodes {committed_nodes} does not fit in u64"
                    ))
                })?);
            }
            ClientResponse::ReadResult { value, consistency } => {
                encoder.write_u8(1);
                encoder.write_option_string(value.as_deref(), "response.read.value")?;
                Self::encode_read_consistency(&mut encoder, *consistency);
            }
            ClientResponse::Redirect { leader } => {
                encoder.write_u8(2);
                Self::encode_node_address(&mut encoder, leader)?;
            }
            ClientResponse::TimedOut {
                operation,
                index,
                observed_nodes,
            } => {
                encoder.write_u8(3);
                Self::encode_timeout_operation(&mut encoder, *operation);
                encoder.write_option_u64(*index);
                encoder.write_u64(u64::try_from(*observed_nodes).map_err(|_| {
                    GrpcCodecError::InvalidPayload(format!(
                        "observed_nodes {observed_nodes} does not fit in u64"
                    ))
                })?);
            }
            ClientResponse::Overloaded {
                pending_entries,
                limit,
            } => {
                encoder.write_u8(4);
                encoder.write_u64(*pending_entries);
                encoder.write_u64(*limit);
            }
            ClientResponse::RouteError { message } => {
                encoder.write_u8(5);
                encoder.write_string(message, "response.route_error.message")?;
            }
            ClientResponse::InvalidQuery { message } => {
                encoder.write_u8(6);
                encoder.write_string(message, "response.invalid_query.message")?;
            }
            ClientResponse::SqlRows {
                columns,
                rows,
                consistency,
            } => {
                encoder.write_u8(7);
                encoder.write_len(columns.len(), "response.sql_rows.columns")?;
                for column in columns {
                    encoder.write_string(column, "response.sql_rows.column")?;
                }

                encoder.write_len(rows.len(), "response.sql_rows.rows")?;
                for row in rows {
                    encoder.write_len(row.len(), "response.sql_rows.row.columns")?;
                    for value in row {
                        encoder.write_string(value, "response.sql_rows.row.value")?;
                    }
                }

                Self::encode_read_consistency(&mut encoder, *consistency);
            }
            ClientResponse::SqlWriteResult {
                affected_rows,
                last_index,
                committed_nodes,
            } => {
                encoder.write_u8(8);
                encoder.write_u64(u64::try_from(*affected_rows).map_err(|_| {
                    GrpcCodecError::InvalidPayload(format!(
                        "affected_rows {affected_rows} does not fit in u64"
                    ))
                })?);
                encoder.write_option_u64(*last_index);
                encoder.write_u64(u64::try_from(*committed_nodes).map_err(|_| {
                    GrpcCodecError::InvalidPayload(format!(
                        "committed_nodes {committed_nodes} does not fit in u64"
                    ))
                })?);
            }
            ClientResponse::InternalError { message } => {
                encoder.write_u8(9);
                encoder.write_string(message, "response.internal_error.message")?;
            }
        }

        Ok(encoder.into_bytes())
    }

    fn decode_response(bytes: &[u8]) -> Result<ResponseEnvelope, GrpcCodecError> {
        let mut decoder = BinaryDecoder::new(bytes);
        let request_id = decoder.read_u64("response.request_id")?;
        let from = Self::decode_node_address(&mut decoder)?;
        let to = Self::decode_node_address(&mut decoder)?;

        let response = match decoder.read_u8("response.tag")? {
            0 => ClientResponse::WriteCommitted {
                index: decoder.read_u64("response.write_committed.index")?,
                committed_nodes: usize::try_from(
                    decoder.read_u64("response.write_committed.committed_nodes")?,
                )
                .map_err(|_| {
                    GrpcCodecError::InvalidPayload(
                        "response.write_committed.committed_nodes does not fit in usize".to_owned(),
                    )
                })?,
            },
            1 => ClientResponse::ReadResult {
                value: decoder.read_option_string("response.read.value")?,
                consistency: Self::decode_read_consistency(&mut decoder)?,
            },
            2 => ClientResponse::Redirect {
                leader: Self::decode_node_address(&mut decoder)?,
            },
            3 => ClientResponse::TimedOut {
                operation: Self::decode_timeout_operation(&mut decoder)?,
                index: decoder.read_option_u64("response.timed_out.index")?,
                observed_nodes: usize::try_from(
                    decoder.read_u64("response.timed_out.observed_nodes")?,
                )
                .map_err(|_| {
                    GrpcCodecError::InvalidPayload(
                        "response.timed_out.observed_nodes does not fit in usize".to_owned(),
                    )
                })?,
            },
            4 => ClientResponse::Overloaded {
                pending_entries: decoder.read_u64("response.overloaded.pending_entries")?,
                limit: decoder.read_u64("response.overloaded.limit")?,
            },
            5 => ClientResponse::RouteError {
                message: decoder.read_string("response.route_error.message")?,
            },
            6 => ClientResponse::InvalidQuery {
                message: decoder.read_string("response.invalid_query.message")?,
            },
            7 => {
                let column_count = decoder.read_len("response.sql_rows.columns")?;
                let mut columns = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    columns.push(decoder.read_string("response.sql_rows.column")?);
                }

                let row_count = decoder.read_len("response.sql_rows.rows")?;
                let mut rows = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    let value_count = decoder.read_len("response.sql_rows.row.columns")?;
                    let mut row = Vec::with_capacity(value_count);
                    for _ in 0..value_count {
                        row.push(decoder.read_string("response.sql_rows.row.value")?);
                    }
                    rows.push(row);
                }

                ClientResponse::SqlRows {
                    columns,
                    rows,
                    consistency: Self::decode_read_consistency(&mut decoder)?,
                }
            }
            8 => ClientResponse::SqlWriteResult {
                affected_rows: usize::try_from(
                    decoder.read_u64("response.sql_write.affected_rows")?,
                )
                .map_err(|_| {
                    GrpcCodecError::InvalidPayload(
                        "response.sql_write.affected_rows does not fit in usize".to_owned(),
                    )
                })?,
                last_index: decoder.read_option_u64("response.sql_write.last_index")?,
                committed_nodes: usize::try_from(
                    decoder.read_u64("response.sql_write.committed_nodes")?,
                )
                .map_err(|_| {
                    GrpcCodecError::InvalidPayload(
                        "response.sql_write.committed_nodes does not fit in usize".to_owned(),
                    )
                })?,
            },
            9 => ClientResponse::InternalError {
                message: decoder.read_string("response.internal_error.message")?,
            },
            tag => {
                return Err(GrpcCodecError::InvalidPayload(format!(
                    "invalid response tag {tag}"
                )));
            }
        };

        decoder.finish()?;
        Ok(ResponseEnvelope {
            request_id,
            from,
            to,
            response,
        })
    }
}

pub trait GrpcWireClient {
    fn unary_call_bytes(
        &mut self,
        method_path: &str,
        request_payload: &[u8],
    ) -> Result<Vec<u8>, TransportError>;
}

pub struct GrpcWireTransportAdapter<C> {
    client: C,
    method_path: String,
}

impl<C> GrpcWireTransportAdapter<C> {
    pub fn new(client: C) -> Self {
        Self {
            client,
            method_path: DEFAULT_GRPC_METHOD_PATH.to_owned(),
        }
    }

    pub fn with_method_path(mut self, method_path: impl Into<String>) -> Self {
        self.method_path = method_path.into();
        self
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }
}

impl<C: GrpcWireClient> Transport for GrpcWireTransportAdapter<C> {
    fn round_trip(&mut self, request: RequestEnvelope) -> Result<ResponseEnvelope, TransportError> {
        let payload = GrpcWireCodec::encode_request(&request)
            .map_err(|error| TransportError::Remote(error.to_string()))?;
        let response_payload = self.client.unary_call_bytes(&self.method_path, &payload)?;
        GrpcWireCodec::decode_response(&response_payload)
            .map_err(|error| TransportError::Remote(error.to_string()))
    }
}

#[derive(Debug)]
pub struct GrpcWireServer {
    service: ApiService,
}

impl GrpcWireServer {
    pub fn new(service: ApiService) -> Self {
        Self { service }
    }

    pub fn service(&self) -> &ApiService {
        &self.service
    }

    pub fn service_mut(&mut self) -> &mut ApiService {
        &mut self.service
    }

    pub fn handle_unary_bytes(
        &mut self,
        method_path: &str,
        request_payload: &[u8],
    ) -> Result<Vec<u8>, TransportError> {
        if method_path != DEFAULT_GRPC_METHOD_PATH {
            return Err(TransportError::Remote(
                GrpcCodecError::UnsupportedMethodPath(method_path.to_owned()).to_string(),
            ));
        }

        let request = GrpcWireCodec::decode_request(request_payload)
            .map_err(|error| TransportError::Remote(error.to_string()))?;
        let response = self.service.handle(request);
        GrpcWireCodec::encode_response(&response)
            .map_err(|error| TransportError::Remote(error.to_string()))
    }

    pub fn handle_unary_chunked_bytes<I, B>(
        &mut self,
        method_path: &str,
        request_chunks: I,
    ) -> Result<Vec<u8>, TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut request_payload = Vec::new();
        for chunk in request_chunks {
            request_payload.extend_from_slice(chunk.as_ref());
        }

        self.handle_unary_bytes(method_path, &request_payload)
    }
}

#[cfg(test)]
mod tests {
    use db_core::Command;
    use db_raft::RaftCluster;

    use super::{
        ApiService, ClientRequest, ClientResponse, GrpcCodecError, GrpcEnvelopeClient,
        GrpcTransportAdapter, GrpcWireClient, GrpcWireCodec, GrpcWireServer,
        GrpcWireTransportAdapter, NodeAddress, ReadConsistency, ReadLease, RequestEnvelope,
        ResponseEnvelope, TimeoutOperation, Transport, TransportConfig, TransportError,
        DEFAULT_GRPC_METHOD_PATH,
    };

    fn client_address() -> NodeAddress {
        NodeAddress::new(9_001, "client://tester")
    }

    fn request(
        request_id: u64,
        from: NodeAddress,
        to: NodeAddress,
        timeout_ticks: Option<u64>,
        request: ClientRequest,
    ) -> RequestEnvelope {
        RequestEnvelope {
            request_id,
            from,
            to,
            timeout_ticks,
            request,
        }
    }

    fn chunk_bytes(bytes: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
        let chunk_size = chunk_size.max(1);
        let mut chunks = Vec::new();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let end = (offset + chunk_size).min(bytes.len());
            chunks.push(bytes[offset..end].to_vec());
            offset = end;
        }
        chunks
    }

    #[derive(Debug, Default)]
    struct FakeGrpcClient {
        seen_paths: Vec<String>,
        seen_requests: Vec<RequestEnvelope>,
        scripted: Vec<Result<ResponseEnvelope, TransportError>>,
    }

    impl FakeGrpcClient {
        fn with_response(response: ResponseEnvelope) -> Self {
            Self {
                seen_paths: Vec::new(),
                seen_requests: Vec::new(),
                scripted: vec![Ok(response)],
            }
        }
    }

    impl GrpcEnvelopeClient for FakeGrpcClient {
        fn unary_call(
            &mut self,
            method_path: &str,
            request: &RequestEnvelope,
        ) -> Result<ResponseEnvelope, TransportError> {
            self.seen_paths.push(method_path.to_owned());
            self.seen_requests.push(request.clone());
            self.scripted.pop().unwrap_or_else(|| {
                Err(TransportError::Remote(
                    "no scripted response available".to_owned(),
                ))
            })
        }
    }

    #[derive(Debug)]
    struct LoopbackGrpcWireClient {
        server: GrpcWireServer,
        seen_paths: Vec<String>,
        seen_payload_sizes: Vec<usize>,
    }

    impl LoopbackGrpcWireClient {
        fn new(server: GrpcWireServer) -> Self {
            Self {
                server,
                seen_paths: Vec::new(),
                seen_payload_sizes: Vec::new(),
            }
        }
    }

    impl GrpcWireClient for LoopbackGrpcWireClient {
        fn unary_call_bytes(
            &mut self,
            method_path: &str,
            request_payload: &[u8],
        ) -> Result<Vec<u8>, TransportError> {
            self.seen_paths.push(method_path.to_owned());
            self.seen_payload_sizes.push(request_payload.len());
            self.server.handle_unary_bytes(method_path, request_payload)
        }
    }

    #[test]
    fn write_to_follower_returns_leader_redirect() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);
        let follower = api.address_for(2).expect("follower address").clone();
        let leader = api.leader_address();

        let response = api.handle(request(
            1,
            client_address(),
            follower,
            None,
            ClientRequest::Write(Command::Put {
                key: "planet".to_owned(),
                value: "earth".to_owned(),
            }),
        ));

        assert_eq!(response.response, ClientResponse::Redirect { leader });
    }

    #[test]
    fn unknown_destination_address_is_rejected() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);

        let response = api.handle(request(
            7,
            client_address(),
            NodeAddress::new(1, "node-1.bad.internal"),
            None,
            ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Eventual,
            },
        ));

        assert!(matches!(
            response.response,
            ClientResponse::RouteError { .. }
        ));
    }

    #[test]
    fn proposal_times_out_without_quorum() {
        let cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");
        let mut api = ApiService::with_config(
            cluster,
            TransportConfig {
                proposal_timeout_ticks: 2,
                read_timeout_ticks: 2,
                read_lease_ticks: 3,
                max_pending_entries: 64,
            },
        );

        api.cluster_mut()
            .disconnect(3)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(4)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(5)
            .expect("disconnect should work");

        let leader = api.leader_address();

        let response = api.handle(request(
            11,
            client_address(),
            leader,
            Some(2),
            ClientRequest::Write(Command::Put {
                key: "x".to_owned(),
                value: "1".to_owned(),
            }),
        ));

        assert_eq!(
            response.response,
            ClientResponse::TimedOut {
                operation: TimeoutOperation::Proposal,
                index: Some(1),
                observed_nodes: 0,
            }
        );
    }

    #[test]
    fn backpressure_rejects_when_pending_backlog_reaches_limit() {
        let cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");
        let mut api = ApiService::with_config(
            cluster,
            TransportConfig {
                proposal_timeout_ticks: 1,
                read_timeout_ticks: 2,
                read_lease_ticks: 3,
                max_pending_entries: 1,
            },
        );

        api.cluster_mut()
            .disconnect(3)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(4)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(5)
            .expect("disconnect should work");

        let leader = api.leader_address();
        let client = client_address();

        let first = api.handle(request(
            21,
            client.clone(),
            leader.clone(),
            Some(1),
            ClientRequest::Write(Command::Put {
                key: "x".to_owned(),
                value: "1".to_owned(),
            }),
        ));
        assert!(matches!(
            first.response,
            ClientResponse::TimedOut {
                operation: TimeoutOperation::Proposal,
                index: Some(1),
                observed_nodes: 0
            }
        ));

        let second = api.handle(request(
            22,
            client,
            leader,
            Some(1),
            ClientRequest::Write(Command::Put {
                key: "y".to_owned(),
                value: "2".to_owned(),
            }),
        ));
        assert_eq!(
            second.response,
            ClientResponse::Overloaded {
                pending_entries: 1,
                limit: 1,
            }
        );
    }

    #[test]
    fn linearizable_read_times_out_without_quorum_but_eventual_read_succeeds() {
        let cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");
        let mut api = ApiService::with_config(
            cluster,
            TransportConfig {
                proposal_timeout_ticks: 2,
                read_timeout_ticks: 1,
                read_lease_ticks: 3,
                max_pending_entries: 64,
            },
        );

        let leader = api.leader_address();
        let client = client_address();

        let write = api.handle(request(
            31,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Write(Command::Put {
                key: "planet".to_owned(),
                value: "saturn".to_owned(),
            }),
        ));
        assert!(matches!(
            write.response,
            ClientResponse::WriteCommitted { index: 1, .. }
        ));

        api.cluster_mut()
            .disconnect(3)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(4)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(5)
            .expect("disconnect should work");

        let linearizable = api.handle(request(
            32,
            client.clone(),
            leader.clone(),
            Some(1),
            ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            linearizable.response,
            ClientResponse::TimedOut {
                operation: TimeoutOperation::LinearizableRead,
                index: None,
                observed_nodes: 2,
            }
        );

        let eventual = api.handle(request(
            33,
            client,
            leader,
            None,
            ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Eventual,
            },
        ));
        assert_eq!(
            eventual.response,
            ClientResponse::ReadResult {
                value: Some("saturn".to_owned()),
                consistency: ReadConsistency::Eventual,
            }
        );
    }

    #[test]
    fn linearizable_read_lease_allows_temporary_quorum_loss_until_expiry() {
        let cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");
        let mut api = ApiService::with_config(
            cluster,
            TransportConfig {
                proposal_timeout_ticks: 2,
                read_timeout_ticks: 0,
                read_lease_ticks: 3,
                max_pending_entries: 64,
            },
        );

        let leader = api.leader_address();
        let client = client_address();

        let write = api.handle(request(
            40,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Write(Command::Put {
                key: "lease-key".to_owned(),
                value: "lease-value".to_owned(),
            }),
        ));
        assert!(matches!(
            write.response,
            ClientResponse::WriteCommitted { index: 1, .. }
        ));

        let acquire_lease = api.handle(request(
            41,
            client.clone(),
            leader.clone(),
            Some(0),
            ClientRequest::Read {
                key: "lease-key".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            acquire_lease.response,
            ClientResponse::ReadResult {
                value: Some("lease-value".to_owned()),
                consistency: ReadConsistency::Linearizable,
            }
        );

        api.cluster_mut()
            .disconnect(3)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(4)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(5)
            .expect("disconnect should work");

        let still_within_lease = api.handle(request(
            42,
            client.clone(),
            leader.clone(),
            Some(0),
            ClientRequest::Read {
                key: "lease-key".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            still_within_lease.response,
            ClientResponse::ReadResult {
                value: Some("lease-value".to_owned()),
                consistency: ReadConsistency::Linearizable,
            }
        );

        api.cluster_mut()
            .tick_many(4)
            .expect("ticks should advance lease clock");

        let after_expiry = api.handle(request(
            43,
            client,
            leader,
            Some(0),
            ClientRequest::Read {
                key: "lease-key".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            after_expiry.response,
            ClientResponse::TimedOut {
                operation: TimeoutOperation::LinearizableRead,
                index: None,
                observed_nodes: 2,
            }
        );
    }

    #[test]
    fn linearizable_read_redirects_after_leadership_change_even_with_cached_lease() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::with_config(
            cluster,
            TransportConfig {
                proposal_timeout_ticks: 2,
                read_timeout_ticks: 0,
                read_lease_ticks: 5,
                max_pending_entries: 64,
            },
        );

        let old_leader = api.leader_address();
        let client = client_address();

        let write = api.handle(request(
            43,
            client.clone(),
            old_leader.clone(),
            None,
            ClientRequest::Write(Command::Put {
                key: "stale".to_owned(),
                value: "guard".to_owned(),
            }),
        ));
        assert!(matches!(
            write.response,
            ClientResponse::WriteCommitted { index: 1, .. }
        ));

        let lease_read = api.handle(request(
            44,
            client.clone(),
            old_leader.clone(),
            Some(0),
            ClientRequest::Read {
                key: "stale".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert!(matches!(
            lease_read.response,
            ClientResponse::ReadResult {
                consistency: ReadConsistency::Linearizable,
                ..
            }
        ));

        let election = api
            .cluster_mut()
            .start_election(2)
            .expect("election should execute");
        assert!(election.elected, "node 2 should become leader");

        let new_leader = api.leader_address();
        assert_ne!(old_leader.node_id, new_leader.node_id);

        let stale_target_read = api.handle(request(
            45,
            client,
            old_leader,
            Some(0),
            ClientRequest::Read {
                key: "stale".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            stale_target_read.response,
            ClientResponse::Redirect { leader: new_leader }
        );
    }

    #[test]
    fn read_lease_window_is_clamped_to_election_timeout_safety_margin() {
        let cluster = RaftCluster::new(vec![1, 2, 3, 4, 5]).expect("cluster should build");
        let mut api = ApiService::with_config(
            cluster,
            TransportConfig {
                proposal_timeout_ticks: 2,
                read_timeout_ticks: 0,
                read_lease_ticks: 50,
                max_pending_entries: 64,
            },
        );

        let leader = api.leader_address();
        let client = client_address();

        let write = api.handle(request(
            46,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Write(Command::Put {
                key: "lease-cap".to_owned(),
                value: "value".to_owned(),
            }),
        ));
        assert!(matches!(
            write.response,
            ClientResponse::WriteCommitted { index: 1, .. }
        ));

        let lease_read = api.handle(request(
            47,
            client.clone(),
            leader.clone(),
            Some(0),
            ClientRequest::Read {
                key: "lease-cap".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert!(matches!(
            lease_read.response,
            ClientResponse::ReadResult {
                consistency: ReadConsistency::Linearizable,
                ..
            }
        ));

        api.cluster_mut()
            .disconnect(3)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(4)
            .expect("disconnect should work");
        api.cluster_mut()
            .disconnect(5)
            .expect("disconnect should work");

        api.cluster_mut()
            .tick_many(6)
            .expect("ticks should advance beyond safety-capped lease");

        let after_cap = api.handle(request(
            48,
            client,
            leader,
            Some(0),
            ClientRequest::Read {
                key: "lease-cap".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            after_cap.response,
            ClientResponse::TimedOut {
                operation: TimeoutOperation::LinearizableRead,
                index: None,
                observed_nodes: 2,
            }
        );
    }

    #[test]
    fn lease_is_invalidated_when_clock_moves_backwards() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);

        api.read_lease = Some(ReadLease {
            leader_id: api.cluster().leader_id(),
            term: api.cluster().current_term(),
            issued_at_tick: 7,
            expires_at_tick: 9,
            acknowledged_nodes: 3,
        });

        assert_eq!(api.active_read_lease(), None);
    }

    #[test]
    fn eventual_read_on_lagging_follower_returns_older_mvcc_version() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);
        let leader = api.leader_address();
        let follower = api
            .address_for(3)
            .expect("follower address should exist")
            .clone();
        let client = client_address();

        let _ = api.handle(request(
            44,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "INSERT INTO kv (key, value) VALUES ('planet', 'earth')".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));

        api.cluster_mut()
            .disconnect(3)
            .expect("disconnect should work");

        let _ = api.handle(request(
            45,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "UPDATE kv SET value = 'saturn' WHERE key = 'planet'".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));

        let eventual_on_lagging_follower = api.handle(request(
            46,
            client.clone(),
            follower,
            None,
            ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Eventual,
            },
        ));
        assert_eq!(
            eventual_on_lagging_follower.response,
            ClientResponse::ReadResult {
                value: Some("earth".to_owned()),
                consistency: ReadConsistency::Eventual,
            }
        );

        let linearizable_on_leader = api.handle(request(
            47,
            client,
            leader,
            None,
            ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            linearizable_on_leader.response,
            ClientResponse::ReadResult {
                value: Some("saturn".to_owned()),
                consistency: ReadConsistency::Linearizable,
            }
        );
    }

    #[test]
    fn mvcc_rebuilds_if_cluster_state_advances_outside_api_service() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);
        let leader = api.leader_address();

        api.cluster_mut()
            .propose(Command::Put {
                key: "out-of-band".to_owned(),
                value: "write".to_owned(),
            })
            .expect("direct cluster proposal should commit");

        let response = api.handle(request(
            48,
            client_address(),
            leader,
            None,
            ClientRequest::Read {
                key: "out-of-band".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        ));

        assert_eq!(
            response.response,
            ClientResponse::ReadResult {
                value: Some("write".to_owned()),
                consistency: ReadConsistency::Linearizable,
            }
        );
    }

    #[test]
    fn sql_insert_select_delete_executes_against_replicated_state() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);
        let leader = api.leader_address();
        let client = client_address();

        let insert = api.handle(request(
            50,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "INSERT INTO kv (key, value) VALUES ('planet', 'saturn')".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));
        assert_eq!(
            insert.response,
            ClientResponse::SqlWriteResult {
                affected_rows: 1,
                last_index: Some(1),
                committed_nodes: 3,
            }
        );

        let update = api.handle(request(
            51,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "UPDATE kv SET value = 'gas giant' WHERE key = 'planet'".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));
        assert_eq!(
            update.response,
            ClientResponse::SqlWriteResult {
                affected_rows: 1,
                last_index: Some(2),
                committed_nodes: 3,
            }
        );

        let select = api.handle(request(
            52,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "SELECT key, value FROM kv WHERE value != 'cold'".to_owned(),
                read_consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            select.response,
            ClientResponse::SqlRows {
                columns: vec!["key".to_owned(), "value".to_owned()],
                rows: vec![vec!["planet".to_owned(), "gas giant".to_owned()]],
                consistency: ReadConsistency::Linearizable,
            }
        );

        let delete = api.handle(request(
            53,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "DELETE FROM kv WHERE key = 'planet'".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));
        assert_eq!(
            delete.response,
            ClientResponse::SqlWriteResult {
                affected_rows: 1,
                last_index: Some(3),
                committed_nodes: 3,
            }
        );

        let select_after_delete = api.handle(request(
            54,
            client,
            leader,
            None,
            ClientRequest::Sql {
                query: "SELECT value FROM kv WHERE key = 'planet'".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));
        assert_eq!(
            select_after_delete.response,
            ClientResponse::SqlRows {
                columns: vec!["value".to_owned()],
                rows: Vec::new(),
                consistency: ReadConsistency::Eventual,
            }
        );
    }

    #[test]
    fn invalid_sql_returns_invalid_query_response() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);
        let leader = api.leader_address();

        let response = api.handle(request(
            55,
            client_address(),
            leader,
            None,
            ClientRequest::Sql {
                query: "UPDATE kv SET key = '2' WHERE key = 'planet'".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));

        assert!(matches!(
            response.response,
            ClientResponse::InvalidQuery { .. }
        ));
    }

    #[test]
    fn sql_predicate_mutation_can_affect_multiple_rows() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);
        let leader = api.leader_address();
        let client = client_address();

        let _ = api.handle(request(
            60,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "INSERT INTO kv (key, value) VALUES ('k1', 'v')".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));
        let _ = api.handle(request(
            61,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "INSERT INTO kv (key, value) VALUES ('k2', 'v')".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));

        let update = api.handle(request(
            62,
            client.clone(),
            leader.clone(),
            None,
            ClientRequest::Sql {
                query: "UPDATE kv SET value = 'changed' WHERE value = 'v'".to_owned(),
                read_consistency: ReadConsistency::Eventual,
            },
        ));
        assert_eq!(
            update.response,
            ClientResponse::SqlWriteResult {
                affected_rows: 2,
                last_index: Some(4),
                committed_nodes: 3,
            }
        );

        let select = api.handle(request(
            63,
            client,
            leader,
            None,
            ClientRequest::Sql {
                query: "SELECT key, value FROM kv WHERE value = 'changed'".to_owned(),
                read_consistency: ReadConsistency::Linearizable,
            },
        ));
        assert_eq!(
            select.response,
            ClientResponse::SqlRows {
                columns: vec!["key".to_owned(), "value".to_owned()],
                rows: vec![
                    vec!["k1".to_owned(), "changed".to_owned()],
                    vec!["k2".to_owned(), "changed".to_owned()],
                ],
                consistency: ReadConsistency::Linearizable,
            }
        );
    }

    #[test]
    fn in_process_transport_round_trip_delegates_to_api_service() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let mut api = ApiService::new(cluster);
        let leader = api.leader_address();

        let mut transport = api.in_process_transport();
        let response = transport
            .round_trip(request(
                51,
                client_address(),
                leader,
                None,
                ClientRequest::Write(Command::Put {
                    key: "transport".to_owned(),
                    value: "in-process".to_owned(),
                }),
            ))
            .expect("round trip should succeed");

        assert!(matches!(
            response.response,
            ClientResponse::WriteCommitted { index: 1, .. }
        ));
    }

    #[test]
    fn grpc_transport_adapter_forwards_method_and_envelope() {
        let client_request = request(
            61,
            client_address(),
            NodeAddress::new(1, "node-1.anvildb.internal"),
            None,
            ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Eventual,
            },
        );
        let expected_response = ResponseEnvelope {
            request_id: 61,
            from: NodeAddress::new(1, "node-1.anvildb.internal"),
            to: client_address(),
            response: ClientResponse::ReadResult {
                value: Some("saturn".to_owned()),
                consistency: ReadConsistency::Eventual,
            },
        };

        let fake_client = FakeGrpcClient::with_response(expected_response.clone());
        let mut transport = GrpcTransportAdapter::new(fake_client);

        let response = transport
            .round_trip(client_request.clone())
            .expect("adapter should return client response");
        assert_eq!(response, expected_response);
        assert_eq!(
            transport.client().seen_paths,
            vec![DEFAULT_GRPC_METHOD_PATH.to_owned()]
        );
        assert_eq!(transport.client().seen_requests, vec![client_request]);
    }

    #[test]
    fn grpc_wire_codec_round_trips_envelopes() {
        let request_envelope = request(
            70,
            client_address(),
            NodeAddress::new(1, "node-1.anvildb.internal"),
            Some(3),
            ClientRequest::Sql {
                query: "SELECT key, value FROM kv WHERE value != 'cold'".to_owned(),
                read_consistency: ReadConsistency::Linearizable,
            },
        );

        let encoded_request =
            GrpcWireCodec::encode_request(&request_envelope).expect("request should encode");
        let decoded_request =
            GrpcWireCodec::decode_request(&encoded_request).expect("request should decode");
        assert_eq!(decoded_request, request_envelope);

        let response_envelope = ResponseEnvelope {
            request_id: 70,
            from: NodeAddress::new(1, "node-1.anvildb.internal"),
            to: client_address(),
            response: ClientResponse::SqlRows {
                columns: vec!["key".to_owned(), "value".to_owned()],
                rows: vec![vec!["planet".to_owned(), "saturn".to_owned()]],
                consistency: ReadConsistency::Linearizable,
            },
        };

        let encoded_response =
            GrpcWireCodec::encode_response(&response_envelope).expect("response should encode");
        let decoded_response =
            GrpcWireCodec::decode_response(&encoded_response).expect("response should decode");
        assert_eq!(decoded_response, response_envelope);
    }

    #[test]
    fn grpc_wire_transport_adapter_round_trips_through_wire_server() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let service = ApiService::new(cluster);
        let leader = service.leader_address();
        let mut transport = GrpcWireTransportAdapter::new(LoopbackGrpcWireClient::new(
            GrpcWireServer::new(service),
        ));

        let write = transport
            .round_trip(request(
                71,
                client_address(),
                leader.clone(),
                None,
                ClientRequest::Write(Command::Put {
                    key: "wire".to_owned(),
                    value: "grpc".to_owned(),
                }),
            ))
            .expect("wire round trip should succeed");
        assert!(matches!(
            write.response,
            ClientResponse::WriteCommitted { index: 1, .. }
        ));

        let read = transport
            .round_trip(request(
                72,
                client_address(),
                leader,
                None,
                ClientRequest::Read {
                    key: "wire".to_owned(),
                    consistency: ReadConsistency::Linearizable,
                },
            ))
            .expect("wire round trip read should succeed");
        assert_eq!(
            read.response,
            ClientResponse::ReadResult {
                value: Some("grpc".to_owned()),
                consistency: ReadConsistency::Linearizable,
            }
        );

        assert_eq!(
            transport.client().seen_paths,
            vec![
                DEFAULT_GRPC_METHOD_PATH.to_owned(),
                DEFAULT_GRPC_METHOD_PATH.to_owned()
            ]
        );
        assert_eq!(transport.client().seen_payload_sizes.len(), 2);
    }

    #[test]
    fn grpc_wire_server_rejects_unknown_method_path() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let service = ApiService::new(cluster);
        let mut transport = GrpcWireTransportAdapter::new(LoopbackGrpcWireClient::new(
            GrpcWireServer::new(service),
        ))
        .with_method_path("/anvildb.v1.Api/Unknown");

        let response = transport.round_trip(request(
            73,
            client_address(),
            NodeAddress::new(1, "node-1.anvildb.internal"),
            None,
            ClientRequest::Read {
                key: "wire".to_owned(),
                consistency: ReadConsistency::Eventual,
            },
        ));

        assert!(matches!(
            response,
            Err(TransportError::Remote(message))
            if message.contains("unsupported gRPC method path")
        ));
    }

    #[test]
    fn grpc_wire_server_handles_fragmented_chunk_streams() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let service = ApiService::new(cluster);
        let leader = service.leader_address();
        let mut server = GrpcWireServer::new(service);

        let write_request = request(
            80,
            client_address(),
            leader.clone(),
            None,
            ClientRequest::Write(Command::Put {
                key: "chunked".to_owned(),
                value: "ok".to_owned(),
            }),
        );
        let write_payload =
            GrpcWireCodec::encode_request(&write_request).expect("request should encode");
        let write_chunks = chunk_bytes(&write_payload, 3);
        let write_response_payload = server
            .handle_unary_chunked_bytes(
                DEFAULT_GRPC_METHOD_PATH,
                write_chunks.iter().map(Vec::as_slice),
            )
            .expect("chunked write should succeed");
        let write_response = GrpcWireCodec::decode_response(&write_response_payload)
            .expect("response should decode");
        assert!(matches!(
            write_response.response,
            ClientResponse::WriteCommitted { index: 1, .. }
        ));

        let read_request = request(
            81,
            client_address(),
            leader,
            None,
            ClientRequest::Read {
                key: "chunked".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
        );
        let read_payload = GrpcWireCodec::encode_request(&read_request).expect("request encodes");
        let read_chunks = chunk_bytes(&read_payload, 2);
        let read_response_payload = server
            .handle_unary_chunked_bytes(
                DEFAULT_GRPC_METHOD_PATH,
                read_chunks.iter().map(Vec::as_slice),
            )
            .expect("chunked read should succeed");
        let read_response =
            GrpcWireCodec::decode_response(&read_response_payload).expect("response should decode");
        assert_eq!(
            read_response.response,
            ClientResponse::ReadResult {
                value: Some("ok".to_owned()),
                consistency: ReadConsistency::Linearizable,
            }
        );
    }

    #[test]
    fn grpc_wire_codec_rejects_truncated_and_trailing_payloads() {
        let req = request(
            82,
            client_address(),
            NodeAddress::new(1, "node-1.anvildb.internal"),
            Some(1),
            ClientRequest::Read {
                key: "planet".to_owned(),
                consistency: ReadConsistency::Eventual,
            },
        );
        let encoded_req = GrpcWireCodec::encode_request(&req).expect("request encodes");

        let truncated_req = &encoded_req[..encoded_req.len() - 1];
        assert!(matches!(
            GrpcWireCodec::decode_request(truncated_req),
            Err(GrpcCodecError::InvalidPayload(_))
        ));

        let mut trailing_req = encoded_req.clone();
        trailing_req.push(0xAA);
        assert!(matches!(
            GrpcWireCodec::decode_request(&trailing_req),
            Err(GrpcCodecError::InvalidPayload(_))
        ));

        let resp = ResponseEnvelope {
            request_id: 82,
            from: NodeAddress::new(1, "node-1.anvildb.internal"),
            to: client_address(),
            response: ClientResponse::ReadResult {
                value: Some("planet".to_owned()),
                consistency: ReadConsistency::Linearizable,
            },
        };
        let encoded_resp = GrpcWireCodec::encode_response(&resp).expect("response encodes");

        let truncated_resp = &encoded_resp[..encoded_resp.len() - 1];
        assert!(matches!(
            GrpcWireCodec::decode_response(truncated_resp),
            Err(GrpcCodecError::InvalidPayload(_))
        ));

        let mut trailing_resp = encoded_resp;
        trailing_resp.extend_from_slice(&[0xBB, 0xCC]);
        assert!(matches!(
            GrpcWireCodec::decode_response(&trailing_resp),
            Err(GrpcCodecError::InvalidPayload(_))
        ));
    }

    #[test]
    fn grpc_wire_transport_matches_in_process_transport_behavior() {
        let mut in_process_api =
            ApiService::new(RaftCluster::new(vec![1, 2, 3]).expect("cluster should build"));
        let wire_api =
            ApiService::new(RaftCluster::new(vec![1, 2, 3]).expect("cluster should build"));

        let leader = in_process_api.leader_address();
        let mut wire_transport = GrpcWireTransportAdapter::new(LoopbackGrpcWireClient::new(
            GrpcWireServer::new(wire_api),
        ));

        let requests = [
            ClientRequest::Write(Command::Put {
                key: "interop".to_owned(),
                value: "one".to_owned(),
            }),
            ClientRequest::Read {
                key: "interop".to_owned(),
                consistency: ReadConsistency::Eventual,
            },
            ClientRequest::Read {
                key: "interop".to_owned(),
                consistency: ReadConsistency::Linearizable,
            },
            ClientRequest::Sql {
                query: "SELECT key, value FROM kv WHERE key = 'interop'".to_owned(),
                read_consistency: ReadConsistency::Linearizable,
            },
        ];

        for (idx, req) in requests.iter().enumerate() {
            let request_id = 90 + (idx as u64);
            let in_process_response = in_process_api.handle(request(
                request_id,
                client_address(),
                leader.clone(),
                None,
                req.clone(),
            ));
            let wire_response = wire_transport
                .round_trip(request(
                    request_id,
                    client_address(),
                    leader.clone(),
                    None,
                    req.clone(),
                ))
                .expect("wire transport should succeed");

            assert_eq!(
                wire_response.response, in_process_response.response,
                "response mismatch at request index {idx}"
            );
        }
    }

    #[test]
    fn grpc_wire_server_rejects_corrupted_request_payload() {
        let cluster = RaftCluster::new(vec![1, 2, 3]).expect("cluster should build");
        let service = ApiService::new(cluster);
        let mut server = GrpcWireServer::new(service);

        let response = server.handle_unary_bytes(DEFAULT_GRPC_METHOD_PATH, &[0xFF, 0x00, 0x01]);

        assert!(matches!(
            response,
            Err(TransportError::Remote(message))
            if message.contains("invalid gRPC payload")
        ));
    }
}
