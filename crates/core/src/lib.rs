use std::error::Error;
use std::fmt;

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: Index,
    pub command: Command,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DbError {
    NodeNotFound(NodeId),

    LeaderMissing(NodeId),

    MissingLogEntry(Index),

    ClusterTooSmall,

    DuplicateNode(NodeId),

    Io(String),

    CorruptWal(String),
}

pub type DbResult<T> = Result<T, DbError>;

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeNotFound(node_id) => write!(f, "node {node_id} not found"),
            Self::LeaderMissing(node_id) => write!(f, "leader {node_id} missing from cluster"),
            Self::MissingLogEntry(index) => write!(f, "log entry with index {index} not found"),
            Self::ClusterTooSmall => write!(f, "invalid cluster: at least 3 nodes required"),
            Self::DuplicateNode(node_id) => {
                write!(f, "invalid cluster: duplicate node id {node_id}")
            }
            Self::Io(message) => write!(f, "io error: {message}"),
            Self::CorruptWal(message) => write!(f, "corrupt wal: {message}"),
        }
    }
}

impl Error for DbError {}
