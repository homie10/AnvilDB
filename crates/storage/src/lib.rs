use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use db_core::{Command, DbError, DbResult, Index, LogEntry, NodeId, Term};

const WAL_OP_PUT: u8 = 1;
const WAL_OP_DELETE: u8 = 2;
const WAL_HEADER_LEN: usize = 1 + 8 + 8 + 4 + 4;
const COMMIT_INDEX_META_LEN: usize = 8;
const TERM_VOTE_META_LEN: usize = 8 + 1 + 8;
const SNAPSHOT_HEADER_LEN: usize = 8 + 8 + 4;
const SNAPSHOT_KV_HEADER_LEN: usize = 4 + 4;

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::Io(format!("{context}: {error}"))
}

fn wal_corruption(message: impl Into<String>) -> DbError {
    DbError::CorruptWal(message.into())
}

fn encode_entry(entry: &LogEntry, buffer: &mut Vec<u8>) -> DbResult<()> {
    let (opcode, key, value) = match &entry.command {
        Command::Put { key, value } => (WAL_OP_PUT, key.as_str(), Some(value.as_str())),
        Command::Delete { key } => (WAL_OP_DELETE, key.as_str(), None),
    };

    let key_bytes = key.as_bytes();
    let key_len = u32::try_from(key_bytes.len())
        .map_err(|_| wal_corruption("key length exceeds WAL format limits"))?;

    let value_bytes = value.map(str::as_bytes).unwrap_or_default();
    let value_len = u32::try_from(value_bytes.len())
        .map_err(|_| wal_corruption("value length exceeds WAL format limits"))?;

    buffer.push(opcode);
    buffer.extend_from_slice(&entry.term.to_le_bytes());
    buffer.extend_from_slice(&entry.index.to_le_bytes());
    buffer.extend_from_slice(&key_len.to_le_bytes());
    buffer.extend_from_slice(&value_len.to_le_bytes());
    buffer.extend_from_slice(key_bytes);
    buffer.extend_from_slice(value_bytes);

    Ok(())
}

fn decode_entries(bytes: &[u8]) -> DbResult<Vec<LogEntry>> {
    let mut entries = Vec::new();
    let mut cursor = 0usize;

    while cursor < bytes.len() {
        if bytes.len() - cursor < WAL_HEADER_LEN {
            return Err(wal_corruption("truncated WAL entry header"));
        }

        let opcode = bytes[cursor];
        cursor += 1;

        let term = u64::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| wal_corruption("invalid WAL term bytes"))?,
        );
        cursor += 8;

        let index = u64::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| wal_corruption("invalid WAL index bytes"))?,
        );
        cursor += 8;

        let key_len = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| wal_corruption("invalid WAL key length bytes"))?,
        ) as usize;
        cursor += 4;

        let value_len = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| wal_corruption("invalid WAL value length bytes"))?,
        ) as usize;
        cursor += 4;

        let payload_len = key_len
            .checked_add(value_len)
            .ok_or_else(|| wal_corruption("WAL payload length overflow"))?;

        if bytes.len() - cursor < payload_len {
            return Err(wal_corruption("truncated WAL payload"));
        }

        let key = String::from_utf8(bytes[cursor..cursor + key_len].to_vec())
            .map_err(|_| wal_corruption("invalid UTF-8 key in WAL"))?;
        cursor += key_len;

        let value_slice = &bytes[cursor..cursor + value_len];
        cursor += value_len;

        let command = match opcode {
            WAL_OP_PUT => {
                let value = String::from_utf8(value_slice.to_vec())
                    .map_err(|_| wal_corruption("invalid UTF-8 value in WAL"))?;
                Command::Put { key, value }
            }
            WAL_OP_DELETE => {
                if value_len != 0 {
                    return Err(wal_corruption(
                        "delete record must not carry a value payload",
                    ));
                }
                Command::Delete { key }
            }
            _ => return Err(wal_corruption(format!("unknown WAL opcode {opcode}"))),
        };

        entries.push(LogEntry {
            term,
            index,
            command,
        });
    }

    Ok(entries)
}

fn validate_entry_sequence(entries: &[LogEntry]) -> DbResult<()> {
    if entries.is_empty() {
        return Ok(());
    }

    if entries[0].index == 0 {
        return Err(wal_corruption(
            "non-contiguous index sequence: index 0 is invalid",
        ));
    }

    for (offset, entry) in entries.iter().enumerate() {
        let expected_index = entries[0].index + (offset as Index);
        if entry.index != expected_index {
            return Err(wal_corruption(format!(
                "non-contiguous index sequence: expected {expected_index}, found {}",
                entry.index
            )));
        }
    }

    Ok(())
}

#[derive(Debug, Default, Clone)]
pub struct MemLog {
    base_index: Index,
    base_term: Term,
    entries: Vec<LogEntry>,
}

impl MemLog {
    pub fn append(&mut self, entry: LogEntry) {
        debug_assert_eq!(
            entry.index,
            self.last_index() + 1,
            "memlog append must be contiguous"
        );
        self.entries.push(entry);
    }

    pub fn append_entries(&mut self, entries: impl IntoIterator<Item = LogEntry>) {
        for entry in entries {
            self.append(entry);
        }
    }

    pub fn last_index(&self) -> Index {
        self.entries
            .last()
            .map_or(self.base_index, |entry| entry.index)
    }

    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.base_term, |entry| entry.term)
    }

    pub fn base_index(&self) -> Index {
        self.base_index
    }

    pub fn base_term(&self) -> Term {
        self.base_term
    }

    pub fn get(&self, index: Index) -> Option<&LogEntry> {
        if index == 0 || index <= self.base_index {
            return None;
        }

        self.entries.get((index - self.base_index - 1) as usize)
    }

    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.base_index && self.base_index > 0 {
            return Some(self.base_term);
        }
        self.get(index).map(|entry| entry.term)
    }

    pub fn entries_from(&self, index: Index) -> Vec<LogEntry> {
        if self.entries.is_empty() {
            return Vec::new();
        }

        let first_index = self.base_index + 1;
        let start_index = index.max(first_index);
        if start_index > self.last_index() {
            return Vec::new();
        }

        self.entries
            .iter()
            .skip((start_index - first_index) as usize)
            .cloned()
            .collect()
    }

    pub fn truncate_from(&mut self, index: Index) {
        let first_index = self.base_index + 1;
        if index <= first_index {
            self.entries.clear();
        } else {
            self.entries
                .truncate((index - self.base_index - 1) as usize);
        }
    }

    pub fn install_snapshot(&mut self, last_included_index: Index, last_included_term: Term) {
        if last_included_index < self.base_index {
            return;
        }

        if last_included_index >= self.last_index() {
            self.entries.clear();
        } else {
            let drain_count = (last_included_index - self.base_index) as usize;
            self.entries.drain(0..drain_count);
        }

        self.base_index = last_included_index;
        self.base_term = last_included_term;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryStateMachine {
    data: BTreeMap<String, String>,
}

impl InMemoryStateMachine {
    pub fn apply(&mut self, command: &Command) -> Option<String> {
        match command {
            Command::Put { key, value } => self.data.insert(key.clone(), value.clone()),
            Command::Delete { key } => self.data.remove(key),
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(String::as_str)
    }

    pub fn snapshot(&self) -> BTreeMap<String, String> {
        self.data.clone()
    }

    pub fn replace_with_snapshot(&mut self, snapshot: BTreeMap<String, String>) {
        self.data = snapshot;
    }
}

#[derive(Debug, Clone)]
pub struct Wal {
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PersistentRaftMeta {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotData {
    pub last_included_index: Index,
    pub last_included_term: Term,
    pub state: BTreeMap<String, String>,
}

impl Wal {
    pub fn open(path: impl Into<PathBuf>) -> DbResult<Self> {
        let path = path.into();

        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| io_error("failed to open WAL", error))?;

        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn commit_index_path(&self) -> PathBuf {
        self.path.with_extension("commit")
    }

    fn raft_meta_path(&self) -> PathBuf {
        self.path.with_extension("meta")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.path.with_extension("snap")
    }

    pub fn append(&self, entry: &LogEntry) -> DbResult<()> {
        self.append_batch(std::slice::from_ref(entry))
    }

    pub fn append_batch(&self, entries: &[LogEntry]) -> DbResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| io_error("failed to open WAL for append", error))?;

        let mut encoded = Vec::new();
        for entry in entries {
            encode_entry(entry, &mut encoded)?;
        }

        file.write_all(&encoded)
            .map_err(|error| io_error("failed to write WAL records", error))?;
        file.sync_data()
            .map_err(|error| io_error("failed to sync WAL records", error))?;

        Ok(())
    }

    pub fn load(&self) -> DbResult<Vec<LogEntry>> {
        let mut file = match OpenOptions::new().read(true).open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io_error("failed to open WAL for read", error)),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| io_error("failed to read WAL", error))?;

        let entries = decode_entries(&bytes)?;
        validate_entry_sequence(&entries)?;
        Ok(entries)
    }

    pub fn persist_commit_index(&self, commit_index: Index) -> DbResult<()> {
        let commit_path = self.commit_index_path();

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&commit_path)
            .map_err(|error| io_error("failed to open commit-index metadata for write", error))?;

        file.write_all(&commit_index.to_le_bytes())
            .map_err(|error| io_error("failed to write commit-index metadata", error))?;
        file.sync_data()
            .map_err(|error| io_error("failed to sync commit-index metadata", error))?;

        Ok(())
    }

    pub fn load_commit_index(&self) -> DbResult<Index> {
        let commit_path = self.commit_index_path();

        let mut file = match OpenOptions::new().read(true).open(&commit_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => {
                return Err(io_error(
                    "failed to open commit-index metadata for read",
                    error,
                ));
            }
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| io_error("failed to read commit-index metadata", error))?;

        if bytes.is_empty() {
            return Ok(0);
        }

        if bytes.len() != COMMIT_INDEX_META_LEN {
            return Err(wal_corruption(format!(
                "commit-index metadata has invalid length {}, expected {COMMIT_INDEX_META_LEN}",
                bytes.len(),
            )));
        }

        let bytes: [u8; COMMIT_INDEX_META_LEN] = bytes
            .try_into()
            .map_err(|_| wal_corruption("invalid commit-index metadata bytes"))?;
        Ok(Index::from_le_bytes(bytes))
    }

    pub fn persist_raft_meta(&self, current_term: Term, voted_for: Option<NodeId>) -> DbResult<()> {
        let meta_path = self.raft_meta_path();

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&meta_path)
            .map_err(|error| io_error("failed to open raft metadata for write", error))?;

        let mut bytes = Vec::with_capacity(TERM_VOTE_META_LEN);
        bytes.extend_from_slice(&current_term.to_le_bytes());
        bytes.push(u8::from(voted_for.is_some()));
        bytes.extend_from_slice(&voted_for.unwrap_or(0).to_le_bytes());

        file.write_all(&bytes)
            .map_err(|error| io_error("failed to write raft metadata", error))?;
        file.sync_data()
            .map_err(|error| io_error("failed to sync raft metadata", error))?;

        Ok(())
    }

    pub fn load_raft_meta(&self) -> DbResult<PersistentRaftMeta> {
        let meta_path = self.raft_meta_path();

        let mut file = match OpenOptions::new().read(true).open(&meta_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PersistentRaftMeta::default());
            }
            Err(error) => return Err(io_error("failed to open raft metadata for read", error)),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| io_error("failed to read raft metadata", error))?;

        if bytes.is_empty() {
            return Ok(PersistentRaftMeta::default());
        }

        if bytes.len() != TERM_VOTE_META_LEN {
            return Err(wal_corruption(format!(
                "raft metadata has invalid length {}, expected {TERM_VOTE_META_LEN}",
                bytes.len(),
            )));
        }

        let current_term = Term::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| wal_corruption("invalid raft term metadata bytes"))?,
        );

        let vote_flag = bytes[8];
        if vote_flag > 1 {
            return Err(wal_corruption(format!(
                "raft vote metadata has invalid presence flag {vote_flag}",
            )));
        }

        let voted_for_raw = NodeId::from_le_bytes(
            bytes[9..TERM_VOTE_META_LEN]
                .try_into()
                .map_err(|_| wal_corruption("invalid raft voted-for metadata bytes"))?,
        );

        let voted_for = if vote_flag == 1 {
            Some(voted_for_raw)
        } else {
            None
        };

        Ok(PersistentRaftMeta {
            current_term,
            voted_for,
        })
    }

    pub fn persist_snapshot(&self, snapshot: &SnapshotData) -> DbResult<()> {
        let snapshot_path = self.snapshot_path();

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&snapshot_path)
            .map_err(|error| io_error("failed to open snapshot for write", error))?;

        let pair_count = u32::try_from(snapshot.state.len())
            .map_err(|_| wal_corruption("snapshot key count exceeds format limits"))?;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&snapshot.last_included_index.to_le_bytes());
        bytes.extend_from_slice(&snapshot.last_included_term.to_le_bytes());
        bytes.extend_from_slice(&pair_count.to_le_bytes());

        for (key, value) in snapshot.state.iter() {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();

            let key_len = u32::try_from(key_bytes.len())
                .map_err(|_| wal_corruption("snapshot key length exceeds format limits"))?;
            let value_len = u32::try_from(value_bytes.len())
                .map_err(|_| wal_corruption("snapshot value length exceeds format limits"))?;

            bytes.extend_from_slice(&key_len.to_le_bytes());
            bytes.extend_from_slice(&value_len.to_le_bytes());
            bytes.extend_from_slice(key_bytes);
            bytes.extend_from_slice(value_bytes);
        }

        file.write_all(&bytes)
            .map_err(|error| io_error("failed to write snapshot", error))?;
        file.sync_data()
            .map_err(|error| io_error("failed to sync snapshot", error))?;

        Ok(())
    }

    pub fn load_snapshot(&self) -> DbResult<Option<SnapshotData>> {
        let snapshot_path = self.snapshot_path();

        let mut file = match OpenOptions::new().read(true).open(&snapshot_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error("failed to open snapshot for read", error)),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| io_error("failed to read snapshot", error))?;

        if bytes.is_empty() {
            return Ok(None);
        }

        if bytes.len() < SNAPSHOT_HEADER_LEN {
            return Err(wal_corruption("truncated snapshot header"));
        }

        let mut cursor = 0usize;

        let last_included_index = Index::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| wal_corruption("invalid snapshot index bytes"))?,
        );
        cursor += 8;

        let last_included_term = Term::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| wal_corruption("invalid snapshot term bytes"))?,
        );
        cursor += 8;

        let pair_count = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| wal_corruption("invalid snapshot pair-count bytes"))?,
        ) as usize;
        cursor += 4;

        let mut state = BTreeMap::new();
        for _ in 0..pair_count {
            if bytes.len() - cursor < SNAPSHOT_KV_HEADER_LEN {
                return Err(wal_corruption("truncated snapshot key/value header"));
            }

            let key_len = u32::from_le_bytes(
                bytes[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| wal_corruption("invalid snapshot key length bytes"))?,
            ) as usize;
            cursor += 4;

            let value_len = u32::from_le_bytes(
                bytes[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| wal_corruption("invalid snapshot value length bytes"))?,
            ) as usize;
            cursor += 4;

            let payload_len = key_len
                .checked_add(value_len)
                .ok_or_else(|| wal_corruption("snapshot payload length overflow"))?;
            if bytes.len() - cursor < payload_len {
                return Err(wal_corruption("truncated snapshot payload"));
            }

            let key = String::from_utf8(bytes[cursor..cursor + key_len].to_vec())
                .map_err(|_| wal_corruption("invalid UTF-8 key in snapshot"))?;
            cursor += key_len;

            let value = String::from_utf8(bytes[cursor..cursor + value_len].to_vec())
                .map_err(|_| wal_corruption("invalid UTF-8 value in snapshot"))?;
            cursor += value_len;

            state.insert(key, value);
        }

        if cursor != bytes.len() {
            return Err(wal_corruption("snapshot contains trailing bytes"));
        }

        Ok(Some(SnapshotData {
            last_included_index,
            last_included_term,
            state,
        }))
    }

    pub fn truncate_to(&self, last_index: Index) -> DbResult<()> {
        let mut entries = self.load()?;
        entries.retain(|entry| entry.index <= last_index);
        self.rewrite(&entries)
    }

    pub fn truncate_prefix(&self, through_index: Index) -> DbResult<()> {
        let mut entries = self.load()?;
        entries.retain(|entry| entry.index > through_index);
        self.rewrite(&entries)
    }

    fn rewrite(&self, entries: &[LogEntry]) -> DbResult<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|error| io_error("failed to open WAL for rewrite", error))?;

        let mut encoded = Vec::new();
        for entry in entries {
            encode_entry(entry, &mut encoded)?;
        }

        file.write_all(&encoded)
            .map_err(|error| io_error("failed to rewrite WAL", error))?;
        file.sync_data()
            .map_err(|error| io_error("failed to sync WAL rewrite", error))?;

        Ok(())
    }
}

pub fn apply_entry(
    log: &MemLog,
    state_machine: &mut InMemoryStateMachine,
    index: Index,
) -> DbResult<Option<String>> {
    let entry = log.get(index).ok_or(DbError::MissingLogEntry(index))?;
    Ok(state_machine.apply(&entry.command))
}

pub fn recover_state_from_wal(wal: &Wal) -> DbResult<(MemLog, InMemoryStateMachine)> {
    let entries = wal.load()?;
    let snapshot = wal.load_snapshot()?;

    let mut log = MemLog::default();
    let mut state_machine = InMemoryStateMachine::default();

    if let Some(snapshot) = snapshot {
        state_machine.replace_with_snapshot(snapshot.state);
        log.install_snapshot(snapshot.last_included_index, snapshot.last_included_term);
    }

    for entry in entries {
        state_machine.apply(&entry.command);
        log.append(entry);
    }

    Ok((log, state_machine))
}

pub fn recover_node_from_wal(wal: &Wal) -> DbResult<(MemLog, InMemoryStateMachine, Index)> {
    let entries = wal.load()?;
    let mut commit_index = wal.load_commit_index()?;
    let snapshot = wal.load_snapshot()?;

    let mut log = MemLog::default();
    let mut state_machine = InMemoryStateMachine::default();
    let snapshot_index = if let Some(snapshot) = snapshot {
        state_machine.replace_with_snapshot(snapshot.state);
        log.install_snapshot(snapshot.last_included_index, snapshot.last_included_term);
        snapshot.last_included_index
    } else {
        0
    };

    if let Some(first_entry) = entries.first() {
        let expected_first_index = snapshot_index + 1;
        if first_entry.index != expected_first_index {
            return Err(wal_corruption(format!(
                "WAL after snapshot must start at index {expected_first_index}, found {}",
                first_entry.index
            )));
        }
    }

    let recovered_last_index = entries.last().map_or(snapshot_index, |entry| entry.index);
    commit_index = commit_index.max(snapshot_index);
    if commit_index > recovered_last_index {
        return Err(wal_corruption(format!(
            "commit-index metadata {commit_index} exceeds recovered log end {recovered_last_index}",
        )));
    }

    for entry in entries {
        log.append(entry.clone());
        if entry.index <= commit_index {
            state_machine.apply(&entry.command);
        }
    }

    Ok((log, state_machine, commit_index))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use db_core::{Command, DbError, LogEntry};

    use super::{
        recover_node_from_wal, recover_state_from_wal, PersistentRaftMeta, SnapshotData, Wal,
    };

    fn temp_wal_path(suffix: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();

        std::env::temp_dir().join(format!(
            "anvildb-wal-{suffix}-{}-{nonce}.bin",
            std::process::id()
        ))
    }

    fn cleanup_wal_artifacts(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("commit"));
        let _ = fs::remove_file(path.with_extension("meta"));
        let _ = fs::remove_file(path.with_extension("snap"));
    }

    #[test]
    fn wal_round_trip_records() {
        let path = temp_wal_path("round-trip");
        let wal = Wal::open(&path).expect("wal should open");

        wal.append(&LogEntry {
            term: 1,
            index: 1,
            command: Command::Put {
                key: "k".to_owned(),
                value: "v".to_owned(),
            },
        })
        .expect("first entry should append");

        wal.append(&LogEntry {
            term: 1,
            index: 2,
            command: Command::Delete {
                key: "k".to_owned(),
            },
        })
        .expect("second entry should append");

        let entries = wal.load().expect("wal should load");
        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[0].command,
            Command::Put {
                ref key,
                ref value
            } if key == "k" && value == "v"
        ));
        assert!(matches!(
            entries[1].command,
            Command::Delete { ref key } if key == "k"
        ));

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn wal_truncate_and_recover_state() {
        let path = temp_wal_path("truncate-recover");
        let wal = Wal::open(&path).expect("wal should open");

        wal.append_batch(&[
            LogEntry {
                term: 1,
                index: 1,
                command: Command::Put {
                    key: "a".to_owned(),
                    value: "1".to_owned(),
                },
            },
            LogEntry {
                term: 1,
                index: 2,
                command: Command::Put {
                    key: "b".to_owned(),
                    value: "2".to_owned(),
                },
            },
            LogEntry {
                term: 1,
                index: 3,
                command: Command::Delete {
                    key: "a".to_owned(),
                },
            },
        ])
        .expect("entries should append");

        wal.truncate_to(2).expect("truncate should succeed");

        let (log, state_machine) = recover_state_from_wal(&wal).expect("recovery should succeed");

        assert_eq!(log.len(), 2);
        assert_eq!(state_machine.get("a"), Some("1"));
        assert_eq!(state_machine.get("b"), Some("2"));

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn wal_detects_non_contiguous_indexes_on_load() {
        let path = temp_wal_path("invalid-index");
        let wal = Wal::open(&path).expect("wal should open");

        wal.append_batch(&[
            LogEntry {
                term: 1,
                index: 1,
                command: Command::Put {
                    key: "a".to_owned(),
                    value: "1".to_owned(),
                },
            },
            LogEntry {
                term: 1,
                index: 3,
                command: Command::Put {
                    key: "b".to_owned(),
                    value: "2".to_owned(),
                },
            },
        ])
        .expect("entries should append");

        let result = wal.load();
        assert!(matches!(result, Err(DbError::CorruptWal(_))));

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn recover_node_applies_only_committed_prefix() {
        let path = temp_wal_path("commit-prefix");
        let wal = Wal::open(&path).expect("wal should open");

        wal.append_batch(&[
            LogEntry {
                term: 1,
                index: 1,
                command: Command::Put {
                    key: "a".to_owned(),
                    value: "1".to_owned(),
                },
            },
            LogEntry {
                term: 1,
                index: 2,
                command: Command::Put {
                    key: "b".to_owned(),
                    value: "2".to_owned(),
                },
            },
        ])
        .expect("entries should append");

        wal.persist_commit_index(1)
            .expect("commit-index metadata should persist");

        let (log, state_machine, commit_index) =
            recover_node_from_wal(&wal).expect("node recovery should succeed");

        assert_eq!(commit_index, 1);
        assert_eq!(log.len(), 2);
        assert_eq!(state_machine.get("a"), Some("1"));
        assert_eq!(state_machine.get("b"), None);

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn recover_node_rejects_commit_index_beyond_log_end() {
        let path = temp_wal_path("commit-out-of-range");
        let wal = Wal::open(&path).expect("wal should open");

        wal.append(&LogEntry {
            term: 1,
            index: 1,
            command: Command::Put {
                key: "a".to_owned(),
                value: "1".to_owned(),
            },
        })
        .expect("entry should append");

        wal.persist_commit_index(2)
            .expect("commit-index metadata should persist");

        let result = recover_node_from_wal(&wal);
        assert!(matches!(result, Err(DbError::CorruptWal(_))));

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn raft_meta_round_trip_persists_term_and_vote() {
        let path = temp_wal_path("raft-meta-round-trip");
        let wal = Wal::open(&path).expect("wal should open");

        assert_eq!(
            wal.load_raft_meta()
                .expect("default raft metadata should load"),
            PersistentRaftMeta::default()
        );

        wal.persist_raft_meta(7, Some(3))
            .expect("raft metadata should persist");
        assert_eq!(
            wal.load_raft_meta().expect("raft metadata should load"),
            PersistentRaftMeta {
                current_term: 7,
                voted_for: Some(3),
            }
        );

        wal.persist_raft_meta(8, None)
            .expect("raft metadata should overwrite");
        assert_eq!(
            wal.load_raft_meta().expect("raft metadata should load"),
            PersistentRaftMeta {
                current_term: 8,
                voted_for: None,
            }
        );

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn raft_meta_rejects_invalid_length() {
        let path = temp_wal_path("raft-meta-invalid-length");
        let wal = Wal::open(&path).expect("wal should open");

        fs::write(path.with_extension("meta"), [1_u8, 2, 3])
            .expect("invalid raft metadata should be written");

        let result = wal.load_raft_meta();
        assert!(matches!(result, Err(DbError::CorruptWal(_))));

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn snapshot_round_trip_and_recovery_with_compacted_wal_prefix() {
        let path = temp_wal_path("snapshot-round-trip");
        let wal = Wal::open(&path).expect("wal should open");

        wal.append_batch(&[
            LogEntry {
                term: 1,
                index: 1,
                command: Command::Put {
                    key: "a".to_owned(),
                    value: "1".to_owned(),
                },
            },
            LogEntry {
                term: 1,
                index: 2,
                command: Command::Put {
                    key: "b".to_owned(),
                    value: "2".to_owned(),
                },
            },
            LogEntry {
                term: 2,
                index: 3,
                command: Command::Put {
                    key: "c".to_owned(),
                    value: "3".to_owned(),
                },
            },
            LogEntry {
                term: 2,
                index: 4,
                command: Command::Delete {
                    key: "b".to_owned(),
                },
            },
        ])
        .expect("entries should append");

        wal.persist_snapshot(&SnapshotData {
            last_included_index: 2,
            last_included_term: 1,
            state: BTreeMap::from([
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), "2".to_owned()),
            ]),
        })
        .expect("snapshot should persist");
        wal.truncate_prefix(2)
            .expect("snapshot compaction should rewrite WAL");
        wal.persist_commit_index(3)
            .expect("commit-index should persist");

        let (log, state_machine, commit_index) =
            recover_node_from_wal(&wal).expect("recovery should succeed");

        assert_eq!(commit_index, 3);
        assert_eq!(log.base_index(), 2);
        assert_eq!(log.base_term(), 1);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.len(), 2);
        assert_eq!(state_machine.get("a"), Some("1"));
        assert_eq!(state_machine.get("b"), Some("2"));
        assert_eq!(state_machine.get("c"), Some("3"));

        cleanup_wal_artifacts(&path);
    }

    #[test]
    fn recover_rejects_wal_suffix_that_does_not_follow_snapshot_index() {
        let path = temp_wal_path("snapshot-gap");
        let wal = Wal::open(&path).expect("wal should open");

        wal.persist_snapshot(&SnapshotData {
            last_included_index: 5,
            last_included_term: 2,
            state: BTreeMap::new(),
        })
        .expect("snapshot should persist");

        wal.append(&LogEntry {
            term: 3,
            index: 7,
            command: Command::Put {
                key: "x".to_owned(),
                value: "bad-gap".to_owned(),
            },
        })
        .expect("entry should append");
        wal.persist_commit_index(7)
            .expect("commit-index should persist");

        let result = recover_node_from_wal(&wal);
        assert!(matches!(result, Err(DbError::CorruptWal(_))));

        cleanup_wal_artifacts(&path);
    }
}
