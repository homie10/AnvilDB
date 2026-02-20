use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableName {
    Kv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnName {
    Key,
    Value,
}

impl ColumnName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Value => "value",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Predicate {
    pub column: ColumnName,
    pub op: ComparisonOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlStatement {
    Insert {
        table: TableName,
        key: String,
        value: String,
    },
    Delete {
        table: TableName,
        predicate: Predicate,
    },
    Update {
        table: TableName,
        value: String,
        predicate: Predicate,
    },
    Select {
        table: TableName,
        projections: Vec<ColumnName>,
        predicate: Option<Predicate>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    Scan {
        table: TableName,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },
    Projection {
        input: Box<LogicalPlan>,
        columns: Vec<ColumnName>,
    },
    InsertValues {
        table: TableName,
        key: String,
        value: String,
    },
    DeleteRows {
        table: TableName,
        predicate: Predicate,
    },
    UpdateRows {
        table: TableName,
        value: String,
        predicate: Predicate,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvExecutionPlan {
    Select {
        columns: Vec<ColumnName>,
        predicate: Option<Predicate>,
    },
    Insert {
        key: String,
        value: String,
    },
    Delete {
        predicate: Predicate,
    },
    Update {
        value: String,
        predicate: Predicate,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    message: String,
}

impl SqlError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sql error: {}", self.message)
    }
}

impl Error for SqlError {}

pub type SqlResult<T> = Result<T, SqlError>;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    StringLiteral(String),
    Comma,
    LParen,
    RParen,
    Eq,
    NotEq,
    Star,
    Semicolon,
}

fn lex(sql: &str) -> SqlResult<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            let _ = chars.next();
            continue;
        }

        match ch {
            ',' => {
                tokens.push(Token::Comma);
                let _ = chars.next();
            }
            '(' => {
                tokens.push(Token::LParen);
                let _ = chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                let _ = chars.next();
            }
            '=' => {
                tokens.push(Token::Eq);
                let _ = chars.next();
            }
            '*' => {
                tokens.push(Token::Star);
                let _ = chars.next();
            }
            ';' => {
                tokens.push(Token::Semicolon);
                let _ = chars.next();
            }
            '!' => {
                let _ = chars.next();
                match chars.next() {
                    Some('=') => tokens.push(Token::NotEq),
                    _ => return Err(SqlError::new("unexpected '!' (expected '!=')")),
                }
            }
            '\'' => {
                let _ = chars.next();
                let mut literal = String::new();
                let mut closed = false;

                while let Some(next) = chars.next() {
                    if next == '\'' {
                        if chars.peek().is_some_and(|peek| *peek == '\'') {
                            literal.push('\'');
                            let _ = chars.next();
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    literal.push(next);
                }

                if !closed {
                    return Err(SqlError::new("unterminated single-quoted literal"));
                }

                tokens.push(Token::StringLiteral(literal));
            }
            _ => {
                if ch.is_ascii_alphanumeric() || ch == '_' {
                    let mut word = String::new();
                    while let Some(next) = chars.peek().copied() {
                        if next.is_ascii_alphanumeric() || next == '_' {
                            word.push(next);
                            let _ = chars.next();
                        } else {
                            break;
                        }
                    }
                    tokens.push(Token::Word(word));
                } else {
                    return Err(SqlError::new(format!("unexpected character '{ch}'")));
                }
            }
        }
    }

    Ok(tokens)
}

#[derive(Debug)]
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn expect_keyword(&mut self, keyword: &str) -> SqlResult<()> {
        let Some(Token::Word(word)) = self.next() else {
            return Err(SqlError::new(format!("expected keyword {keyword}")));
        };

        if !word.eq_ignore_ascii_case(keyword) {
            return Err(SqlError::new(format!(
                "expected keyword {keyword}, found {word}"
            )));
        }

        Ok(())
    }

    fn expect_table(&mut self) -> SqlResult<TableName> {
        let Some(Token::Word(word)) = self.next() else {
            return Err(SqlError::new("expected table name"));
        };

        if word.eq_ignore_ascii_case("kv") {
            Ok(TableName::Kv)
        } else {
            Err(SqlError::new(format!(
                "unsupported table '{word}' (expected kv)"
            )))
        }
    }

    fn expect_column(&mut self) -> SqlResult<ColumnName> {
        let Some(Token::Word(word)) = self.next() else {
            return Err(SqlError::new("expected column name"));
        };

        if word.eq_ignore_ascii_case("key") {
            Ok(ColumnName::Key)
        } else if word.eq_ignore_ascii_case("value") {
            Ok(ColumnName::Value)
        } else {
            Err(SqlError::new(format!(
                "unsupported column '{word}' (expected key or value)"
            )))
        }
    }

    fn expect_string_literal(&mut self) -> SqlResult<String> {
        match self.next() {
            Some(Token::StringLiteral(value)) => Ok(value),
            _ => Err(SqlError::new("expected single-quoted literal")),
        }
    }

    fn expect_token(&mut self, expected: Token) -> SqlResult<()> {
        let token = self.next();
        if token == Some(expected.clone()) {
            return Ok(());
        }

        Err(SqlError::new(format!(
            "expected token {:?}, found {:?}",
            expected, token
        )))
    }

    fn maybe_token(&mut self, expected: Token) -> bool {
        if self.peek() == Some(&expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn parse_statement(&mut self) -> SqlResult<SqlStatement> {
        let statement = match self.peek() {
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("insert") => {
                self.parse_insert_statement()?
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("delete") => {
                self.parse_delete_statement()?
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("update") => {
                self.parse_update_statement()?
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("select") => {
                self.parse_select_statement()?
            }
            Some(_) => {
                return Err(SqlError::new(
                    "unsupported SQL statement; expected INSERT/DELETE/UPDATE/SELECT",
                ));
            }
            None => return Err(SqlError::new("empty SQL statement")),
        };

        let _ = self.maybe_token(Token::Semicolon);
        if self.peek().is_some() {
            return Err(SqlError::new("unexpected tokens after end of statement"));
        }

        Ok(statement)
    }

    fn parse_insert_statement(&mut self) -> SqlResult<SqlStatement> {
        self.expect_keyword("INSERT")?;
        self.expect_keyword("INTO")?;
        let table = self.expect_table()?;

        let mut columns = vec![ColumnName::Key, ColumnName::Value];
        if self.maybe_token(Token::LParen) {
            columns.clear();
            loop {
                columns.push(self.expect_column()?);
                if self.maybe_token(Token::Comma) {
                    continue;
                }
                break;
            }
            self.expect_token(Token::RParen)?;
        }

        self.expect_keyword("VALUES")?;
        self.expect_token(Token::LParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.expect_string_literal()?);
            if self.maybe_token(Token::Comma) {
                continue;
            }
            break;
        }
        self.expect_token(Token::RParen)?;

        if columns.len() != values.len() {
            return Err(SqlError::new(
                "INSERT column count does not match VALUES count",
            ));
        }

        let mut mapped_key: Option<String> = None;
        let mut mapped_value: Option<String> = None;
        for (column, value) in columns.into_iter().zip(values.into_iter()) {
            match column {
                ColumnName::Key => {
                    if mapped_key.is_some() {
                        return Err(SqlError::new("INSERT specifies duplicate key column"));
                    }
                    mapped_key = Some(value);
                }
                ColumnName::Value => {
                    if mapped_value.is_some() {
                        return Err(SqlError::new("INSERT specifies duplicate value column"));
                    }
                    mapped_value = Some(value);
                }
            }
        }

        let key = mapped_key.ok_or_else(|| SqlError::new("INSERT missing key column value"))?;
        let value =
            mapped_value.ok_or_else(|| SqlError::new("INSERT missing value column value"))?;

        Ok(SqlStatement::Insert { table, key, value })
    }

    fn parse_delete_statement(&mut self) -> SqlResult<SqlStatement> {
        self.expect_keyword("DELETE")?;
        self.expect_keyword("FROM")?;
        let table = self.expect_table()?;
        self.expect_keyword("WHERE")?;
        let predicate = self.parse_predicate()?;

        Ok(SqlStatement::Delete { table, predicate })
    }

    fn parse_update_statement(&mut self) -> SqlResult<SqlStatement> {
        self.expect_keyword("UPDATE")?;
        let table = self.expect_table()?;
        self.expect_keyword("SET")?;

        let assignment_column = self.expect_column()?;
        if assignment_column != ColumnName::Value {
            return Err(SqlError::new("only `SET value = ...` is supported"));
        }
        self.expect_token(Token::Eq)?;
        let value = self.expect_string_literal()?;

        if self.maybe_token(Token::Comma) {
            return Err(SqlError::new(
                "multiple UPDATE assignments are not supported yet",
            ));
        }

        self.expect_keyword("WHERE")?;
        let predicate = self.parse_predicate()?;

        Ok(SqlStatement::Update {
            table,
            value,
            predicate,
        })
    }

    fn parse_select_statement(&mut self) -> SqlResult<SqlStatement> {
        self.expect_keyword("SELECT")?;

        let projections = if self.maybe_token(Token::Star) {
            vec![ColumnName::Key, ColumnName::Value]
        } else {
            let mut columns = Vec::new();
            loop {
                columns.push(self.expect_column()?);
                if self.maybe_token(Token::Comma) {
                    continue;
                }
                break;
            }
            columns
        };

        self.expect_keyword("FROM")?;
        let table = self.expect_table()?;
        let predicate = if let Some(Token::Word(word)) = self.peek() {
            if word.eq_ignore_ascii_case("WHERE") {
                let _ = self.next();
                Some(self.parse_predicate()?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(SqlStatement::Select {
            table,
            projections,
            predicate,
        })
    }

    fn parse_predicate(&mut self) -> SqlResult<Predicate> {
        let column = self.expect_column()?;
        let op = match self.next() {
            Some(Token::Eq) => ComparisonOp::Eq,
            Some(Token::NotEq) => ComparisonOp::NotEq,
            other => {
                return Err(SqlError::new(format!(
                    "expected comparison operator '=' or '!=', found {:?}",
                    other
                )));
            }
        };
        let value = self.expect_string_literal()?;

        Ok(Predicate { column, op, value })
    }
}

pub fn parse_statement(sql: &str) -> SqlResult<SqlStatement> {
    let tokens = lex(sql)?;
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}

pub fn plan_statement(statement: SqlStatement) -> LogicalPlan {
    match statement {
        SqlStatement::Insert { table, key, value } => {
            LogicalPlan::InsertValues { table, key, value }
        }
        SqlStatement::Delete { table, predicate } => LogicalPlan::DeleteRows { table, predicate },
        SqlStatement::Update {
            table,
            value,
            predicate,
        } => LogicalPlan::UpdateRows {
            table,
            value,
            predicate,
        },
        SqlStatement::Select {
            table,
            projections,
            predicate,
        } => {
            let mut plan = LogicalPlan::Scan { table };
            if let Some(predicate) = predicate {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                };
            }
            LogicalPlan::Projection {
                input: Box::new(plan),
                columns: projections,
            }
        }
    }
}

pub fn plan_sql(sql: &str) -> SqlResult<LogicalPlan> {
    let statement = parse_statement(sql)?;
    Ok(plan_statement(statement))
}

pub fn lower_plan_for_kv(plan: &LogicalPlan) -> SqlResult<KvExecutionPlan> {
    match plan {
        LogicalPlan::InsertValues {
            table: TableName::Kv,
            key,
            value,
        } => Ok(KvExecutionPlan::Insert {
            key: key.clone(),
            value: value.clone(),
        }),
        LogicalPlan::DeleteRows {
            table: TableName::Kv,
            predicate,
        } => Ok(KvExecutionPlan::Delete {
            predicate: predicate.clone(),
        }),
        LogicalPlan::UpdateRows {
            table: TableName::Kv,
            value,
            predicate,
        } => Ok(KvExecutionPlan::Update {
            value: value.clone(),
            predicate: predicate.clone(),
        }),
        LogicalPlan::Projection { .. } | LogicalPlan::Filter { .. } | LogicalPlan::Scan { .. } => {
            let (columns, predicate) = lower_select_plan(plan)?;
            Ok(KvExecutionPlan::Select { columns, predicate })
        }
    }
}

fn lower_select_plan(plan: &LogicalPlan) -> SqlResult<(Vec<ColumnName>, Option<Predicate>)> {
    fn recurse(
        plan: &LogicalPlan,
        columns: &mut Option<Vec<ColumnName>>,
        predicate: &mut Option<Predicate>,
    ) -> SqlResult<()> {
        match plan {
            LogicalPlan::Projection { input, columns: c } => {
                if columns.is_some() {
                    return Err(SqlError::new("multiple projection nodes are not supported"));
                }
                *columns = Some(c.clone());
                recurse(input, columns, predicate)
            }
            LogicalPlan::Filter {
                input,
                predicate: p,
            } => {
                if predicate.is_some() {
                    return Err(SqlError::new("multiple filter nodes are not supported"));
                }
                *predicate = Some(p.clone());
                recurse(input, columns, predicate)
            }
            LogicalPlan::Scan {
                table: TableName::Kv,
            } => Ok(()),
            _ => Err(SqlError::new(
                "logical plan is not a lowerable SELECT query for kv table",
            )),
        }
    }

    let mut columns = None;
    let mut predicate = None;
    recurse(plan, &mut columns, &mut predicate)?;

    Ok((
        columns.unwrap_or_else(|| vec![ColumnName::Key, ColumnName::Value]),
        predicate,
    ))
}

pub fn evaluate_predicate(key: &str, value: &str, predicate: &Predicate) -> bool {
    let lhs = match predicate.column {
        ColumnName::Key => key,
        ColumnName::Value => value,
    };

    match predicate.op {
        ComparisonOp::Eq => lhs == predicate.value.as_str(),
        ComparisonOp::NotEq => lhs != predicate.value.as_str(),
    }
}

pub fn project_row(key: &str, value: &str, columns: &[ColumnName]) -> Vec<String> {
    columns
        .iter()
        .map(|column| match column {
            ColumnName::Key => key.to_owned(),
            ColumnName::Value => value.to_owned(),
        })
        .collect()
}

pub fn column_headers(columns: &[ColumnName]) -> Vec<String> {
    columns
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect()
}

pub type TxnId = u64;
pub type Timestamp = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub begin_ts: Timestamp,
    pub end_ts: Option<Timestamp>,
    pub value: Option<String>,
    pub writer_txn: TxnId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionChain {
    versions: Vec<VersionedValue>,
}

impl VersionChain {
    pub fn versions(&self) -> &[VersionedValue] {
        &self.versions
    }

    pub fn latest_visible(&self, read_ts: Timestamp) -> Option<&VersionedValue> {
        self.versions.iter().rev().find(|version| {
            version.begin_ts <= read_ts
                && version
                    .end_ts
                    .map(|end_ts| read_ts < end_ts)
                    .unwrap_or(true)
        })
    }

    pub fn append_committed(
        &mut self,
        writer_txn: TxnId,
        commit_ts: Timestamp,
        value: Option<String>,
    ) {
        if let Some(last) = self.versions.last_mut() {
            if last.end_ts.is_none() {
                last.end_ts = Some(commit_ts);
            }
        }

        self.versions.push(VersionedValue {
            begin_ts: commit_ts,
            end_ts: None,
            value,
            writer_txn,
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transaction {
    pub id: TxnId,
    pub read_ts: Timestamp,
}

#[derive(Debug, Clone, Default)]
pub struct MvccCatalog {
    next_txn_id: TxnId,
    chains: BTreeMap<String, VersionChain>,
}

impl MvccCatalog {
    pub fn begin_transaction(&mut self, read_ts: Timestamp) -> Transaction {
        self.next_txn_id = self.next_txn_id.saturating_add(1);
        Transaction {
            id: self.next_txn_id,
            read_ts,
        }
    }

    pub fn apply_committed_put(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        commit_ts: Timestamp,
        writer_txn: TxnId,
    ) {
        let chain = self.chains.entry(key.into()).or_default();
        chain.append_committed(writer_txn, commit_ts, Some(value.into()));
    }

    pub fn apply_committed_delete(
        &mut self,
        key: impl Into<String>,
        commit_ts: Timestamp,
        writer_txn: TxnId,
    ) {
        let chain = self.chains.entry(key.into()).or_default();
        chain.append_committed(writer_txn, commit_ts, None);
    }

    pub fn read_visible(&self, key: &str, read_ts: Timestamp) -> Option<&str> {
        self.chains
            .get(key)
            .and_then(|chain| chain.latest_visible(read_ts))
            .and_then(|version| version.value.as_deref())
    }

    pub fn visible_rows(&self, read_ts: Timestamp) -> Vec<(String, String)> {
        self.chains
            .iter()
            .filter_map(|(key, chain)| {
                chain
                    .latest_visible(read_ts)
                    .and_then(|version| version.value.clone().map(|value| (key.clone(), value)))
            })
            .collect()
    }

    pub fn chain(&self, key: &str) -> Option<&VersionChain> {
        self.chains.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        column_headers, evaluate_predicate, lower_plan_for_kv, parse_statement, plan_sql,
        plan_statement, project_row, ColumnName, ComparisonOp, KvExecutionPlan, LogicalPlan,
        MvccCatalog, Predicate, SqlStatement, TableName,
    };

    #[test]
    fn parses_insert_delete_update_and_select_statements() {
        let insert = parse_statement("INSERT INTO kv (key, value) VALUES ('planet', 'saturn');")
            .expect("insert should parse");
        assert_eq!(
            insert,
            SqlStatement::Insert {
                table: TableName::Kv,
                key: "planet".to_owned(),
                value: "saturn".to_owned(),
            }
        );

        let delete =
            parse_statement("DELETE FROM kv WHERE key = 'planet'").expect("delete should parse");
        assert_eq!(
            delete,
            SqlStatement::Delete {
                table: TableName::Kv,
                predicate: Predicate {
                    column: ColumnName::Key,
                    op: ComparisonOp::Eq,
                    value: "planet".to_owned(),
                },
            }
        );

        let update = parse_statement("UPDATE kv SET value = 'gas' WHERE key != 'moon'")
            .expect("update should parse");
        assert_eq!(
            update,
            SqlStatement::Update {
                table: TableName::Kv,
                value: "gas".to_owned(),
                predicate: Predicate {
                    column: ColumnName::Key,
                    op: ComparisonOp::NotEq,
                    value: "moon".to_owned(),
                },
            }
        );

        let select = parse_statement("SELECT key, value FROM kv WHERE value != 'cold'")
            .expect("select should parse");
        assert_eq!(
            select,
            SqlStatement::Select {
                table: TableName::Kv,
                projections: vec![ColumnName::Key, ColumnName::Value],
                predicate: Some(Predicate {
                    column: ColumnName::Value,
                    op: ComparisonOp::NotEq,
                    value: "cold".to_owned(),
                }),
            }
        );
    }

    #[test]
    fn parse_rejects_unsupported_forms() {
        let unsupported =
            parse_statement("CREATE TABLE kv (key TEXT)").expect_err("ddl should be rejected");
        assert!(unsupported
            .to_string()
            .contains("unsupported SQL statement"));

        let bad_update = parse_statement("UPDATE kv SET key = 'x' WHERE key = 'y'")
            .expect_err("key assignment should be rejected");
        assert!(bad_update
            .to_string()
            .contains("only `SET value = ...` is supported"));
    }

    #[test]
    fn planner_builds_tree_and_lowering_extracts_kv_plan() {
        let select_plan = plan_sql("SELECT value FROM kv WHERE key = 'planet'")
            .expect("select plan should build");
        assert!(matches!(select_plan, LogicalPlan::Projection { .. }));

        let lowered = lower_plan_for_kv(&select_plan).expect("lowering should succeed");
        assert_eq!(
            lowered,
            KvExecutionPlan::Select {
                columns: vec![ColumnName::Value],
                predicate: Some(Predicate {
                    column: ColumnName::Key,
                    op: ComparisonOp::Eq,
                    value: "planet".to_owned(),
                }),
            }
        );

        let insert_stmt =
            parse_statement("INSERT INTO kv (key, value) VALUES ('a', '1')").expect("insert");
        let insert_plan = plan_statement(insert_stmt);
        let lowered_insert = lower_plan_for_kv(&insert_plan).expect("insert lowering");
        assert_eq!(
            lowered_insert,
            KvExecutionPlan::Insert {
                key: "a".to_owned(),
                value: "1".to_owned(),
            }
        );
    }

    #[test]
    fn predicate_and_projection_helpers_work() {
        let predicate = Predicate {
            column: ColumnName::Value,
            op: ComparisonOp::NotEq,
            value: "earth".to_owned(),
        };
        assert!(evaluate_predicate("planet", "saturn", &predicate));
        assert!(!evaluate_predicate("planet", "earth", &predicate));

        let row = project_row("planet", "saturn", &[ColumnName::Key, ColumnName::Value]);
        assert_eq!(row, vec!["planet".to_owned(), "saturn".to_owned()]);
        assert_eq!(
            column_headers(&[ColumnName::Key, ColumnName::Value]),
            vec!["key".to_owned(), "value".to_owned()]
        );
    }

    #[test]
    fn mvcc_catalog_tracks_version_visibility() {
        let mut catalog = MvccCatalog::default();
        let txn1 = catalog.begin_transaction(1);
        let txn2 = catalog.begin_transaction(2);
        assert_eq!(txn1.id, 1);
        assert_eq!(txn2.id, 2);

        catalog.apply_committed_put("planet", "earth", 10, txn1.id);
        catalog.apply_committed_put("planet", "saturn", 20, txn2.id);
        catalog.apply_committed_delete("planet", 30, txn2.id);

        assert_eq!(catalog.read_visible("planet", 9), None);
        assert_eq!(catalog.read_visible("planet", 10), Some("earth"));
        assert_eq!(catalog.read_visible("planet", 25), Some("saturn"));
        assert_eq!(catalog.read_visible("planet", 35), None);

        let chain = catalog.chain("planet").expect("chain should exist");
        assert_eq!(chain.versions().len(), 3);
        assert_eq!(chain.versions()[0].end_ts, Some(20));
        assert_eq!(chain.versions()[1].end_ts, Some(30));
        assert_eq!(chain.versions()[2].end_ts, None);
    }
}
