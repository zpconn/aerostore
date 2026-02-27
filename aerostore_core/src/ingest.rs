use std::fmt;
use std::hash::Hash;
use std::io;
use std::str;
use std::time::Instant;

use memchr::memchr;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::mvcc::MvccError;
use crate::wal::DurableDatabase;

#[derive(Debug, Clone)]
pub struct TsvDecodeError {
    pub column: usize,
    pub message: &'static str,
}

impl TsvDecodeError {
    pub fn new(column: usize, message: &'static str) -> Self {
        Self { column, message }
    }
}

pub trait TsvDecoder<K, V>: Send + Sync {
    fn decode(&self, cols: &mut TsvColumns<'_>) -> Result<(K, V), TsvDecodeError>;
}

pub struct TsvColumns<'a> {
    line: &'a [u8],
    cursor: usize,
    column_idx: usize,
}

impl<'a> TsvColumns<'a> {
    pub fn new(line: &'a [u8]) -> Self {
        Self {
            line,
            cursor: 0,
            column_idx: 0,
        }
    }

    pub fn next_bytes(&mut self) -> Option<&'a [u8]> {
        if self.cursor > self.line.len() {
            return None;
        }

        let start = self.cursor;
        let rel = memchr(b'\t', &self.line[start..]);
        let end = rel.map(|idx| start + idx).unwrap_or(self.line.len());

        self.cursor = if rel.is_some() {
            end + 1
        } else {
            self.line.len() + 1
        };
        self.column_idx += 1;
        Some(&self.line[start..end])
    }

    pub fn expect_bytes(&mut self) -> Result<&'a [u8], TsvDecodeError> {
        self.next_bytes()
            .ok_or_else(|| TsvDecodeError::new(self.column_idx + 1, "missing column"))
    }

    pub fn expect_str(&mut self) -> Result<&'a str, TsvDecodeError> {
        let column = self.column_idx + 1;
        let bytes = self.expect_bytes()?;
        str::from_utf8(bytes).map_err(|_| TsvDecodeError::new(column, "invalid utf8"))
    }

    pub fn expect_i32(&mut self) -> Result<i32, TsvDecodeError> {
        let column = self.column_idx + 1;
        self.expect_str()?
            .parse::<i32>()
            .map_err(|_| TsvDecodeError::new(column, "invalid i32"))
    }

    pub fn expect_u16(&mut self) -> Result<u16, TsvDecodeError> {
        let column = self.column_idx + 1;
        self.expect_str()?
            .parse::<u16>()
            .map_err(|_| TsvDecodeError::new(column, "invalid u16"))
    }

    pub fn expect_u64(&mut self) -> Result<u64, TsvDecodeError> {
        let column = self.column_idx + 1;
        self.expect_str()?
            .parse::<u64>()
            .map_err(|_| TsvDecodeError::new(column, "invalid u64"))
    }

    pub fn expect_f64(&mut self) -> Result<f64, TsvDecodeError> {
        let column = self.column_idx + 1;
        self.expect_str()?
            .parse::<f64>()
            .map_err(|_| TsvDecodeError::new(column, "invalid f64"))
    }

    pub fn ensure_end(&self) -> Result<(), TsvDecodeError> {
        if self.cursor > self.line.len() {
            Ok(())
        } else {
            Err(TsvDecodeError::new(
                self.column_idx + 1,
                "unexpected extra columns",
            ))
        }
    }
}

#[derive(Debug)]
pub enum IngestError {
    EmptyBatchSize,
    Decode {
        line: usize,
        column: usize,
        message: &'static str,
    },
    Mvcc(MvccError),
    Io(io::Error),
}

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IngestError::EmptyBatchSize => write!(f, "batch size must be > 0"),
            IngestError::Decode {
                line,
                column,
                message,
            } => write!(
                f,
                "decode error at line {}, column {}: {}",
                line, column, message
            ),
            IngestError::Mvcc(err) => write!(f, "mvcc error: {:?}", err),
            IngestError::Io(err) => write!(f, "io error: {}", err),
        }
    }
}

impl std::error::Error for IngestError {}

impl From<MvccError> for IngestError {
    fn from(value: MvccError) -> Self {
        IngestError::Mvcc(value)
    }
}

impl From<io::Error> for IngestError {
    fn from(value: io::Error) -> Self {
        IngestError::Io(value)
    }
}

#[derive(Clone, Debug, Default)]
pub struct IngestStats {
    pub rows_seen: usize,
    pub rows_inserted: usize,
    pub rows_updated: usize,
    pub batches_committed: usize,
    pub elapsed_ms: f64,
}

impl IngestStats {
    pub fn metrics_line(&self) -> String {
        let mut out = String::with_capacity(120);

        let mut ibuf = itoa::Buffer::new();
        out.push_str("rows_seen=");
        out.push_str(ibuf.format(self.rows_seen));
        out.push(' ');

        let mut ibuf = itoa::Buffer::new();
        out.push_str("inserted=");
        out.push_str(ibuf.format(self.rows_inserted));
        out.push(' ');

        let mut ibuf = itoa::Buffer::new();
        out.push_str("updated=");
        out.push_str(ibuf.format(self.rows_updated));
        out.push(' ');

        let mut ibuf = itoa::Buffer::new();
        out.push_str("batches=");
        out.push_str(ibuf.format(self.batches_committed));
        out.push(' ');

        out.push_str("elapsed_ms=");
        let mut fbuf = ryu::Buffer::new();
        out.push_str(fbuf.format(self.elapsed_ms));

        out
    }
}

pub async fn bulk_upsert_tsv<K, V, D>(
    db: &DurableDatabase<K, V>,
    input: &[u8],
    batch_size: usize,
    decoder: &D,
) -> Result<IngestStats, IngestError>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + Serialize + DeserializeOwned + 'static,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    D: TsvDecoder<K, V>,
{
    if batch_size == 0 {
        return Err(IngestError::EmptyBatchSize);
    }

    let started = Instant::now();
    let mut stats = IngestStats::default();
    let mut batch_ops = 0_usize;
    let mut line_no = 0_usize;
    let mut cursor = 0_usize;
    let mut tx = db.begin();

    while cursor < input.len() {
        let rel_newline = memchr(b'\n', &input[cursor..]);
        let line_end = rel_newline.map(|idx| cursor + idx).unwrap_or(input.len());
        let mut line = &input[cursor..line_end];
        cursor = rel_newline.map(|_| line_end + 1).unwrap_or(input.len());
        line_no += 1;

        if matches!(line.last(), Some(b'\r')) {
            line = &line[..line.len().saturating_sub(1)];
        }

        if line.is_empty() {
            continue;
        }

        let mut cols = TsvColumns::new(line);
        let (key, value) = decoder
            .decode(&mut cols)
            .map_err(|err| IngestError::Decode {
                line: line_no,
                column: err.column,
                message: err.message,
            })?;
        cols.ensure_end().map_err(|err| IngestError::Decode {
            line: line_no,
            column: err.column,
            message: err.message,
        })?;

        stats.rows_seen += 1;
        if db.read_visible(&key, &tx).is_some() {
            db.update(&mut tx, &key, value)?;
            stats.rows_updated += 1;
        } else {
            db.insert(&mut tx, key, value)?;
            stats.rows_inserted += 1;
        }
        batch_ops += 1;

        if batch_ops >= batch_size {
            db.commit(tx).await?;
            stats.batches_committed += 1;
            tx = db.begin();
            batch_ops = 0;
        }
    }

    if batch_ops > 0 {
        db.commit(tx).await?;
        stats.batches_committed += 1;
    } else {
        db.abort(tx);
    }

    stats.elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(stats)
}
