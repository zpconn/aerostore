//! Contention-owned fixture for a controlled expiry-index experiment.
//! The original Extended Crucible and production engine are unchanged. Default
//! row/index construction preserves the original five extractors. Every mapping
//! carries an immutable fixture identity so attachments cannot silently choose a
//! different extractor from the owner. This is benchmark metadata, not recovery.
use crate::extended_crucible::model::{Record, DEDUP, FLIGHT, OUTBOX, POSITION, SCHEDULED};
use aerostore_core::occ_partitioned::{OccCommitRecord, OccCommittedWrite};
use aerostore_core::{
    map_tmpfs_shared, serialize_commit_record, wal_commit_from_occ_record_with_policy, IndexValue,
    OccTable, RelPtr, SecondaryIndex, ShmArena, TmpfsAttachMode, WalEncodingPolicy,
};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub use crate::extended_crucible::aerostore::{Ring, WAL_SLOTS, WAL_SLOT_BYTES};
const INDEX_NAMES: [&str; 5] = ["callsign", "tail", "family", "due", "event_time"];
const INDEX_KEYS: [fn(&Record) -> Option<IndexValue>; 5] = [
    |row| keys(row)[0].map(IndexValue::I64),
    |row| keys(row)[1].map(IndexValue::I64),
    |row| keys(row)[2].map(IndexValue::I64),
    |row| keys(row)[3].map(IndexValue::I64),
    |row| keys(row)[4].map(IndexValue::I64),
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExpiryIndexPolicy {
    #[default]
    AllActive,
    Housekeeping,
}
impl ExpiryIndexPolicy {
    pub fn name(self) -> &'static str {
        match self {
            Self::AllActive => "all-active",
            Self::Housekeeping => "housekeeping",
        }
    }
    fn code(self) -> u32 {
        match self {
            Self::AllActive => 1,
            Self::Housekeeping => 2,
        }
    }
    fn keys(self, row: &Record) -> [Option<i64>; 5] {
        let mut values = keys(row);
        if self == Self::Housekeeping && !matches!(row.kind, POSITION | OUTBOX | DEDUP) {
            values[4] = None;
        }
        values
    }
    fn extractors(self) -> [fn(&Record) -> Option<IndexValue>; 5] {
        let mut functions = INDEX_KEYS;
        if self == Self::Housekeeping {
            functions[4] = |row| Self::Housekeeping.keys(row)[4].map(IndexValue::I64);
        }
        functions
    }
}

/// All fields have valid arbitrary bit patterns. RelPtr validates bounds and
/// alignment before this immutable bootstrap record is inspected. Index names
/// are local labels, so they cannot identify an owner's shared extractor policy.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FixtureIdentity {
    magic: u64,
    version: u32,
    expiry_policy: u32,
    table_header: u32,
    indexes: [u32; 5],
    ring: u32,
    table_slots_offset: u32,
    table_slots_count: u32,
}
impl FixtureIdentity {
    fn expected(attachment: &Attachment, table_slots_offset: u32) -> Result<Self, String> {
        Ok(Self {
            magic: 0x4846_4558_5049_5831, // HFEXPIX1
            version: 1,
            expiry_policy: attachment.expiry_index_policy.code(),
            table_header: attachment.table_header,
            indexes: attachment
                .indexes
                .as_slice()
                .try_into()
                .map_err(|_| "invalid contention index attachment")?,
            ring: attachment.ring,
            table_slots_offset,
            table_slots_count: attachment
                .table_slots
                .len()
                .try_into()
                .map_err(|_| "too many contention table slots")?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attachment {
    pub path: PathBuf,
    pub bytes: usize,
    pub table_header: u32,
    pub table_slots: Vec<u32>,
    pub indexes: Vec<u32>,
    pub ring: u32,
    #[serde(default)]
    pub expiry_index_policy: ExpiryIndexPolicy,
}

pub struct Shared {
    pub arena: Arc<ShmArena>,
    pub table: Arc<OccTable<Record>>,
    pub indexes: Vec<SecondaryIndex<usize>>,
    pub ring: Ring,
    pub expiry_index_policy: ExpiryIndexPolicy,
}

fn keys(row: &Record) -> [Option<i64>; 5] {
    if !row.active {
        return [None; 5];
    }
    [
        (row.kind == FLIGHT).then_some(row.callsign),
        (row.kind == FLIGHT && row.tail != 0).then_some(row.tail),
        Some(row.family),
        (row.kind == SCHEDULED).then_some(row.due),
        Some(row.event_time),
    ]
}

fn maximum_wal_record_bytes() -> Result<usize, String> {
    // Record contains only fixed-width scalar values. Use the longest possible
    // row-id string and force every row to full encoding, which stores its value
    // in both value_payload and wal_record_payload. Let the actual codecs account
    // for framing, duplicate payloads, alignment and future encoding changes.
    let record = OccCommitRecord {
        txid: u64::MAX,
        writes: (0..crate::extended_crucible::model::SLOTS_PER_FAMILY)
            .map(|offset| {
                let row_id = usize::MAX - offset;
                OccCommittedWrite {
                    row_id,
                    base_offset: u32::MAX,
                    new_offset: u32::MAX,
                    base_value: Record::default(),
                    value: Record {
                        id: row_id,
                        ..Record::default()
                    },
                    dirty_columns_bitmask: u64::MAX,
                }
            })
            .collect(),
    };
    let encoded = wal_commit_from_occ_record_with_policy(&record, |_| WalEncodingPolicy::ForceFull)
        .map_err(|error| error.to_string())?;
    Ok(serialize_commit_record(&encoded)
        .map_err(|error| error.to_string())?
        .len())
}

fn immutable_arena_prefix(arena: &ShmArena) -> [u8; 16] {
    // ShmHeader is repr(C); its first four u32 values are immutable magic,
    // layout version, capacity and data_start. Together they are exactly the
    // fields checked by ShmArena::is_header_valid. Obtain current values from a
    // freshly initialized engine arena, rather than duplicating private constants.
    let mut prefix = [0; 16];
    // SAFETY: ShmArena validates a mapping larger than its complete initialized
    // header. These first 16 bytes contain no concurrently mutable atomic fields.
    unsafe {
        std::ptr::copy_nonoverlapping(
            arena.mmap_base().as_ptr(),
            prefix.as_mut_ptr(),
            prefix.len(),
        );
    }
    prefix
}

fn attach_existing_arena(path: &Path, bytes: usize) -> Result<ShmArena, String> {
    // Do not let map_tmpfs_shared create, resize, or reinitialize a damaged
    // attachment. Opening an existing inode first also pins the file across a
    // rename/unlink: the mapper opens this exact descriptor through procfs.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("open existing arena {}: {error}", path.display()))?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.len() != bytes as u64 {
        return Err(format!("arena attachment requires an existing regular file of exactly {bytes} bytes; observed {}", metadata.len()));
    }
    let expected = {
        let template = ShmArena::new(bytes).map_err(|error| error.to_string())?;
        immutable_arena_prefix(&template)
    };
    let mut observed = [0; 16];
    file.read_exact(&mut observed)
        .map_err(|error| error.to_string())?;
    if observed != expected {
        return Err(
            "arena attachment header is invalid or incompatible; refusing to reinitialize it"
                .into(),
        );
    }
    let descriptor_path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
    let mapped = map_tmpfs_shared(&descriptor_path, bytes).map_err(|error| error.to_string())?;
    if mapped.mode != TmpfsAttachMode::WarmStart || !mapped.arena.is_header_valid() {
        return Err("arena attachment did not preserve a valid warm mapping".into());
    }
    Ok(mapped.arena)
}

impl Shared {
    pub fn create(path: &Path, bytes: usize, records: &[Record]) -> Result<Self, String> {
        Self::create_with_policy(path, bytes, records, ExpiryIndexPolicy::AllActive)
    }

    pub fn create_with_policy(
        path: &Path,
        bytes: usize,
        records: &[Record],
        expiry_index_policy: ExpiryIndexPolicy,
    ) -> Result<Self, String> {
        // Reject oversized fixture transactions before any rows can commit.
        let maximum = maximum_wal_record_bytes()?;
        if maximum > WAL_SLOT_BYTES {
            return Err(format!("fixture's maximum encoded transaction is {maximum} bytes, exceeding {WAL_SLOT_BYTES}-byte WAL slots"));
        }
        let arena = Arc::new(
            map_tmpfs_shared(path, bytes)
                .map_err(|e| e.to_string())?
                .arena,
        );
        if arena.boot_layout_offset() != 0 {
            return Err("contention fixture creation requires an unused boot identity".into());
        }
        let mut table =
            OccTable::new(Arc::clone(&arena), records.len()).map_err(|e| e.to_string())?;
        let indexes = INDEX_NAMES
            .iter()
            .map(|name| SecondaryIndex::new_in_shared(name, Arc::clone(&arena)))
            .collect::<Vec<_>>();
        for row in records {
            table.seed_row(row.id, *row).map_err(|e| e.to_string())?;
            for (index, key) in indexes.iter().zip(expiry_index_policy.keys(row)) {
                if let Some(key) = key {
                    index
                        .try_insert(IndexValue::I64(key), row.id)
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        for (index, key) in indexes.iter().zip(expiry_index_policy.extractors()) {
            table
                .bind_index(index.clone(), key)
                .map_err(|e| e.to_string())?;
        }
        let table = Arc::new(table);
        let ring = Ring::create(Arc::clone(&arena)).map_err(|e| e.to_string())?;
        let shared = Self {
            arena,
            table,
            indexes,
            ring,
            expiry_index_policy,
        };
        let attachment = shared.attachment(path);
        let slot_bytes = attachment
            .table_slots
            .len()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or("contention slot identity size overflow")?;
        let slots_offset = shared
            .arena
            .chunked_arena()
            .alloc_raw(slot_bytes, std::mem::align_of::<u32>())
            .map_err(|e| e.to_string())?;
        // SAFETY: alloc_raw reserved this unique, correctly aligned range for
        // exactly table_slots.len() u32 values. No pointer is published until
        // after initialization, and these offsets are never modified afterward.
        unsafe {
            std::ptr::copy_nonoverlapping(
                attachment.table_slots.as_ptr(),
                shared
                    .arena
                    .mmap_base()
                    .as_ptr()
                    .add(slots_offset as usize)
                    .cast::<u32>(),
                attachment.table_slots.len(),
            );
        }
        let identity = FixtureIdentity::expected(&attachment, slots_offset)?;
        let marker = shared
            .arena
            .chunked_arena()
            .alloc(identity)
            .map_err(|e| e.to_string())?;
        // Only this quiescent owner publishes the marker. Attached processes
        // acquire its offset before inspecting identity or binding extractors.
        shared
            .arena
            .set_boot_layout_offset(marker.load(Ordering::Acquire));
        Ok(shared)
    }

    pub fn attachment(&self, path: &Path) -> Attachment {
        Attachment {
            path: path.to_path_buf(),
            bytes: self.arena.len(),
            table_header: self.table.shared_header_offset(),
            table_slots: self.table.index_slot_offsets(),
            indexes: self
                .indexes
                .iter()
                .map(SecondaryIndex::header_offset)
                .collect(),
            ring: self.ring.ring_ptr().load(Ordering::Acquire),
            expiry_index_policy: self.expiry_index_policy,
        }
    }

    pub fn attach(a: &Attachment) -> Result<Self, String> {
        if a.indexes.len() != INDEX_NAMES.len() {
            return Err("invalid contention index attachment".into());
        }
        let arena = Arc::new(attach_existing_arena(&a.path, a.bytes)?);
        let identity = RelPtr::<FixtureIdentity>::from_offset(arena.boot_layout_offset());
        let identity = identity
            .as_ref(arena.mmap_base())
            .ok_or("missing or invalid contention fixture identity")?;
        let expected = FixtureIdentity::expected(a, identity.table_slots_offset)?;
        if identity != &expected {
            return Err(
                "contention fixture identity differs from attachment policy or offsets".into(),
            );
        }
        for (position, expected) in a.table_slots.iter().enumerate() {
            let offset = u32::try_from(position)
                .ok()
                .and_then(|position| position.checked_mul(4))
                .and_then(|offset| identity.table_slots_offset.checked_add(offset))
                .ok_or("contention slot identity offset overflow")?;
            if RelPtr::<u32>::from_offset(offset).as_ref(arena.mmap_base()) != Some(expected) {
                return Err("contention table slots differ from immutable fixture identity".into());
            }
        }
        let mut table =
            OccTable::from_existing(Arc::clone(&arena), a.table_header, a.table_slots.clone())
                .map_err(|e| e.to_string())?;
        let indexes = INDEX_NAMES
            .iter()
            .zip(&a.indexes)
            .map(|(name, offset)| {
                SecondaryIndex::from_existing(name, Arc::clone(&arena), *offset)
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, key) in indexes.iter().zip(a.expiry_index_policy.extractors()) {
            table
                .bind_index(index.clone(), key)
                .map_err(|e| e.to_string())?;
        }
        let table = Arc::new(table);
        let ring = Ring::from_existing(Arc::clone(&arena), RelPtr::from_offset(a.ring));
        Ok(Self {
            arena,
            table,
            indexes,
            ring,
            expiry_index_policy: a.expiry_index_policy,
        })
    }

    pub fn snapshot(&self) -> Result<Vec<Record>, String> {
        let mut rows = self
            .table
            .snapshot_latest_rows()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(_, r)| r)
            .collect::<Vec<_>>();
        rows.sort_by_key(|r| r.id);
        Ok(rows)
    }

    /// Called only while every worker is at a phase barrier or has exited.
    pub fn audit(&self) -> Result<serde_json::Value, String> {
        let rows = self.snapshot()?;
        let mut audits = Vec::new();
        for (number, index) in self.indexes.iter().enumerate() {
            let mut expected = rows
                .iter()
                .filter_map(|r| {
                    self.expiry_index_policy.keys(r)[number].map(|k| (IndexValue::I64(k), r.id))
                })
                .collect::<Vec<_>>();
            expected.sort();
            let mut actual = index.try_entries().map_err(|e| e.to_string())?;
            actual.sort();
            if actual != expected {
                return Err(format!(
                    "{} index disagrees with table: expected {} postings, observed {}",
                    INDEX_NAMES[number],
                    expected.len(),
                    actual.len()
                ));
            }
            for _ in 0..16 {
                index.collect_garbage_once(usize::MAX);
            }
            let telemetry = index.mutation_telemetry();
            if telemetry.retired_backlog != 0
                || telemetry.retired_postings != 0
                || telemetry.gc_recycle_errors != 0
            {
                return Err(format!(
                    "{} reclamation did not drain cleanly",
                    INDEX_NAMES[number]
                ));
            }
            let audit = index
                .audit_allocations()
                .map_err(|e| format!("{} allocation ownership: {e}", INDEX_NAMES[number]))?;
            audits.push(serde_json::json!({"index":INDEX_NAMES[number],"postings":actual.len(),"ownership":format!("{audit:?}"),"retired_postings":telemetry.retired_postings}));
        }
        Ok(
            serde_json::json!({"indexes":audits,"arena_high_water_bytes":self.arena.chunked_arena().head_offset(),"expiry_index_policy":self.expiry_index_policy}),
        )
    }
}
