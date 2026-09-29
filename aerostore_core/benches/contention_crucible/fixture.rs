//! Contention-owned fixture for explicit expiry and due-index experiments.
//! The original Extended Crucible retains its own fixture. Default row/index
//! construction preserves the original five extractors. Every mapping
//! carries an immutable fixture identity so attachments cannot silently choose a
//! different extractor from the owner. This is benchmark metadata, not recovery.
use crate::extended_crucible::model::{Record, DEDUP, FLIGHT, OUTBOX, POSITION, SCHEDULED};
use aerostore_core::occ_partitioned::{OccCommitRecord, OccCommittedWrite};
use aerostore_core::{
    map_tmpfs_shared, serialize_commit_record, wal_commit_from_occ_record_with_policy,
    IndexPublicationPolicy, IndexValue, OccTable, RelPtr, SecondaryIndex, ShmArena,
    TmpfsAttachMode, WalEncodingPolicy,
};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::OpenOptionsExt;
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

// Only the arena moves; WAL and evidence paths remain in the case directory.
pub const ARENA_BACKING_ENV: &str = "AEROSTORE_CONTENTION_ARENA_BACKING";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ArenaBacking {
    #[default]
    File,
    Memfd,
}

impl ArenaBacking {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "file" => Ok(Self::File),
            "memfd" => Ok(Self::Memfd),
            _ => Err("--arena-backing must be file or memfd".into()),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Memfd => "memfd",
        }
    }

    fn lifetime(self) -> &'static str {
        match self {
            Self::File => "path_survives_owner_until_unlinked",
            Self::Memfd => "new_attachments_require_live_owner_existing_mappings_survive",
        }
    }
}

pub fn reject_legacy_arena_environment(value: Option<&OsStr>) -> Result<(), String> {
    if value.is_some() {
        Err(format!("{ARENA_BACKING_ENV} is no longer supported; unset it and use --arena-backing file|memfd"))
    } else {
        Ok(())
    }
}

/// Observations are obtained from the descriptor used to map the arena, before
/// admission. A request alone never counts as an observed storage placement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArenaBackingMetadata {
    pub format: String,
    pub requested: ArenaBacking,
    pub effective: String,
    pub observed: bool,
    pub filesystem_type: Option<String>,
    pub filesystem_magic: Option<String>,
    pub lifetime: Option<String>,
    pub wal_placement: String,
}

impl ArenaBackingMetadata {
    pub fn pending(requested: ArenaBacking, postgres: bool) -> Self {
        Self {
            format: "arena-backing-v1".into(),
            requested,
            effective: if postgres {
                "not_applicable"
            } else {
                "unobserved"
            }
            .into(),
            observed: false,
            filesystem_type: None,
            filesystem_magic: None,
            lifetime: None,
            wal_placement: if postgres {
                "postgres_managed"
            } else {
                "case_directory_file"
            }
            .into(),
        }
    }

    fn observe(file: &File, requested: ArenaBacking) -> Result<Self, String> {
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: fstatfs initializes stat on success; file owns a live descriptor.
        if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(format!(
                "observe arena filesystem: {}",
                std::io::Error::last_os_error()
            ));
        }
        let magic = unsafe { stat.assume_init() }.f_type as u64;
        let target = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
            .map_err(|error| format!("observe arena descriptor: {error}"))?;
        let memfd = target
            .as_os_str()
            .as_encoded_bytes()
            .starts_with(b"/memfd:");
        if memfd != (requested == ArenaBacking::Memfd)
            || (memfd && magic != libc::TMPFS_MAGIC as u64)
        {
            return Err("arena descriptor backing differs from requested backing".into());
        }
        let result = Self {
            format: "arena-backing-v1".into(),
            requested,
            effective: requested.name().into(),
            observed: true,
            filesystem_type: Some(
                match magic {
                    0x0102_1994 => "tmpfs",
                    0xef53 => "ext2/ext3/ext4",
                    _ => "other",
                }
                .into(),
            ),
            filesystem_magic: Some(format!("0x{magic:x}")),
            lifetime: Some(requested.lifetime().into()),
            wal_placement: "case_directory_file".into(),
        };
        result.validate_native(requested)?;
        Ok(result)
    }

    pub fn validate_native(&self, requested: ArenaBacking) -> Result<(), String> {
        let magic = self
            .filesystem_magic
            .as_deref()
            .and_then(|value| value.strip_prefix("0x"))
            .and_then(|value| u64::from_str_radix(value, 16).ok());
        let canonical_magic = magic.map(|magic| format!("0x{magic:x}"));
        let expected_type = magic.map(|magic| match magic {
            0x0102_1994 => "tmpfs",
            0xef53 => "ext2/ext3/ext4",
            _ => "other",
        });
        if self.format != "arena-backing-v1"
            || self.requested != requested
            || self.effective != requested.name()
            || !self.observed
            || magic.is_none()
            || canonical_magic != self.filesystem_magic
            || self.filesystem_type.as_deref() != expected_type
            || self.lifetime.as_deref() != Some(requested.lifetime())
            || self.wal_placement != "case_directory_file"
            || (requested == ArenaBacking::Memfd && magic != Some(libc::TMPFS_MAGIC as u64))
        {
            return Err("arena backing metadata is absent, unobserved, or inconsistent with requested backing".into());
        }
        Ok(())
    }
}

/// Keep the memfd reopenable through the fixture's existing attachment path.
/// Existing mappings pin its pages independently; a new attachment requires
/// this owner to remain alive. Unlike a named tmpfs arena, a killed process
/// leaves no kernel storage after the last mapping/inherited descriptor closes.
/// A SIGKILL can leave a dangling symlink in the retained case directory.
struct MemfdArenaOwner {
    _file: File,
    path: PathBuf,
    target: PathBuf,
}

impl MemfdArenaOwner {
    fn create(path: &Path) -> Result<Self, String> {
        // SAFETY: the static name is NUL-terminated. On success the descriptor
        // is newly owned here and transferred exactly once into File.
        let fd = unsafe {
            libc::memfd_create(
                b"aerostore-contention-arena\0".as_ptr().cast(),
                libc::MFD_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(format!(
                "create benchmark memfd: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: memfd_create returned a new owned descriptor above.
        let file = unsafe { File::from_raw_fd(fd) };
        let target = PathBuf::from(format!("/proc/{}/fd/{fd}", std::process::id()));
        // symlink fails if any file or symlink already occupies this name. Never
        // truncate or replace a prior arena, even when that symlink is dangling.
        std::os::unix::fs::symlink(&target, path)
            .map_err(|error| format!("create benchmark arena link {}: {error}", path.display()))?;
        Ok(Self {
            _file: file,
            path: path.to_path_buf(),
            target,
        })
    }
}

impl Drop for MemfdArenaOwner {
    fn drop(&mut self) {
        // The runner also unlinks its disposable arena after auditing. Do not
        // delete an unrelated replacement or report a missing link as an error.
        if std::fs::read_link(&self.path).ok().as_ref() == Some(&self.target) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

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

pub fn default_due_index_origin() -> i64 {
    // Calibrated messages express this epoch in nanoseconds. Other workloads
    // may explicitly select parameters in their own event-time units.
    1_700_000_000_000_000_000
}

pub fn default_due_index_width() -> u64 {
    1_000_000_000
}

pub fn default_expiry_index_origin() -> i64 {
    default_due_index_origin()
}

pub fn default_expiry_index_width() -> u64 {
    default_due_index_width()
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExpiryPublicationPolicy {
    #[default]
    Hashed,
    Ordered,
}

impl ExpiryPublicationPolicy {
    pub fn name(self) -> &'static str {
        match self {
            Self::Hashed => "hashed",
            Self::Ordered => "ordered",
        }
    }

    fn code(self) -> u32 {
        match self {
            Self::Hashed => 1,
            Self::Ordered => 2,
        }
    }

    fn publication_policy(self, origin: i64, width: u64) -> Result<IndexPublicationPolicy, String> {
        if width == 0 {
            return Err("expiry index width must be positive".into());
        }
        Ok(match self {
            Self::Hashed => IndexPublicationPolicy::Hashed,
            Self::Ordered => IndexPublicationPolicy::OrderedI64 { origin, width },
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DueIndexPolicy {
    #[default]
    Hashed,
    Ordered,
}

impl DueIndexPolicy {
    pub fn name(self) -> &'static str {
        match self {
            Self::Hashed => "hashed",
            Self::Ordered => "ordered",
        }
    }

    fn code(self) -> u32 {
        match self {
            Self::Hashed => 1,
            Self::Ordered => 2,
        }
    }

    fn publication_policy(self, origin: i64, width: u64) -> Result<IndexPublicationPolicy, String> {
        if width == 0 {
            return Err("due index width must be positive".into());
        }
        Ok(match self {
            Self::Hashed => IndexPublicationPolicy::Hashed,
            Self::Ordered => IndexPublicationPolicy::OrderedI64 { origin, width },
        })
    }
}

/// All fields have valid arbitrary bit patterns. RelPtr validates bounds and
/// alignment before this immutable bootstrap record is inspected. Index names
/// are local labels, so they cannot identify an owner's shared extractor policy.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FixtureIdentity {
    magic: u64,
    due_index_origin: i64,
    due_index_width: u64,
    version: u32,
    expiry_policy: u32,
    due_policy: u32,
    table_header: u32,
    indexes: [u32; 5],
    ring: u32,
    table_slots_offset: u32,
    table_slots_count: u32,
    expiry_publication: u32,
    expiry_index_origin: i64,
    expiry_index_width: u64,
}

/// The v2/v3 prefix is stable. Reject an older marker before interpreting the
/// larger v3 identity. No fixture upgrade or production recovery is attempted.
#[repr(C)]
struct FixtureIdentityPrefix {
    magic: u64,
    due_index_origin: i64,
    due_index_width: u64,
    version: u32,
}

impl FixtureIdentity {
    fn expected(attachment: &Attachment, table_slots_offset: u32) -> Result<Self, String> {
        Ok(Self {
            magic: 0x4846_4558_5049_5831, // HFEXPIX1
            due_index_origin: attachment.due_index_origin,
            due_index_width: attachment.due_index_width,
            version: 3,
            expiry_policy: attachment.expiry_index_policy.code(),
            due_policy: attachment.due_index_policy.code(),
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
            expiry_publication: attachment.expiry_publication_policy.code(),
            expiry_index_origin: attachment.expiry_index_origin,
            expiry_index_width: attachment.expiry_index_width,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attachment {
    #[serde(default)]
    pub arena_backing: ArenaBacking,
    pub path: PathBuf,
    pub bytes: usize,
    pub table_header: u32,
    pub table_slots: Vec<u32>,
    pub indexes: Vec<u32>,
    pub ring: u32,
    #[serde(default)]
    pub expiry_index_policy: ExpiryIndexPolicy,
    #[serde(default)]
    pub due_index_policy: DueIndexPolicy,
    #[serde(default = "default_due_index_origin")]
    pub due_index_origin: i64,
    #[serde(default = "default_due_index_width")]
    pub due_index_width: u64,
    #[serde(default)]
    pub expiry_publication_policy: ExpiryPublicationPolicy,
    #[serde(default = "default_expiry_index_origin")]
    pub expiry_index_origin: i64,
    #[serde(default = "default_expiry_index_width")]
    pub expiry_index_width: u64,
}

pub struct Shared {
    pub arena_backing_metadata: ArenaBackingMetadata,
    pub arena: Arc<ShmArena>,
    pub table: Arc<OccTable<Record>>,
    pub indexes: Vec<SecondaryIndex<usize>>,
    pub ring: Ring,
    pub expiry_index_policy: ExpiryIndexPolicy,
    pub due_index_policy: DueIndexPolicy,
    pub due_index_origin: i64,
    pub due_index_width: u64,
    pub expiry_publication_policy: ExpiryPublicationPolicy,
    pub expiry_index_origin: i64,
    pub expiry_index_width: u64,
    _arena_owner: Option<MemfdArenaOwner>,
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

fn attach_existing_arena(
    path: &Path,
    bytes: usize,
    backing: ArenaBacking,
) -> Result<(ShmArena, ArenaBackingMetadata), String> {
    // Do not let map_tmpfs_shared create, resize, or reinitialize a damaged
    // attachment. Opening an existing inode first also pins the file across a
    // rename/unlink: the mapper opens this exact descriptor through procfs.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("open existing arena {}: {error}", path.display()))?;
    let backing_metadata = ArenaBackingMetadata::observe(&file, backing)?;
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
    Ok((mapped.arena, backing_metadata))
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
        Self::create_with_policies(
            path,
            bytes,
            records,
            expiry_index_policy,
            DueIndexPolicy::Hashed,
            default_due_index_origin(),
            default_due_index_width(),
        )
    }

    pub fn create_with_policies(
        path: &Path,
        bytes: usize,
        records: &[Record],
        expiry_index_policy: ExpiryIndexPolicy,
        due_index_policy: DueIndexPolicy,
        due_index_origin: i64,
        due_index_width: u64,
    ) -> Result<Self, String> {
        Self::create_with_publication_policies(
            path,
            bytes,
            records,
            expiry_index_policy,
            due_index_policy,
            due_index_origin,
            due_index_width,
            ExpiryPublicationPolicy::Hashed,
            default_expiry_index_origin(),
            default_expiry_index_width(),
        )
    }

    pub fn create_with_publication_policies(
        path: &Path,
        bytes: usize,
        records: &[Record],
        expiry_index_policy: ExpiryIndexPolicy,
        due_index_policy: DueIndexPolicy,
        due_index_origin: i64,
        due_index_width: u64,
        expiry_publication_policy: ExpiryPublicationPolicy,
        expiry_index_origin: i64,
        expiry_index_width: u64,
    ) -> Result<Self, String> {
        Self::create_with_arena_backing(
            path,
            bytes,
            records,
            expiry_index_policy,
            due_index_policy,
            due_index_origin,
            due_index_width,
            expiry_publication_policy,
            expiry_index_origin,
            expiry_index_width,
            ArenaBacking::File,
        )
    }

    pub fn create_with_arena_backing(
        path: &Path,
        bytes: usize,
        records: &[Record],
        expiry_index_policy: ExpiryIndexPolicy,
        due_index_policy: DueIndexPolicy,
        due_index_origin: i64,
        due_index_width: u64,
        expiry_publication_policy: ExpiryPublicationPolicy,
        expiry_index_origin: i64,
        expiry_index_width: u64,
        backing: ArenaBacking,
    ) -> Result<Self, String> {
        reject_legacy_arena_environment(std::env::var_os(ARENA_BACKING_ENV).as_deref())?;
        let due_publication =
            due_index_policy.publication_policy(due_index_origin, due_index_width)?;
        let expiry_publication = expiry_publication_policy
            .publication_policy(expiry_index_origin, expiry_index_width)?;
        // Reject oversized fixture transactions before any rows can commit.
        let maximum = maximum_wal_record_bytes()?;
        if maximum > WAL_SLOT_BYTES {
            return Err(format!("fixture's maximum encoded transaction is {maximum} bytes, exceeding {WAL_SLOT_BYTES}-byte WAL slots"));
        }
        let arena_owner = match backing {
            ArenaBacking::File => None,
            ArenaBacking::Memfd => Some(MemfdArenaOwner::create(path)?),
        };
        // Creation never truncates a preexisting file or follows an unexpected
        // replacement. Pin the descriptor before observing and mapping it.
        let file = match backing {
            ArenaBacking::File => OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path),
            ArenaBacking::Memfd => OpenOptions::new().read(true).write(true).open(path),
        }
        .map_err(|error| format!("create arena {}: {error}", path.display()))?;
        let arena_backing_metadata = ArenaBackingMetadata::observe(&file, backing)?;
        let descriptor_path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
        let arena = Arc::new(
            map_tmpfs_shared(&descriptor_path, bytes)
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
            .enumerate()
            .map(|(number, name)| {
                SecondaryIndex::new_in_shared_with_publication_policy(
                    name,
                    Arc::clone(&arena),
                    if number == 3 {
                        due_publication
                    } else if number == 4 {
                        expiry_publication
                    } else {
                        IndexPublicationPolicy::Hashed
                    },
                )
                .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
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
            arena_backing_metadata,
            arena,
            table,
            indexes,
            ring,
            expiry_index_policy,
            due_index_policy,
            due_index_origin,
            due_index_width,
            expiry_publication_policy,
            expiry_index_origin,
            expiry_index_width,
            _arena_owner: arena_owner,
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
            arena_backing: self.arena_backing_metadata.requested,
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
            due_index_policy: self.due_index_policy,
            due_index_origin: self.due_index_origin,
            due_index_width: self.due_index_width,
            expiry_publication_policy: self.expiry_publication_policy,
            expiry_index_origin: self.expiry_index_origin,
            expiry_index_width: self.expiry_index_width,
        }
    }

    pub fn attach(a: &Attachment) -> Result<Self, String> {
        let due_publication = a
            .due_index_policy
            .publication_policy(a.due_index_origin, a.due_index_width)?;
        let expiry_publication = a
            .expiry_publication_policy
            .publication_policy(a.expiry_index_origin, a.expiry_index_width)?;
        if a.indexes.len() != INDEX_NAMES.len() {
            return Err("invalid contention index attachment".into());
        }
        let (arena, arena_backing_metadata) =
            attach_existing_arena(&a.path, a.bytes, a.arena_backing)?;
        let arena = Arc::new(arena);
        let prefix = RelPtr::<FixtureIdentityPrefix>::from_offset(arena.boot_layout_offset());
        let prefix = prefix
            .as_ref(arena.mmap_base())
            .ok_or("missing or invalid contention fixture identity")?;
        if prefix.magic != 0x4846_4558_5049_5831 || prefix.version != 3 {
            return Err(
                "unsupported contention fixture identity; rebuild the quiescent fixture".into(),
            );
        }
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
        for (number, index) in indexes.iter().enumerate() {
            let expected = if number == 3 {
                due_publication
            } else if number == 4 {
                expiry_publication
            } else {
                IndexPublicationPolicy::Hashed
            };
            if index.publication_policy().map_err(|e| e.to_string())? != expected {
                return Err(
                    "persisted index publication policy differs from contention fixture".into(),
                );
            }
        }
        for (index, key) in indexes.iter().zip(a.expiry_index_policy.extractors()) {
            table
                .bind_index(index.clone(), key)
                .map_err(|e| e.to_string())?;
        }
        let table = Arc::new(table);
        let ring = Ring::from_existing(Arc::clone(&arena), RelPtr::from_offset(a.ring));
        Ok(Self {
            arena_backing_metadata,
            arena,
            table,
            indexes,
            ring,
            expiry_index_policy: a.expiry_index_policy,
            due_index_policy: a.due_index_policy,
            due_index_origin: a.due_index_origin,
            due_index_width: a.due_index_width,
            expiry_publication_policy: a.expiry_publication_policy,
            expiry_index_origin: a.expiry_index_origin,
            expiry_index_width: a.expiry_index_width,
            _arena_owner: None,
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
            let expected_policy = if number == 3 {
                self.due_index_policy
                    .publication_policy(self.due_index_origin, self.due_index_width)?
            } else if number == 4 {
                self.expiry_publication_policy
                    .publication_policy(self.expiry_index_origin, self.expiry_index_width)?
            } else {
                IndexPublicationPolicy::Hashed
            };
            if index.publication_policy().map_err(|e| e.to_string())? != expected_policy {
                return Err("persisted publication policy changed during fixture execution".into());
            }
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
            serde_json::json!({"indexes":audits,"arena_high_water_bytes":self.arena.chunked_arena().head_offset(),"expiry_index_policy":self.expiry_index_policy,"due_index_policy":self.due_index_policy,"due_index_origin":self.due_index_origin,"due_index_width":self.due_index_width,"expiry_publication_policy":self.expiry_publication_policy,"expiry_index_origin":self.expiry_index_origin,"expiry_index_width":self.expiry_index_width}),
        )
    }
}

#[cfg(all(test, target_os = "linux"))]
mod arena_backing_tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    fn create(path: &Path, bytes: usize, backing: ArenaBacking) -> Result<Shared, String> {
        Shared::create_with_arena_backing(
            path,
            bytes,
            &[Record {
                id: 0,
                active: true,
                kind: FLIGHT,
                family: 7,
                ..Record::default()
            }],
            ExpiryIndexPolicy::AllActive,
            DueIndexPolicy::Hashed,
            default_due_index_origin(),
            default_due_index_width(),
            ExpiryPublicationPolicy::Hashed,
            default_expiry_index_origin(),
            default_expiry_index_width(),
            backing,
        )
    }

    #[test]
    fn backing_selection_is_explicit_and_fails_closed() {
        assert_eq!(ArenaBacking::default(), ArenaBacking::File);
        assert_eq!(ArenaBacking::parse("file").unwrap(), ArenaBacking::File);
        assert_eq!(ArenaBacking::parse("memfd").unwrap(), ArenaBacking::Memfd);
        for value in ["", "tmpfs", "MEMFD", " memfd", "memfd "] {
            assert!(ArenaBacking::parse(value).is_err());
        }
        assert!(reject_legacy_arena_environment(None).is_ok());
        for value in ["", "file", "memfd", "unknown"] {
            assert!(reject_legacy_arena_environment(Some(OsStr::new(value)))
                .unwrap_err()
                .contains("unset it and use --arena-backing"));
        }
    }

    #[test]
    fn memfd_is_tmpfs_and_warm_attachments_share_the_owner_mapping() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arena.mmap");
        let shared = create(&path, 16 << 20, ArenaBacking::Memfd).unwrap();
        assert!(std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        let file = File::open(&path).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 16 << 20);
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: fstatfs initializes stat on success and file is a live descriptor.
        assert_eq!(
            unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) },
            0
        );
        assert_eq!(unsafe { stat.assume_init() }.f_type, libc::TMPFS_MAGIC);
        let pointer = shared.arena.chunked_arena().alloc(1234_u64).unwrap();
        let attachment = shared.attachment(&path);
        let attached = Shared::attach(&attachment).unwrap();
        assert_eq!(attached.snapshot().unwrap(), shared.snapshot().unwrap());
        assert_eq!(
            attached.audit().unwrap()["indexes"],
            shared.audit().unwrap()["indexes"]
        );
        let offset = pointer.load(Ordering::Acquire);
        assert_eq!(
            RelPtr::<u64>::from_offset(offset).as_ref(attached.arena.mmap_base()),
            Some(&1234)
        );
        let expected_rows = attached.snapshot().unwrap();
        drop(file);
        drop(shared);
        assert!(std::fs::symlink_metadata(&path).is_err());
        assert!(Shared::attach(&attachment).is_err());
        assert_eq!(attached.snapshot().unwrap(), expected_rows);
        assert_eq!(
            RelPtr::<u64>::from_offset(offset).as_ref(attached.arena.mmap_base()),
            Some(&1234)
        );
    }

    #[test]
    fn file_backing_keeps_existing_path_lifetime() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arena.mmap");
        let shared = create(&path, 16 << 20, ArenaBacking::File).unwrap();
        let metadata = std::fs::symlink_metadata(&path).unwrap();
        assert!(metadata.file_type().is_file());
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let attachment = shared.attachment(&path);
        let rows = shared.snapshot().unwrap();
        drop(shared);
        assert_eq!(
            Shared::attach(&attachment).unwrap().snapshot().unwrap(),
            rows
        );
    }

    #[test]
    fn memfd_creation_preserves_collisions_and_cleans_failed_initialization() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arena.mmap");
        std::fs::write(&path, b"prior arena").unwrap();
        assert!(MemfdArenaOwner::create(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"prior arena");
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink("missing-target", &path).unwrap();
        assert!(MemfdArenaOwner::create(&path).is_err());
        assert_eq!(
            std::fs::read_link(&path).unwrap(),
            Path::new("missing-target")
        );
        std::fs::remove_file(&path).unwrap();
        assert!(create(&path, 0, ArenaBacking::Memfd).is_err());
        assert!(std::fs::symlink_metadata(&path).is_err());
        let owner = MemfdArenaOwner::create(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        drop(owner);
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
    }

    #[test]
    fn arena_backing_metadata_and_attachment_are_observed_and_fail_on_mismatch() {
        let directory = tempfile::tempdir().unwrap();
        for backing in [ArenaBacking::File, ArenaBacking::Memfd] {
            let path = directory.path().join(backing.name());
            let shared = create(&path, 16 << 20, backing).unwrap();
            let metadata = &shared.arena_backing_metadata;
            metadata.validate_native(backing).unwrap();
            assert_eq!(metadata.requested, backing);
            assert_eq!(metadata.effective, backing.name());
            assert!(metadata.observed);
            assert_eq!(metadata.wal_placement, "case_directory_file");
            if backing == ArenaBacking::Memfd {
                assert_eq!(metadata.filesystem_type.as_deref(), Some("tmpfs"));
                assert_eq!(metadata.filesystem_magic.as_deref(), Some("0x1021994"));
            }
            let attachment = shared.attachment(&path);
            let encoded = serde_json::to_vec(&attachment).unwrap();
            let decoded: Attachment = serde_json::from_slice(&encoded).unwrap();
            let attached = Shared::attach(&decoded).unwrap();
            assert_eq!(attached.arena_backing_metadata, *metadata);
            let mut mismatched = attachment;
            mismatched.arena_backing = if backing == ArenaBacking::File {
                ArenaBacking::Memfd
            } else {
                ArenaBacking::File
            };
            assert!(Shared::attach(&mismatched)
                .err()
                .unwrap()
                .contains("backing differs"));
            assert!(metadata.validate_native(mismatched.arena_backing).is_err());
            for field in [
                "format",
                "effective",
                "filesystem_type",
                "filesystem_magic",
                "lifetime",
                "wal_placement",
            ] {
                let mut value = serde_json::to_value(metadata).unwrap();
                value[field] = serde_json::json!("wrong");
                let invalid: ArenaBackingMetadata = serde_json::from_value(value).unwrap();
                assert!(
                    invalid.validate_native(backing).is_err(),
                    "accepted {field}"
                );
            }
            let mut invalid = metadata.clone();
            invalid.observed = false;
            assert!(invalid.validate_native(backing).is_err());
            let mut invalid = metadata.clone();
            invalid.filesystem_magic = None;
            assert!(invalid.validate_native(backing).is_err());
        }
    }

    #[test]
    fn arena_backing_pending_and_postgres_metadata_never_claim_observation() {
        for backing in [ArenaBacking::File, ArenaBacking::Memfd] {
            let pending = ArenaBackingMetadata::pending(backing, false);
            assert_eq!(pending.effective, "unobserved");
            assert!(!pending.observed);
            assert!(pending.filesystem_type.is_none());
            assert!(pending.filesystem_magic.is_none());
            assert!(pending.lifetime.is_none());
            assert!(pending.validate_native(backing).is_err());
            let postgres = ArenaBackingMetadata::pending(backing, true);
            assert_eq!(postgres.requested, backing);
            assert_eq!(postgres.effective, "not_applicable");
            assert_eq!(postgres.wal_placement, "postgres_managed");
            assert!(!postgres.observed);
            assert!(postgres.filesystem_type.is_none());
            assert!(postgres.filesystem_magic.is_none());
            assert!(postgres.lifetime.is_none());
            assert!(postgres.validate_native(backing).is_err());
        }
    }

    #[test]
    fn file_arena_creation_preserves_existing_files_and_symlinks() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arena.mmap");
        std::fs::write(&path, b"prior arena").unwrap();
        assert!(create(&path, 16 << 20, ArenaBacking::File).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"prior arena");
        std::fs::remove_file(&path).unwrap();
        let target = directory.path().join("target");
        std::fs::write(&target, b"unrelated file").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(create(&path, 16 << 20, ArenaBacking::File).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"unrelated file");
        assert_eq!(std::fs::read_link(&path).unwrap(), target);
    }

    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn memfd_owner_death_leaves_existing_mapping_valid_but_no_reopenable_path() {
        const CHILD_DIRECTORY: &str = "AEROSTORE_MEMFD_FIXTURE_TEST_CHILD_DIRECTORY";
        if let Some(directory) = std::env::var_os(CHILD_DIRECTORY) {
            let directory = PathBuf::from(directory);
            let path = directory.join("arena.mmap");
            // Exercise explicit backing in an isolated owner process.
            let shared = create(&path, 16 << 20, ArenaBacking::Memfd).unwrap();
            std::fs::write(
                directory.join("attachment.json"),
                serde_json::to_vec(&shared.attachment(&path)).unwrap(),
            )
            .unwrap();
            std::fs::write(directory.join("ready"), b"ready").unwrap();
            loop {
                std::thread::park();
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let mut child = KillOnDrop(Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &format!("{}::memfd_owner_death_leaves_existing_mapping_valid_but_no_reopenable_path", module_path!().split_once("::").unwrap().1), "--nocapture"])
            .env(CHILD_DIRECTORY, directory.path())
            .stdin(Stdio::null()).stdout(Stdio::null())
            .spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(20);
        while !directory.path().join("ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "fixture child exited before readiness"
            );
            assert!(
                Instant::now() < deadline,
                "fixture child readiness timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let attachment: Attachment = serde_json::from_slice(
            &std::fs::read(directory.path().join("attachment.json")).unwrap(),
        )
        .unwrap();
        let attached = Shared::attach(&attachment).unwrap();
        let rows = attached.snapshot().unwrap();
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert!(std::fs::symlink_metadata(&attachment.path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(!attachment.path.exists());
        assert!(Shared::attach(&attachment).is_err());
        assert_eq!(attached.snapshot().unwrap(), rows);
        attached.audit().unwrap();
    }
}
