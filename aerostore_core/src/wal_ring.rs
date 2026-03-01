use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use rkyv::{
    AlignedVec, Archive, Deserialize as RkyvDeserialize, Infallible, Serialize as RkyvSerialize,
};

use crate::occ::OccCommitRecord;
use crate::shm::{RelPtr, ShmAllocError, ShmArena};
use crate::wal_delta::{
    build_update_record, serialize_wal_record as serialize_delta_wal_record, WalDeltaCodec,
    WalRecord,
};

pub const SYNCHRONOUS_COMMIT_KEY: &str = "aerostore.synchronous_commit";
pub const WAL_RING_COMMIT_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SynchronousCommit {
    On,
    Off,
}

impl SynchronousCommit {
    #[inline]
    pub fn from_setting(value: &str) -> Self {
        if value.eq_ignore_ascii_case("off") {
            Self::Off
        } else {
            Self::On
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalEncodingPolicy {
    DeltaAllowed,
    ForceFull,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct WalRingWrite {
    pub row_id: u64,
    pub base_offset: u32,
    pub new_offset: u32,
    pub value_payload: Vec<u8>,
    pub wal_record_payload: Vec<u8>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct WalRingCommit {
    pub version: u16,
    pub txid: u64,
    pub writes: Vec<WalRingWrite>,
}

pub fn wal_commit_from_occ_record<T>(
    record: &OccCommitRecord<T>,
) -> Result<WalRingCommit, WalRingError>
where
    T: WalDeltaCodec + Copy,
{
    wal_commit_from_occ_record_with_policy(record, |_| WalEncodingPolicy::DeltaAllowed)
}

pub fn wal_commit_from_occ_record_with_policy<T, F>(
    record: &OccCommitRecord<T>,
    mut encoding_for_row: F,
) -> Result<WalRingCommit, WalRingError>
where
    T: WalDeltaCodec + Copy,
    F: FnMut(usize) -> WalEncodingPolicy,
{
    let mut writes = Vec::with_capacity(record.writes.len());
    for w in &record.writes {
        let pk = T::wal_primary_key(w.row_id, &w.value);
        let wal_record = match encoding_for_row(w.row_id) {
            WalEncodingPolicy::DeltaAllowed => {
                build_update_record(pk, &w.base_value, &w.value, w.dirty_columns_bitmask)
                    .map_err(|err| WalRingError::Serialize(err.to_string()))?
            }
            WalEncodingPolicy::ForceFull => WalRecord::UpdateFull {
                pk,
                payload: bincode::serialize(&w.value)
                    .map_err(|err| WalRingError::Serialize(err.to_string()))?,
            },
        };
        let wal_record_payload = serialize_delta_wal_record(&wal_record)
            .map_err(|err| WalRingError::Serialize(err.to_string()))?;
        let value_payload = match wal_record {
            WalRecord::UpdateFull { payload, .. } => payload,
            WalRecord::UpdateDelta { .. } => Vec::new(),
        };
        writes.push(WalRingWrite {
            row_id: w.row_id as u64,
            base_offset: w.base_offset,
            new_offset: w.new_offset,
            value_payload,
            wal_record_payload,
        });
    }

    Ok(WalRingCommit {
        version: WAL_RING_COMMIT_VERSION,
        txid: record.txid,
        writes,
    })
}

#[derive(Debug)]
pub enum WalRingError {
    InvalidConfiguration(&'static str),
    Closed,
    MessageTooLarge { len: usize, slot_bytes: usize },
    CorruptedSlotLength { len: usize, slot_bytes: usize },
    InvalidRingPointer { offset: u32 },
    Allocation(String),
    Serialize(String),
    Deserialize(String),
}

impl fmt::Display for WalRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalRingError::InvalidConfiguration(msg) => {
                write!(f, "invalid wal ring config: {}", msg)
            }
            WalRingError::Closed => write!(f, "wal ring is closed"),
            WalRingError::MessageTooLarge { len, slot_bytes } => write!(
                f,
                "wal message length {} exceeds slot size {}",
                len, slot_bytes
            ),
            WalRingError::CorruptedSlotLength { len, slot_bytes } => write!(
                f,
                "corrupted wal slot length {} exceeds slot size {}",
                len, slot_bytes
            ),
            WalRingError::InvalidRingPointer { offset } => {
                write!(f, "invalid wal ring pointer offset {}", offset)
            }
            WalRingError::Allocation(msg) => write!(f, "wal ring allocation failed: {}", msg),
            WalRingError::Serialize(msg) => write!(f, "wal ring serialize failed: {}", msg),
            WalRingError::Deserialize(msg) => write!(f, "wal ring deserialize failed: {}", msg),
        }
    }
}

impl std::error::Error for WalRingError {}

impl From<ShmAllocError> for WalRingError {
    fn from(value: ShmAllocError) -> Self {
        WalRingError::Allocation(value.to_string())
    }
}

#[repr(C, align(64))]
struct WalRingSlot<const SLOT_BYTES: usize> {
    sequence: AtomicU64,
    len: AtomicU32,
    _pad: AtomicU32,
    data: [u8; SLOT_BYTES],
}

impl<const SLOT_BYTES: usize> WalRingSlot<SLOT_BYTES> {
    #[inline]
    fn new(sequence: u64) -> Self {
        Self {
            sequence: AtomicU64::new(sequence),
            len: AtomicU32::new(0),
            _pad: AtomicU32::new(0),
            data: [0_u8; SLOT_BYTES],
        }
    }
}

#[repr(C)]
pub struct WalRing<const SLOTS: usize, const SLOT_BYTES: usize> {
    head: AtomicU64,
    tail: AtomicU64,
    closed: AtomicU32,
    writer_epoch: AtomicU64,
    slots: [WalRingSlot<SLOT_BYTES>; SLOTS],
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> WalRing<SLOTS, SLOT_BYTES> {
    #[inline]
    pub fn new() -> Self {
        Self {
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            closed: AtomicU32::new(0),
            writer_epoch: AtomicU64::new(0),
            slots: std::array::from_fn(|idx| WalRingSlot::new(idx as u64)),
        }
    }

    #[inline]
    pub fn close(&self) {
        self.closed.store(1, Ordering::Release);
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    #[inline]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch.load(Ordering::Acquire)
    }

    #[inline]
    pub fn bump_writer_epoch(&self) -> u64 {
        self.writer_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1)
    }

    pub fn push_blocking(&self, bytes: &[u8]) -> Result<(), WalRingError> {
        if bytes.len() > SLOT_BYTES {
            return Err(WalRingError::MessageTooLarge {
                len: bytes.len(),
                slot_bytes: SLOT_BYTES,
            });
        }

        let cap = SLOTS as u64;
        let mut spins = 0_u32;

        loop {
            if self.is_closed() {
                return Err(WalRingError::Closed);
            }

            let pos = self.head.load(Ordering::Acquire);
            let slot = &self.slots[(pos % cap) as usize];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as i64 - pos as i64;

            if diff == 0 {
                if self
                    .head
                    .compare_exchange_weak(pos, pos + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    slot.len.store(bytes.len() as u32, Ordering::Relaxed);

                    // SAFETY:
                    // producer owns this slot after winning head CAS for `pos`.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            bytes.as_ptr(),
                            slot.data.as_ptr() as *mut u8,
                            bytes.len(),
                        );
                    }

                    slot.sequence.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
            } else if diff < 0 {
                spins = spins.wrapping_add(1);
                if spins % 1024 == 0 {
                    std::thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
            } else {
                std::hint::spin_loop();
            }
        }
    }

    pub fn pop_into(&self, out: &mut [u8; SLOT_BYTES]) -> Result<Option<usize>, WalRingError> {
        let cap = SLOTS as u64;

        loop {
            let pos = self.tail.load(Ordering::Acquire);
            let slot = &self.slots[(pos % cap) as usize];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as i64 - (pos + 1) as i64;

            if diff == 0 {
                if self
                    .tail
                    .compare_exchange_weak(pos, pos + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let len = slot.len.load(Ordering::Relaxed) as usize;
                    if len > SLOT_BYTES {
                        return Err(WalRingError::CorruptedSlotLength {
                            len,
                            slot_bytes: SLOT_BYTES,
                        });
                    }

                    out[..len].copy_from_slice(&slot.data[..len]);
                    slot.sequence.store(pos + cap, Ordering::Release);
                    return Ok(Some(len));
                }
            } else if diff < 0 {
                return Ok(None);
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> Default for WalRing<SLOTS, SLOT_BYTES> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct SharedWalRing<const SLOTS: usize, const SLOT_BYTES: usize> {
    shm: Arc<ShmArena>,
    ring_ptr: RelPtr<WalRing<SLOTS, SLOT_BYTES>>,
}

impl<const SLOTS: usize, const SLOT_BYTES: usize> SharedWalRing<SLOTS, SLOT_BYTES> {
    pub fn create(shm: Arc<ShmArena>) -> Result<Self, WalRingError> {
        if SLOTS == 0 {
            return Err(WalRingError::InvalidConfiguration("SLOTS must be > 0"));
        }
        if SLOT_BYTES == 0 {
            return Err(WalRingError::InvalidConfiguration("SLOT_BYTES must be > 0"));
        }

        let ring_ptr = shm
            .chunked_arena()
            .alloc(WalRing::<SLOTS, SLOT_BYTES>::new())?;
        Ok(Self { shm, ring_ptr })
    }

    #[inline]
    pub fn from_existing(shm: Arc<ShmArena>, ring_ptr: RelPtr<WalRing<SLOTS, SLOT_BYTES>>) -> Self {
        Self { shm, ring_ptr }
    }

    #[inline]
    pub fn ring_ptr(&self) -> RelPtr<WalRing<SLOTS, SLOT_BYTES>> {
        self.ring_ptr.clone()
    }

    #[inline]
    pub fn push_bytes_blocking(&self, payload: &[u8]) -> Result<(), WalRingError> {
        self.ring_ref()?.push_blocking(payload)
    }

    pub fn push_commit_record(&self, commit: &WalRingCommit) -> Result<(), WalRingError> {
        let bytes = serialize_commit_record(commit)?;
        self.push_bytes_blocking(bytes.as_slice())
    }

    pub fn pop_bytes(&self) -> Result<Option<Vec<u8>>, WalRingError> {
        let ring = self.ring_ref()?;
        let mut scratch = [0_u8; SLOT_BYTES];
        let Some(len) = ring.pop_into(&mut scratch)? else {
            return Ok(None);
        };
        Ok(Some(scratch[..len].to_vec()))
    }

    #[inline]
    pub fn close(&self) -> Result<(), WalRingError> {
        self.ring_ref()?.close();
        Ok(())
    }

    #[inline]
    pub fn is_closed(&self) -> Result<bool, WalRingError> {
        Ok(self.ring_ref()?.is_closed())
    }

    #[inline]
    pub fn is_empty(&self) -> Result<bool, WalRingError> {
        Ok(self.ring_ref()?.is_empty())
    }

    #[inline]
    pub fn writer_epoch(&self) -> Result<u64, WalRingError> {
        Ok(self.ring_ref()?.writer_epoch())
    }

    #[inline]
    pub fn bump_writer_epoch(&self) -> Result<u64, WalRingError> {
        Ok(self.ring_ref()?.bump_writer_epoch())
    }

    fn ring_ref(&self) -> Result<&WalRing<SLOTS, SLOT_BYTES>, WalRingError> {
        let offset = self.ring_ptr.load(Ordering::Acquire);
        self.ring_ptr
            .as_ref(self.shm.mmap_base())
            .ok_or(WalRingError::InvalidRingPointer { offset })
    }
}

pub fn serialize_commit_record(commit: &WalRingCommit) -> Result<AlignedVec, WalRingError> {
    rkyv::to_bytes::<_, 8192>(commit).map_err(|err| WalRingError::Serialize(err.to_string()))
}

pub fn deserialize_commit_record(bytes: &[u8]) -> Result<WalRingCommit, WalRingError> {
    let mut aligned = AlignedVec::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);

    let archived = rkyv::check_archived_root::<WalRingCommit>(aligned.as_slice())
        .map_err(|err| WalRingError::Deserialize(err.to_string()))?;
    match RkyvDeserialize::<WalRingCommit, Infallible>::deserialize(archived, &mut Infallible) {
        Ok(decoded) => {
            if decoded.version != WAL_RING_COMMIT_VERSION {
                return Err(WalRingError::Deserialize(format!(
                    "unsupported wal commit version {} (expected {})",
                    decoded.version, WAL_RING_COMMIT_VERSION
                )));
            }
            Ok(decoded)
        }
        Err(_) => Err(WalRingError::Deserialize(
            "infallible deserialize unexpectedly failed".to_string(),
        )),
    }
}
