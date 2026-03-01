use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

pub const PROCARRAY_SLOTS: usize = 256;
const EMPTY_SLOT: u64 = 0;

#[repr(align(64))]
pub struct ProcSlot {
    txid: AtomicU64,
}

impl ProcSlot {
    #[inline]
    fn new() -> Self {
        Self {
            txid: AtomicU64::new(EMPTY_SLOT),
        }
    }

    #[inline]
    fn load(&self, order: Ordering) -> u64 {
        self.txid.load(order)
    }
}

pub struct ProcArray {
    slots: [ProcSlot; PROCARRAY_SLOTS],
}

impl ProcArray {
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| ProcSlot::new()),
        }
    }

    pub fn begin_transaction(
        &self,
        global_txid: &AtomicU64,
    ) -> Result<ProcArrayRegistration, ProcArrayError> {
        let txid = global_txid.fetch_add(1, Ordering::AcqRel);

        for (slot_idx, slot) in self.slots.iter().enumerate() {
            if slot
                .txid
                .compare_exchange(EMPTY_SLOT, txid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(ProcArrayRegistration {
                    slot_idx: slot_idx as u16,
                    txid,
                });
            }
        }

        Err(ProcArrayError::NoFreeSlot { txid })
    }

    pub fn end_transaction(
        &self,
        registration: ProcArrayRegistration,
    ) -> Result<(), ProcArrayError> {
        let slot_idx = registration.slot_idx as usize;
        if slot_idx >= PROCARRAY_SLOTS {
            return Err(ProcArrayError::InvalidSlot {
                slot_idx: registration.slot_idx,
            });
        }

        let slot = &self.slots[slot_idx];
        let observed = slot.load(Ordering::Acquire);
        if observed != registration.txid {
            return Err(ProcArrayError::SlotOwnershipMismatch {
                slot_idx: registration.slot_idx,
                expected_txid: registration.txid,
                observed_txid: observed,
            });
        }

        slot.txid.store(EMPTY_SLOT, Ordering::Release);
        Ok(())
    }

    pub fn create_snapshot(&self, global_txid: &AtomicU64) -> ProcSnapshot {
        let mut xmax = global_txid.load(Ordering::Relaxed);
        let mut xmin = xmax;
        let mut max_in_flight = 0_u64;
        let mut in_flight = [const { MaybeUninit::uninit() }; PROCARRAY_SLOTS];
        let mut in_flight_len = 0_u16;

        for slot in self.slots.iter() {
            let txid = slot.load(Ordering::Relaxed);
            if txid == EMPTY_SLOT {
                continue;
            }

            in_flight[in_flight_len as usize].write(txid);
            in_flight_len += 1;
            xmin = xmin.min(txid);
            max_in_flight = max_in_flight.max(txid);
        }

        if in_flight_len == 0 {
            xmin = xmax;
        } else {
            xmax = xmax.max(max_in_flight.saturating_add(1));
        }

        ProcSnapshot {
            xmin,
            xmax,
            in_flight,
            in_flight_len,
        }
    }

    pub fn clear_orphaned_slots(&self) -> usize {
        let mut cleared = 0_usize;
        for slot in self.slots.iter() {
            let txid = slot.load(Ordering::Acquire);
            if txid == EMPTY_SLOT {
                continue;
            }
            slot.txid.store(EMPTY_SLOT, Ordering::Release);
            cleared += 1;
        }
        cleared
    }

    #[inline]
    pub fn slots_len(&self) -> usize {
        PROCARRAY_SLOTS
    }

    #[inline]
    pub fn slot_txid(&self, slot_idx: usize) -> Option<u64> {
        self.slots
            .get(slot_idx)
            .map(|slot| slot.load(Ordering::Acquire))
    }
}

impl Default for ProcArray {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ProcSnapshot {
    pub xmin: u64,
    pub xmax: u64,
    in_flight: [MaybeUninit<u64>; PROCARRAY_SLOTS],
    in_flight_len: u16,
}

impl ProcSnapshot {
    #[inline]
    pub fn len(&self) -> usize {
        self.in_flight_len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.in_flight_len == 0
    }

    #[inline]
    pub fn in_flight_txids(&self) -> &[u64] {
        // SAFETY:
        // `create_snapshot` only writes initialized txids into slots `[0..in_flight_len)`.
        unsafe { std::slice::from_raw_parts(self.in_flight.as_ptr().cast(), self.len()) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcArrayRegistration {
    pub slot_idx: u16,
    pub txid: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcArrayError {
    NoFreeSlot {
        txid: u64,
    },
    InvalidSlot {
        slot_idx: u16,
    },
    SlotOwnershipMismatch {
        slot_idx: u16,
        expected_txid: u64,
        observed_txid: u64,
    },
}

impl fmt::Display for ProcArrayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcArrayError::NoFreeSlot { txid } => write!(
                f,
                "no free ProcArray slot available for txid {}; max {} concurrent workers reached",
                txid, PROCARRAY_SLOTS
            ),
            ProcArrayError::InvalidSlot { slot_idx } => {
                write!(f, "invalid ProcArray slot index {}", slot_idx)
            }
            ProcArrayError::SlotOwnershipMismatch {
                slot_idx,
                expected_txid,
                observed_txid,
            } => write!(
                f,
                "slot {} ownership mismatch (expected txid {}, observed txid {})",
                slot_idx, expected_txid, observed_txid
            ),
        }
    }
}

impl std::error::Error for ProcArrayError {}
