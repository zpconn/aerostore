use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::shm_lock::ShmMutex;

pub const PROCARRAY_SLOTS: usize = 256;
const EMPTY_SLOT: u64 = 0;

#[cfg(test)]
thread_local! {
    static REGISTRATION_RESERVED_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

#[repr(align(64))]
pub struct ProcSlot {
    txid: AtomicU64,
    snapshot_xmin: AtomicU64,
}

impl ProcSlot {
    #[inline]
    fn new() -> Self {
        Self {
            txid: AtomicU64::new(EMPTY_SLOT),
            snapshot_xmin: AtomicU64::new(EMPTY_SLOT),
        }
    }

    #[inline]
    fn load(&self, order: Ordering) -> u64 {
        self.txid.load(order)
    }
}

pub struct ProcArray {
    // Only protects metadata transitions and snapshot copying. In particular,
    // this is never held for the lifetime of a transaction. Reserving an ID
    // and advertising it must be indivisible to snapshot observers; otherwise
    // a paused new transaction can be mistaken for an already committed one.
    lifecycle: ShmMutex,
    slots: [ProcSlot; PROCARRAY_SLOTS],
}

impl ProcArray {
    pub fn new() -> Self {
        Self {
            lifecycle: ShmMutex::new(),
            slots: std::array::from_fn(|_| ProcSlot::new()),
        }
    }

    pub fn begin_transaction(
        &self,
        global_txid: &AtomicU64,
    ) -> Result<ProcArrayRegistration, ProcArrayError> {
        let _lifecycle = self.lifecycle.lock();
        let txid = global_txid.fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        REGISTRATION_RESERVED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });

        for (slot_idx, slot) in self.slots.iter().enumerate() {
            if slot
                .txid
                .compare_exchange(EMPTY_SLOT, txid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                slot.snapshot_xmin.store(txid, Ordering::Release);
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
        let _lifecycle = self.lifecycle.lock();
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

        slot.snapshot_xmin.store(EMPTY_SLOT, Ordering::Release);
        slot.txid.store(EMPTY_SLOT, Ordering::Release);
        Ok(())
    }

    pub fn create_snapshot(&self, global_txid: &AtomicU64) -> ProcSnapshot {
        let _lifecycle = self.lifecycle.lock();
        self.snapshot_locked(global_txid)
    }

    /// Capture and publish a transaction's retention horizon in one metadata
    /// critical section. Vacuum must not pass this horizon while the snapshot
    /// can still need versions deleted by an older, then-active writer.
    pub fn create_transaction_snapshot(
        &self,
        registration: ProcArrayRegistration,
        global_txid: &AtomicU64,
    ) -> Result<ProcSnapshot, ProcArrayError> {
        let _lifecycle = self.lifecycle.lock();
        let slot =
            self.slots
                .get(registration.slot_idx as usize)
                .ok_or(ProcArrayError::InvalidSlot {
                    slot_idx: registration.slot_idx,
                })?;
        let observed = slot.load(Ordering::Acquire);
        if observed != registration.txid {
            return Err(ProcArrayError::SlotOwnershipMismatch {
                slot_idx: registration.slot_idx,
                expected_txid: registration.txid,
                observed_txid: observed,
            });
        }
        let snapshot = self.snapshot_locked(global_txid);
        slot.snapshot_xmin.store(snapshot.xmin, Ordering::Release);
        Ok(snapshot)
    }

    /// Oldest pinned MVCC snapshot, including a reader's older active writers.
    /// This is deliberately separate from new snapshots' active-tx xmin: copying
    /// retained horizons into subsequent snapshots would pin old versions
    /// indefinitely under a continuous stream of overlapping readers.
    pub fn oldest_snapshot_xmin(&self, global_txid: &AtomicU64) -> u64 {
        let _lifecycle = self.lifecycle.lock();
        let mut xmin = global_txid.load(Ordering::Acquire);
        for slot in &self.slots {
            if slot.load(Ordering::Acquire) != EMPTY_SLOT {
                xmin = xmin.min(slot.snapshot_xmin.load(Ordering::Acquire));
            }
        }
        xmin
    }

    fn snapshot_locked(&self, global_txid: &AtomicU64) -> ProcSnapshot {
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

    /// Startup recovery only: the caller must establish exclusive ownership
    /// after all previous workers have stopped. This already clears every slot,
    /// including apparently active registrations; it is not live-worker cleanup.
    /// Reset the metadata latch too, since a dead process may have held it.
    pub fn clear_orphaned_slots(&self) -> usize {
        self.lifecycle.reset_after_exclusive_recovery();
        let _lifecycle = self.lifecycle.lock();
        let mut cleared = 0_usize;
        for slot in self.slots.iter() {
            let txid = slot.load(Ordering::Acquire);
            if txid == EMPTY_SLOT {
                continue;
            }
            slot.snapshot_xmin.store(EMPTY_SLOT, Ordering::Release);
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

#[cfg(test)]
mod tests {
    use super::{ProcArray, ProcArrayError, PROCARRAY_SLOTS};
    use std::sync::atomic::AtomicU64;

    #[test]
    fn snapshot_cannot_mistake_reserved_unpublished_txid_for_a_commit() {
        use std::sync::{mpsc, Arc, Barrier};
        use std::time::Duration;
        let procarray = Arc::new(ProcArray::new());
        let global = Arc::new(AtomicU64::new(100));
        let reserved = Arc::new(Barrier::new(2));
        let publish = Arc::new(Barrier::new(2));
        let writer_array = Arc::clone(&procarray);
        let writer_global = Arc::clone(&global);
        let writer_reserved = Arc::clone(&reserved);
        let writer_publish = Arc::clone(&publish);
        let writer = std::thread::spawn(move || {
            super::REGISTRATION_RESERVED_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    writer_reserved.wait();
                    writer_publish.wait();
                }))
            });
            writer_array.begin_transaction(&writer_global).unwrap()
        });
        reserved.wait();
        assert_eq!(global.load(std::sync::atomic::Ordering::Acquire), 101);
        let reservation_holds_latch = procarray.lifecycle.try_lock().is_none();
        let reader_array = Arc::clone(&procarray);
        let reader_global = Arc::clone(&global);
        let (started_send, started_recv) = mpsc::channel();
        let (snapshot_send, snapshot_recv) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            started_send.send(()).unwrap();
            snapshot_send
                .send(reader_array.create_snapshot(&reader_global))
                .unwrap();
        });
        started_recv.recv().unwrap();
        let premature = snapshot_recv.recv_timeout(Duration::from_millis(20));
        publish.wait();
        let registration = writer.join().unwrap();
        let snapshot = match premature {
            Ok(snapshot) => snapshot,
            Err(_) => snapshot_recv.recv_timeout(Duration::from_secs(2)).unwrap(),
        };
        reader.join().unwrap();
        // Release/join both workers before asserting, including when the
        // missing-lifecycle-lock mutation exposes the reservation gap.
        assert!(
            reservation_holds_latch,
            "reservation must hold snapshot metadata latch"
        );
        // A scan without the lifecycle latch can incorrectly return xmax=101
        // and an empty active set while txid100 is reserved but unpublished.
        assert!(snapshot.in_flight_txids().contains(&registration.txid));
        assert_eq!(snapshot.xmin, registration.txid);
        procarray.end_transaction(registration).unwrap();
    }

    #[test]
    fn begin_transaction_exhausts_slots_and_returns_no_free_slot() {
        let procarray = ProcArray::new();
        let global = AtomicU64::new(1);
        let mut regs = Vec::with_capacity(PROCARRAY_SLOTS);
        for _ in 0..PROCARRAY_SLOTS {
            regs.push(
                procarray
                    .begin_transaction(&global)
                    .expect("slot should be available"),
            );
        }

        let err = procarray
            .begin_transaction(&global)
            .expect_err("slots should be exhausted");
        assert!(matches!(err, ProcArrayError::NoFreeSlot { .. }));

        for reg in regs {
            procarray
                .end_transaction(reg)
                .expect("release occupied slot");
        }
    }

    #[test]
    fn end_transaction_rejects_invalid_slot() {
        let procarray = ProcArray::new();
        let err = procarray
            .end_transaction(super::ProcArrayRegistration {
                slot_idx: u16::MAX,
                txid: 1,
            })
            .expect_err("invalid slot should fail");
        assert!(matches!(err, ProcArrayError::InvalidSlot { .. }));
    }

    #[test]
    fn end_transaction_rejects_slot_ownership_mismatch() {
        let procarray = ProcArray::new();
        let global = AtomicU64::new(1);
        let reg = procarray
            .begin_transaction(&global)
            .expect("begin_transaction should succeed");
        let err = procarray
            .end_transaction(super::ProcArrayRegistration {
                slot_idx: reg.slot_idx,
                txid: reg.txid + 1,
            })
            .expect_err("mismatched txid should fail");
        assert!(matches!(err, ProcArrayError::SlotOwnershipMismatch { .. }));

        procarray
            .end_transaction(reg)
            .expect("cleanup should still succeed");
    }

    #[test]
    fn snapshot_xmin_xmax_and_inflight_set_consistent() {
        let procarray = ProcArray::new();
        let global = AtomicU64::new(100);
        let a = procarray.begin_transaction(&global).expect("slot A");
        let b = procarray.begin_transaction(&global).expect("slot B");
        let snap = procarray.create_snapshot(&global);

        assert!(snap.len() >= 2);
        assert!(snap.in_flight_txids().contains(&a.txid));
        assert!(snap.in_flight_txids().contains(&b.txid));
        assert!(snap.xmin <= a.txid.min(b.txid));
        assert!(snap.xmax > a.txid.max(b.txid));

        procarray.end_transaction(a).expect("end A");
        procarray.end_transaction(b).expect("end B");
    }

    #[test]
    fn exclusive_orphan_recovery_resets_a_dead_metadata_latch() {
        let procarray = ProcArray::new();
        let global = AtomicU64::new(1);
        let _registration = procarray.begin_transaction(&global).unwrap();
        // A terminated process cannot run the guard destructor. There are no
        // live users in this fixture, matching exclusive startup recovery.
        std::mem::forget(procarray.lifecycle.lock());
        assert_eq!(procarray.clear_orphaned_slots(), 1);
        let registration = procarray.begin_transaction(&global).unwrap();
        procarray.end_transaction(registration).unwrap();
    }

    #[test]
    fn clear_orphaned_slots_clears_and_reports_count() {
        let procarray = ProcArray::new();
        let global = AtomicU64::new(1);
        let a = procarray.begin_transaction(&global).expect("slot A");
        let b = procarray.begin_transaction(&global).expect("slot B");

        let cleared = procarray.clear_orphaned_slots();
        assert_eq!(cleared, 2);
        for idx in 0..procarray.slots_len() {
            assert_eq!(procarray.slot_txid(idx).unwrap_or_default(), 0);
        }

        // Double-clear should be a no-op.
        assert_eq!(procarray.clear_orphaned_slots(), 0);

        // Releasing old registrations should now fail due to cleared slots.
        assert!(matches!(
            procarray.end_transaction(a),
            Err(ProcArrayError::SlotOwnershipMismatch { .. })
        ));
        assert!(matches!(
            procarray.end_transaction(b),
            Err(ProcArrayError::SlotOwnershipMismatch { .. })
        ));
    }
}
