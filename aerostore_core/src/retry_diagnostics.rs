//! Optional observations of the first native rejection in one synchronous
//! adapter operation. These report branches, not counterfactual conflict causes.
//! Callers clear before an operation and take immediately after its result,
//! before cleanup. State is thread-local and never changes transaction state.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause {
    IndexBucketBusy,
    LookupPostSnapshotStamp,
    LookupChangedCapturedStamp,
    StickyIndexConflict,
    PredicateValidationStamp,
    LockForUpdateHeld,
    LockForUpdateRace,
    ReadRowLocked,
    WriteRowLocked,
    WriteDirtyRowLocked,
    CommitRowLocked,
    ReadVersionIdentityChanged,
    ReadVersionDeletedAfterSnapshot,
    WriteBaseHeadChanged,
    WriteBaseXmaxSet,
    VisibleChainLimit,
    PartitionLockBusy,
    WalWriterEpochChanged,
}

impl Cause {
    pub const fn name(self) -> &'static str {
        match self {
            Self::IndexBucketBusy => "index_bucket_busy",
            Self::LookupPostSnapshotStamp => "lookup_post_snapshot_stamp",
            Self::LookupChangedCapturedStamp => "lookup_changed_captured_stamp",
            Self::StickyIndexConflict => "sticky_index_conflict",
            Self::PredicateValidationStamp => "predicate_validation_stamp",
            Self::LockForUpdateHeld => "lock_for_update_held",
            Self::LockForUpdateRace => "lock_for_update_race",
            Self::ReadRowLocked => "read_row_locked",
            Self::WriteRowLocked => "write_row_locked",
            Self::WriteDirtyRowLocked => "write_dirty_row_locked",
            Self::CommitRowLocked => "commit_row_locked",
            Self::ReadVersionIdentityChanged => "read_version_identity_changed",
            Self::ReadVersionDeletedAfterSnapshot => "read_version_deleted_after_snapshot",
            Self::WriteBaseHeadChanged => "write_base_head_changed",
            Self::WriteBaseXmaxSet => "write_base_xmax_set",
            Self::VisibleChainLimit => "visible_chain_limit",
            Self::PartitionLockBusy => "partition_lock_busy",
            Self::WalWriterEpochChanged => "wal_writer_epoch_changed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Event {
    pub cause: Cause,
    pub index_offset: Option<u32>,
    pub row_id: Option<usize>,
}

#[cfg(feature = "retry-diagnostics")]
#[derive(Clone, Copy)]
struct State {
    enabled: bool,
    first: Option<Event>,
}

#[cfg(feature = "retry-diagnostics")]
std::thread_local! {
    static STATE: std::cell::Cell<State> = const {
        std::cell::Cell::new(State { enabled: false, first: None })
    };
}

#[inline]
pub const fn compiled() -> bool {
    cfg!(feature = "retry-diagnostics")
}

/// Enable or disable this thread and discard any earlier observation.
#[inline]
pub fn set_enabled(enabled: bool) {
    #[cfg(feature = "retry-diagnostics")]
    let _ = STATE.try_with(|state| {
        state.set(State {
            enabled,
            first: None,
        })
    });
    #[cfg(not(feature = "retry-diagnostics"))]
    let _ = enabled;
}

#[inline]
pub fn clear() {
    #[cfg(feature = "retry-diagnostics")]
    let _ = STATE.try_with(|state| {
        state.set(State {
            first: None,
            ..state.get()
        })
    });
}

#[inline]
pub fn take() -> Option<Event> {
    #[cfg(feature = "retry-diagnostics")]
    return STATE
        .try_with(|state| {
            let previous = state.get();
            state.set(State {
                first: None,
                ..previous
            });
            previous.first
        })
        .ok()
        .flatten();
    #[cfg(not(feature = "retry-diagnostics"))]
    None
}

/// A fixed-size, allocation-free observation on an already rejecting branch.
/// `try_with` also makes teardown calls harmless when TLS is unavailable.
#[cfg(feature = "retry-diagnostics")]
#[inline]
pub(crate) fn record(cause: Cause, index_offset: Option<u32>, row_id: Option<usize>) {
    let _ = STATE.try_with(|state| {
        let previous = state.get();
        if previous.enabled && previous.first.is_none() {
            state.set(State {
                first: Some(Event {
                    cause,
                    index_offset,
                    row_id,
                }),
                ..previous
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IndexCompare, IndexValue, OccError, OccTable, SecondaryIndex, ShmArena};
    use std::sync::Arc;

    struct Observe;
    impl Observe {
        fn new() -> Self {
            set_enabled(true);
            Self
        }
    }
    impl Drop for Observe {
        fn drop(&mut self) {
            set_enabled(false);
        }
    }
    fn expect(cause: Cause, index_offset: Option<u32>) {
        assert_eq!(
            take(),
            compiled().then_some(Event {
                cause,
                index_offset,
                row_id: None
            })
        );
    }

    #[test]
    fn exhausted_partition_lock_budget_reports_retry_without_publication() {
        let _observe = Observe::new();
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let table = OccTable::new(arena.clone(), 1).unwrap();
        table.seed_row(0, 10_u64).unwrap();
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, 20).unwrap();
        for lock in arena.occ_partition_locks() {
            assert!(lock.try_lock());
        }
        let result = table.commit(&mut tx);
        for lock in arena.occ_partition_locks() {
            lock.unlock();
        }
        assert_eq!(result, Err(OccError::SerializationFailure));
        expect(Cause::PartitionLockBusy, None);
        assert_eq!(table.latest_value(0).unwrap(), Some(10));
        table.abort(&mut tx).unwrap();
    }

    #[test]
    fn exhausted_index_bucket_budget_reports_the_actual_index() {
        let _observe = Observe::new();
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let mut table = OccTable::new(arena.clone(), 1).unwrap();
        table.seed_row(0, 10_u64).unwrap();
        let index = SecondaryIndex::new_in_shared("locked", arena);
        index.try_insert(IndexValue::U64(10), 0).unwrap();
        table
            .bind_index(index.clone(), |v| Some(IndexValue::U64(*v)))
            .unwrap();
        let bucket = index
            .transactional_key_bucket(&IndexValue::U64(10))
            .unwrap();
        let guard = index
            .transactional_try_lock_bucket(bucket)
            .unwrap()
            .unwrap();
        let mut tx = table.begin_transaction().unwrap();
        let result = table.index_lookup(&mut tx, &index, &IndexCompare::Eq(IndexValue::U64(10)));
        drop(guard);
        assert_eq!(result, Err(OccError::SerializationFailure));
        expect(Cause::IndexBucketBusy, Some(index.header_offset()));
        table.abort(&mut tx).unwrap();
    }

    std::thread_local! {
        static EPOCH_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const {
            std::cell::RefCell::new(None)
        };
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
    struct EpochRow(u64);
    impl serde::Serialize for EpochRow {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let hook = EPOCH_HOOK.with(|hook| hook.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
            serializer.serialize_u64(self.0)
        }
    }
    impl crate::wal_delta::WalDeltaCodec for EpochRow {}

    #[test]
    fn changed_epoch_after_payload_preparation_rejects_before_wal_or_publication() {
        let _observe = Observe::new();
        let arena = Arc::new(ShmArena::new(8 << 20).unwrap());
        let table = OccTable::new(arena.clone(), 1).unwrap();
        table.seed_row(0, EpochRow(10)).unwrap();
        let ring = crate::wal_ring::SharedWalRing::<8, 1024>::create(arena.clone()).unwrap();
        let mut committer = crate::wal_writer::OccCommitter::new_asynchronous(ring.clone());
        let mut tx = table.begin_transaction().unwrap();
        table.write(&mut tx, 0, EpochRow(20)).unwrap();
        EPOCH_HOOK.with(|hook| {
            let ring = ring.clone();
            *hook.borrow_mut() = Some(Box::new(move || {
                ring.bump_writer_epoch().unwrap();
            }));
        });
        assert!(matches!(
            committer.commit(&table, &mut tx),
            Err(crate::wal_writer::WalWriterError::Occ(
                OccError::SerializationFailure
            ))
        ));
        expect(Cause::WalWriterEpochChanged, None);
        assert!(EPOCH_HOOK.with(|hook| hook.borrow().is_none()));
        assert!(ring.is_empty().unwrap());
        assert_eq!(table.latest_value(0).unwrap(), Some(EpochRow(10)));
        table.abort(&mut tx).unwrap();
        assert!(arena.create_snapshot().is_empty());
    }

    #[test]
    fn default_or_disabled_observations_are_empty() {
        assert_eq!(compiled(), cfg!(feature = "retry-diagnostics"));
        set_enabled(false);
        #[cfg(feature = "retry-diagnostics")]
        record(Cause::IndexBucketBusy, Some(7), None);
        clear();
        assert_eq!(take(), None);
        assert_eq!(take(), None);
    }

    #[cfg(feature = "retry-diagnostics")]
    #[test]
    fn first_event_is_consumed_cleared_and_isolated_between_threads() {
        set_enabled(true);
        let first = Event {
            cause: Cause::ReadRowLocked,
            index_offset: None,
            row_id: Some(3),
        };
        record(first.cause, first.index_offset, first.row_id);
        record(Cause::PartitionLockBusy, None, None);
        std::thread::spawn(|| {
            assert_eq!(take(), None);
            set_enabled(true);
            record(Cause::WalWriterEpochChanged, None, None);
            assert_eq!(take().unwrap().cause, Cause::WalWriterEpochChanged);
            set_enabled(false);
        })
        .join()
        .unwrap();
        assert_eq!(take(), Some(first));
        assert_eq!(take(), None);
        record(Cause::PartitionLockBusy, None, None);
        clear();
        assert_eq!(take(), None);
        record(Cause::IndexBucketBusy, Some(9), None);
        set_enabled(false);
        assert_eq!(take(), None);
        record(Cause::IndexBucketBusy, Some(9), None);
        assert_eq!(take(), None);
    }
}
