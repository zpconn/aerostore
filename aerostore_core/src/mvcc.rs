use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::epoch::{self, Atomic, Guard, Owned, Shared};

use crate::txn::{Snapshot, Transaction, TransactionManager, TxId};

const DEFAULT_BUCKETS: usize = 1 << 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvccError {
    NotFound,
    WriteConflict,
}

pub struct RowVersion<V> {
    pub value: V,
    pub _xmin: TxId,
    pub _xmax: AtomicU64,
    pub next: Atomic<RowVersion<V>>,
}

impl<V> RowVersion<V> {
    #[inline]
    pub fn new(value: V, xmin: TxId) -> Self {
        Self {
            value,
            _xmin: xmin,
            _xmax: AtomicU64::new(0),
            next: Atomic::null(),
        }
    }
}

struct BucketEntry<K, V> {
    key: K,
    head: Atomic<RowVersion<V>>,
    next: Atomic<BucketEntry<K, V>>,
}

impl<K, V> BucketEntry<K, V> {
    #[inline]
    fn new(key: K) -> Self {
        Self {
            key,
            head: Atomic::null(),
            next: Atomic::null(),
        }
    }
}

pub struct MvccTable<K, V, S = RandomState> {
    buckets: Box<[Atomic<BucketEntry<K, V>>]>,
    hasher: S,
}

impl<K, V> MvccTable<K, V, RandomState>
where
    K: Eq + Hash + Clone,
{
    pub fn new(bucket_count: usize) -> Self {
        Self::with_hasher(bucket_count, RandomState::new())
    }
}

impl<K, V, S> MvccTable<K, V, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    pub fn with_hasher(bucket_count: usize, hasher: S) -> Self {
        let buckets = if bucket_count == 0 {
            DEFAULT_BUCKETS
        } else {
            bucket_count
        };

        let mut v = Vec::with_capacity(buckets);
        for _ in 0..buckets {
            v.push(Atomic::null());
        }

        Self {
            buckets: v.into_boxed_slice(),
            hasher,
        }
    }

    pub fn insert(&self, key: K, value: V, tx: &Transaction) -> Result<(), MvccError> {
        let guard = &epoch::pin();
        let entry = self.get_or_insert_entry(&key, guard);
        let entry_ref = unsafe { entry.as_ref().expect("entry pointer unexpectedly null") };

        let mut new_head = Owned::new(RowVersion::new(value, tx.txid));
        loop {
            let current_head = entry_ref.head.load(Ordering::Acquire, guard);
            new_head.next.store(current_head, Ordering::Relaxed);

            match entry_ref.head.compare_exchange(
                current_head,
                new_head,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(err) => new_head = err.new,
            }
        }
    }

    pub fn update(&self, key: &K, value: V, tx: &Transaction) -> Result<(), MvccError>
    where
        V: Clone,
    {
        let guard = &epoch::pin();
        let entry = self.find_entry(key, guard).ok_or(MvccError::NotFound)?;
        let entry_ref = unsafe { entry.as_ref().ok_or(MvccError::NotFound)? };

        loop {
            let current_head = entry_ref.head.load(Ordering::Acquire, guard);
            let current_ref = unsafe { current_head.as_ref().ok_or(MvccError::NotFound)? };

            if !is_visible(current_ref, tx.txid, &tx.snapshot) {
                return Err(MvccError::WriteConflict);
            }

            if current_ref
                ._xmax
                .compare_exchange(0, tx.txid, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            let new_head = Owned::new(RowVersion::new(value.clone(), tx.txid));
            new_head.next.store(current_head, Ordering::Relaxed);

            match entry_ref.head.compare_exchange(
                current_head,
                new_head,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => {
                    let _ = current_ref._xmax.compare_exchange(
                        tx.txid,
                        0,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    return Err(MvccError::WriteConflict);
                }
            }
        }
    }

    pub fn delete(&self, key: &K, tx: &Transaction) -> Result<(), MvccError> {
        let guard = &epoch::pin();
        let entry = self.find_entry(key, guard).ok_or(MvccError::NotFound)?;
        let entry_ref = unsafe { entry.as_ref().ok_or(MvccError::NotFound)? };
        let current_head = entry_ref.head.load(Ordering::Acquire, guard);
        let current_ref = unsafe { current_head.as_ref().ok_or(MvccError::NotFound)? };

        if !is_visible(current_ref, tx.txid, &tx.snapshot) {
            return Err(MvccError::WriteConflict);
        }

        current_ref
            ._xmax
            .compare_exchange(0, tx.txid, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| MvccError::WriteConflict)
    }

    pub fn read_visible(&self, key: &K, tx: &Transaction) -> Option<V>
    where
        V: Clone,
    {
        let guard = &epoch::pin();
        let entry = self.find_entry(key, guard)?;
        let entry_ref = unsafe { entry.as_ref()? };
        let mut cursor = entry_ref.head.load(Ordering::Acquire, guard);

        while let Some(row) = unsafe { cursor.as_ref() } {
            if is_visible(row, tx.txid, &tx.snapshot) {
                return Some(row.value.clone());
            }
            cursor = row.next.load(Ordering::Acquire, guard);
        }

        None
    }

    pub fn commit(&self, tx_manager: &TransactionManager, tx: &Transaction) -> usize {
        tx_manager.commit(tx);
        let oldest_active = tx_manager.oldest_active_snapshot_xmin();
        self.garbage_collect(oldest_active)
    }

    pub fn garbage_collect(&self, oldest_active_txid: TxId) -> usize {
        let guard = &epoch::pin();
        let mut reclaimed = 0_usize;

        for bucket in self.buckets.iter() {
            let mut entry_cursor = bucket.load(Ordering::Acquire, guard);
            while let Some(entry_ref) = unsafe { entry_cursor.as_ref() } {
                'retry_chain: loop {
                    let mut prev: Option<Shared<'_, RowVersion<V>>> = None;
                    let mut current = entry_ref.head.load(Ordering::Acquire, guard);

                    while let Some(row_ref) = unsafe { current.as_ref() } {
                        let next = row_ref.next.load(Ordering::Acquire, guard);
                        let xmax = row_ref._xmax.load(Ordering::Acquire);
                        let reclaimable = xmax != 0 && xmax < oldest_active_txid;

                        if reclaimable {
                            let unlinked = if let Some(prev_ptr) = prev {
                                let prev_ref = unsafe {
                                    prev_ptr
                                        .as_ref()
                                        .expect("version node disappeared during traversal")
                                };
                                prev_ref.next.compare_exchange(
                                    current,
                                    next,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                    guard,
                                )
                            } else {
                                entry_ref.head.compare_exchange(
                                    current,
                                    next,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                    guard,
                                )
                            };

                            match unlinked {
                                Ok(unlinked_ptr) => {
                                    if !unlinked_ptr.is_null() {
                                        reclaimed += 1;
                                        unsafe {
                                            guard.defer_destroy(unlinked_ptr);
                                        }
                                    }
                                    current = next;
                                    continue;
                                }
                                Err(_) => continue 'retry_chain,
                            }
                        }

                        prev = Some(current);
                        current = next;
                    }

                    break;
                }

                entry_cursor = entry_ref.next.load(Ordering::Acquire, guard);
            }
        }

        reclaimed
    }

    #[inline]
    fn bucket_index(&self, key: &K) -> usize {
        let mut state = self.hasher.build_hasher();
        key.hash(&mut state);
        (state.finish() as usize) % self.buckets.len()
    }

    fn find_entry<'g>(&self, key: &K, guard: &'g Guard) -> Option<Shared<'g, BucketEntry<K, V>>> {
        let idx = self.bucket_index(key);
        let mut current = self.buckets[idx].load(Ordering::Acquire, guard);

        loop {
            let Some(entry_ref) = (unsafe { current.as_ref() }) else {
                return None;
            };

            if &entry_ref.key == key {
                return Some(current);
            }

            current = entry_ref.next.load(Ordering::Acquire, guard);
        }
    }

    fn get_or_insert_entry<'g>(&self, key: &K, guard: &'g Guard) -> Shared<'g, BucketEntry<K, V>> {
        let idx = self.bucket_index(key);
        let bucket = &self.buckets[idx];

        loop {
            let head = bucket.load(Ordering::Acquire, guard);
            let mut current = head;

            loop {
                let Some(entry_ref) = (unsafe { current.as_ref() }) else {
                    break;
                };

                if &entry_ref.key == key {
                    return current;
                }

                current = entry_ref.next.load(Ordering::Acquire, guard);
            }

            let new_entry = Owned::new(BucketEntry::new(key.clone()));
            new_entry.next.store(head, Ordering::Relaxed);

            match bucket.compare_exchange(
                head,
                new_entry,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(inserted) => return inserted,
                Err(_) => continue,
            }
        }
    }
}

pub fn is_visible<V>(row: &RowVersion<V>, current_txid: TxId, snapshot: &Snapshot) -> bool {
    if !xmin_visible(row._xmin, current_txid, snapshot) {
        return false;
    }

    let xmax = row._xmax.load(Ordering::Acquire);
    xmax_visible(xmax, current_txid, snapshot)
}

#[inline]
fn xmin_visible(xmin: TxId, current_txid: TxId, snapshot: &Snapshot) -> bool {
    if xmin == current_txid {
        return true;
    }

    if xmin >= snapshot.xmax {
        return false;
    }

    if xmin < snapshot.xmin {
        return true;
    }

    !snapshot.is_active(xmin)
}

#[inline]
fn xmax_visible(xmax: TxId, current_txid: TxId, snapshot: &Snapshot) -> bool {
    if xmax == 0 {
        return true;
    }

    if xmax == current_txid {
        return false;
    }

    if xmax >= snapshot.xmax {
        return true;
    }

    if xmax < snapshot.xmin {
        return false;
    }

    snapshot.is_active(xmax)
}

unsafe impl<K: Send, V: Send, S: Send> Send for MvccTable<K, V, S> {}
unsafe impl<K: Send + Sync, V: Send + Sync, S: Send + Sync> Sync for MvccTable<K, V, S> {}

impl<K, V, S> Drop for MvccTable<K, V, S> {
    fn drop(&mut self) {
        unsafe {
            let guard = epoch::unprotected();
            for bucket in self.buckets.iter() {
                let mut entry_cursor = bucket.load(Ordering::Relaxed, guard);
                while let Some(entry_ref) = entry_cursor.as_ref() {
                    let mut row_cursor = entry_ref.head.load(Ordering::Relaxed, guard);
                    while let Some(row_ref) = row_cursor.as_ref() {
                        let next_row = row_ref.next.load(Ordering::Relaxed, guard);
                        drop(Box::from_raw(row_cursor.as_raw() as *mut RowVersion<V>));
                        row_cursor = next_row;
                    }

                    let next_entry = entry_ref.next.load(Ordering::Relaxed, guard);
                    drop(Box::from_raw(
                        entry_cursor.as_raw() as *mut BucketEntry<K, V>
                    ));
                    entry_cursor = next_entry;
                }
            }
        }
    }
}
