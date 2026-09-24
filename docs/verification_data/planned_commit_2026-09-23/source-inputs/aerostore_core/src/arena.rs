use std::cell::UnsafeCell;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crossbeam::epoch::{self, Atomic, Guard, Owned, Shared};

const DEFAULT_CHUNK_CAPACITY: usize = 64 * 1024;
const DEFAULT_BUCKETS: usize = 1 << 16;

struct Chunk<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    cursor: AtomicUsize,
    next: AtomicPtr<Chunk<T>>,
}

impl<T> Chunk<T> {
    fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        Self {
            slots: slots.into_boxed_slice(),
            cursor: AtomicUsize::new(0),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

unsafe impl<T: Send> Send for Chunk<T> {}
unsafe impl<T: Send> Sync for Chunk<T> {}

pub struct ChunkedArena<T> {
    chunk_capacity: usize,
    head: *mut Chunk<T>,
    current: AtomicPtr<Chunk<T>>,
}

impl<T> ChunkedArena<T> {
    pub fn new(chunk_capacity: usize) -> Self {
        assert!(chunk_capacity > 0, "chunk_capacity must be > 0");

        let first = Box::into_raw(Box::new(Chunk::new(chunk_capacity)));

        Self {
            chunk_capacity,
            head: first,
            current: AtomicPtr::new(first),
        }
    }

    #[inline]
    pub fn alloc(&self, value: T) -> *mut T {
        let mut pending = Some(value);

        loop {
            let current_ptr = self.current.load(Ordering::Relaxed);
            debug_assert!(!current_ptr.is_null());
            let current = unsafe { &*current_ptr };

            let idx = current.cursor.fetch_add(1, Ordering::Relaxed);
            if idx < self.chunk_capacity {
                unsafe {
                    (*current.slots[idx].get()).write(pending.take().expect("value already moved"));
                }
                return current.slots[idx].get().cast::<T>();
            }

            if idx == self.chunk_capacity {
                let new_chunk = Box::into_raw(Box::new(Chunk::new(self.chunk_capacity)));
                let next_ptr = match current.next.compare_exchange(
                    ptr::null_mut(),
                    new_chunk,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => new_chunk,
                    Err(existing) => {
                        unsafe {
                            drop(Box::from_raw(new_chunk));
                        }
                        existing
                    }
                };

                let _ = self.current.compare_exchange(
                    current_ptr,
                    next_ptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                );
            } else {
                let next_ptr = current.next.load(Ordering::Acquire);
                if !next_ptr.is_null() {
                    let _ = self.current.compare_exchange(
                        current_ptr,
                        next_ptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        let mut total = 0_usize;
        let mut chunk_ptr = self.head;

        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            total += chunk
                .cursor
                .load(Ordering::Acquire)
                .min(self.chunk_capacity);
            chunk_ptr = chunk.next.load(Ordering::Acquire);
        }

        total
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn chunk_capacity(&self) -> usize {
        self.chunk_capacity
    }
}

impl<T> Default for ChunkedArena<T> {
    fn default() -> Self {
        Self::new(DEFAULT_CHUNK_CAPACITY)
    }
}

unsafe impl<T: Send> Send for ChunkedArena<T> {}
unsafe impl<T: Send> Sync for ChunkedArena<T> {}

impl<T> Drop for ChunkedArena<T> {
    fn drop(&mut self) {
        unsafe {
            let mut chunk_ptr = self.head;
            while !chunk_ptr.is_null() {
                let chunk = Box::from_raw(chunk_ptr);
                let used = chunk
                    .cursor
                    .load(Ordering::Acquire)
                    .min(self.chunk_capacity);
                for idx in 0..used {
                    ptr::drop_in_place((*chunk.slots[idx].get()).as_mut_ptr());
                }
                chunk_ptr = chunk.next.load(Ordering::Acquire);
            }
        }
    }
}

pub struct VersionNode<V> {
    pub value: V,
    pub xmin: u64,
    pub xmax: AtomicU64,
    pub next: Atomic<VersionNode<V>>,
}

impl<V> VersionNode<V> {
    #[inline]
    pub fn new(value: V, xmin: u64) -> Self {
        Self {
            value,
            xmin,
            xmax: AtomicU64::new(0),
            next: Atomic::null(),
        }
    }
}

struct BucketEntry<K, V> {
    key: K,
    head: Atomic<VersionNode<V>>,
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

pub struct Table<K, V, S = RandomState> {
    buckets: Box<[Atomic<BucketEntry<K, V>>]>,
    version_arena: ChunkedArena<VersionNode<V>>,
    hasher: S,
}

impl<K, V> Table<K, V, RandomState>
where
    K: Eq + Hash + Clone,
{
    pub fn new(bucket_count: usize, chunk_capacity: usize) -> Self {
        Self::with_hasher(bucket_count, chunk_capacity, RandomState::new())
    }
}

impl<K, V, S> Table<K, V, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    pub fn with_hasher(bucket_count: usize, chunk_capacity: usize, hasher: S) -> Self {
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
            version_arena: ChunkedArena::new(chunk_capacity),
            hasher,
        }
    }

    #[inline]
    pub fn upsert(&self, key: K, value: V, xmin: u64) -> *mut VersionNode<V> {
        let guard = &epoch::pin();
        let entry = self.get_or_insert_entry(&key, guard);
        let entry_ref = unsafe { entry.as_ref().expect("entry pointer unexpectedly null") };

        let node_ptr = self.version_arena.alloc(VersionNode::new(value, xmin));

        loop {
            let current_head = entry_ref.head.load(Ordering::Acquire, guard);
            unsafe {
                (*node_ptr).next.store(current_head, Ordering::Relaxed);
            }

            let next_head = Shared::from(node_ptr as *const VersionNode<V>);
            match entry_ref.head.compare_exchange(
                current_head,
                next_head,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return node_ptr,
                Err(_) => continue,
            }
        }
    }

    #[inline]
    pub fn head<'g>(&self, key: &K, guard: &'g Guard) -> Option<Shared<'g, VersionNode<V>>> {
        let entry = self.find_entry(key, guard)?;
        let entry_ref = unsafe { entry.as_ref() }?;
        let head = entry_ref.head.load(Ordering::Acquire, guard);
        if head.is_null() {
            None
        } else {
            Some(head)
        }
    }

    #[inline]
    pub fn arena_len(&self) -> usize {
        self.version_arena.len()
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

unsafe impl<K: Send, V: Send, S: Send> Send for Table<K, V, S> {}
unsafe impl<K: Send + Sync, V: Send + Sync, S: Send + Sync> Sync for Table<K, V, S> {}

impl<K, V, S> Drop for Table<K, V, S> {
    fn drop(&mut self) {
        unsafe {
            let guard = epoch::unprotected();
            for bucket in self.buckets.iter() {
                let mut current = bucket.load(Ordering::Relaxed, guard);
                while let Some(entry) = current.as_ref() {
                    let next = entry.next.load(Ordering::Relaxed, guard);
                    drop(Box::from_raw(current.as_raw() as *mut BucketEntry<K, V>));
                    current = next;
                }
            }
        }
    }
}
