//! A process-shared mutation lock. Its state contains no process-local pointers.
//!
//! The same acquire/release protocol is exercised with Loom's atomics under
//! `--cfg aerostore_loom`; do not replace this with a process-local `std::sync::Mutex`.

#[cfg(aerostore_loom)]
use loom::sync::atomic::{AtomicU32, Ordering};
#[cfg(not(aerostore_loom))]
use std::sync::atomic::{AtomicU32, Ordering};

#[repr(C)]
pub(crate) struct ShmMutex {
    state: AtomicU32,
    priority_waiters: AtomicU32,
}

impl ShmMutex {
    #[cfg(not(aerostore_loom))]
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
            priority_waiters: AtomicU32::new(0),
        }
    }

    #[cfg(aerostore_loom)]
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
            priority_waiters: AtomicU32::new(0),
        }
    }

    #[inline]
    pub(crate) fn try_lock(&self) -> Option<ShmMutexGuard<'_>> {
        if self.priority_waiters.load(Ordering::Acquire) != 0 {
            return None;
        }
        let guard = self.try_acquire()?;
        // Close the registration race: a foreground contender that observed
        // an empty queue before GC registered must yield its acquired turn.
        if self.priority_waiters.load(Ordering::Acquire) != 0 {
            drop(guard);
            return None;
        }
        Some(guard)
    }

    fn try_acquire(&self) -> Option<ShmMutexGuard<'_>> {
        self.state
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| ShmMutexGuard {
                mutex: self,
                _not_send: std::marker::PhantomData,
            })
    }

    pub(crate) fn lock(&self) -> ShmMutexGuard<'_> {
        self.acquire(false)
    }

    /// Give reclamation precedence over new foreground operations. Ordinary
    /// operations retain the inexpensive uncontended CAS path; making every
    /// operation FIFO forces process handoffs for microsecond critical sections.
    pub(crate) fn lock_priority(&self) -> ShmMutexGuard<'_> {
        self.priority_waiters.fetch_add(1, Ordering::AcqRel);
        let guard = self.acquire(true);
        self.priority_waiters.fetch_sub(1, Ordering::Release);
        guard
    }

    /// Reset abandoned metadata only after the caller has excluded all prior
    /// users. Never use this to unlock a live contender or ordinary contention.
    pub(crate) fn reset_after_exclusive_recovery(&self) {
        self.priority_waiters.store(0, Ordering::Release);
        self.state.store(0, Ordering::Release);
    }

    fn acquire(&self, priority: bool) -> ShmMutexGuard<'_> {
        let mut attempts = 0_u32;
        loop {
            if let Some(guard) = if priority {
                self.try_acquire()
            } else {
                self.try_lock()
            } {
                return guard;
            }
            attempts = attempts.saturating_add(1);
            #[cfg(aerostore_loom)]
            loom::thread::yield_now();
            #[cfg(not(aerostore_loom))]
            if attempts < 16 {
                std::hint::spin_loop();
            } else if attempts < 256 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(std::time::Duration::from_micros(25));
            }
        }
    }
}

pub(crate) struct ShmMutexGuard<'a> {
    mutex: &'a ShmMutex,
    _not_send: std::marker::PhantomData<*mut ()>,
}

impl Drop for ShmMutexGuard<'_> {
    fn drop(&mut self) {
        self.mutex.state.store(0, Ordering::Release);
    }
}
