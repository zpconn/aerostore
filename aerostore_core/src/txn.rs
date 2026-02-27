use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

pub type TxId = u64;

static GLOBAL_TXID: AtomicU64 = AtomicU64::new(1);

struct ActiveTxnNode {
    txid: TxId,
    active: AtomicBool,
    snapshot_xmin: AtomicU64,
    next: AtomicPtr<ActiveTxnNode>,
}

impl ActiveTxnNode {
    #[inline]
    fn new(txid: TxId) -> Self {
        Self {
            txid,
            active: AtomicBool::new(true),
            snapshot_xmin: AtomicU64::new(txid),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub xmin: TxId,
    pub xmax: TxId,
    active: HashSet<TxId>,
}

impl Snapshot {
    #[inline]
    pub fn is_active(&self, txid: TxId) -> bool {
        self.active.contains(&txid)
    }

    #[inline]
    pub fn active_txids(&self) -> &HashSet<TxId> {
        &self.active
    }
}

#[derive(Debug)]
pub struct Transaction {
    pub txid: TxId,
    pub snapshot: Snapshot,
    node: *mut ActiveTxnNode,
}

unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

pub struct TransactionManager {
    active_head: AtomicPtr<ActiveTxnNode>,
    begin_started: AtomicU64,
    begin_completed: AtomicU64,
}

impl TransactionManager {
    #[inline]
    pub fn new() -> Self {
        Self {
            active_head: AtomicPtr::new(std::ptr::null_mut()),
            begin_started: AtomicU64::new(0),
            begin_completed: AtomicU64::new(0),
        }
    }

    pub fn begin(&self) -> Transaction {
        let txid = GLOBAL_TXID.fetch_add(1, Ordering::AcqRel);
        self.begin_with_assigned_txid(txid)
    }

    pub fn begin_with_txid(&self, txid: TxId) -> Transaction {
        let mut current = GLOBAL_TXID.load(Ordering::Acquire);
        while current <= txid {
            match GLOBAL_TXID.compare_exchange(
                current,
                txid.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }

        self.begin_with_assigned_txid(txid)
    }

    fn begin_with_assigned_txid(&self, txid: TxId) -> Transaction {
        self.begin_started.fetch_add(1, Ordering::AcqRel);
        let node_ptr = Box::into_raw(Box::new(ActiveTxnNode::new(txid)));
        self.push_active(node_ptr);
        self.begin_completed.fetch_add(1, Ordering::AcqRel);

        let snapshot = self.snapshot_for(txid);
        unsafe {
            (*node_ptr)
                .snapshot_xmin
                .store(snapshot.xmin, Ordering::Release);
        }

        Transaction {
            txid,
            snapshot,
            node: node_ptr,
        }
    }

    pub fn snapshot_for(&self, current_txid: TxId) -> Snapshot {
        loop {
            let began_before = self.begin_started.load(Ordering::Acquire);
            let completed_before = self.begin_completed.load(Ordering::Acquire);
            if began_before != completed_before {
                std::hint::spin_loop();
                continue;
            }

            let xmax = GLOBAL_TXID.load(Ordering::Acquire);
            let mut xmin = xmax;
            let mut active = HashSet::new();

            let mut current = self.active_head.load(Ordering::Acquire);
            while !current.is_null() {
                let node = unsafe { &*current };
                if node.active.load(Ordering::Acquire) {
                    xmin = xmin.min(node.txid);
                    if node.txid != current_txid {
                        active.insert(node.txid);
                    }
                }
                current = node.next.load(Ordering::Acquire);
            }

            let began_after = self.begin_started.load(Ordering::Acquire);
            let completed_after = self.begin_completed.load(Ordering::Acquire);
            if began_before == began_after && completed_before == completed_after {
                if xmin == xmax {
                    xmin = current_txid;
                }
                return Snapshot { xmin, xmax, active };
            }
        }
    }

    #[inline]
    pub fn commit(&self, tx: &Transaction) {
        self.finish(tx);
    }

    #[inline]
    pub fn abort(&self, tx: &Transaction) {
        self.finish(tx);
    }

    pub fn oldest_active_txid(&self) -> TxId {
        let mut oldest = GLOBAL_TXID.load(Ordering::Acquire);
        let mut current = self.active_head.load(Ordering::Acquire);

        while !current.is_null() {
            let node = unsafe { &*current };
            if node.active.load(Ordering::Acquire) {
                oldest = oldest.min(node.txid);
            }
            current = node.next.load(Ordering::Acquire);
        }

        oldest
    }

    pub fn oldest_active_snapshot_xmin(&self) -> TxId {
        let mut oldest = GLOBAL_TXID.load(Ordering::Acquire);
        let mut current = self.active_head.load(Ordering::Acquire);

        while !current.is_null() {
            let node = unsafe { &*current };
            if node.active.load(Ordering::Acquire) {
                let snapshot_xmin = node.snapshot_xmin.load(Ordering::Acquire);
                oldest = oldest.min(snapshot_xmin);
            }
            current = node.next.load(Ordering::Acquire);
        }

        oldest
    }

    fn push_active(&self, node_ptr: *mut ActiveTxnNode) {
        loop {
            let head = self.active_head.load(Ordering::Acquire);
            unsafe {
                (*node_ptr).next.store(head, Ordering::Relaxed);
            }

            if self
                .active_head
                .compare_exchange(head, node_ptr, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    #[inline]
    fn finish(&self, tx: &Transaction) {
        if tx.node.is_null() {
            return;
        }

        unsafe {
            (*tx.node).active.store(false, Ordering::Release);
        }
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TransactionManager {
    fn drop(&mut self) {
        let mut current = self.active_head.load(Ordering::Relaxed);
        while !current.is_null() {
            unsafe {
                let boxed = Box::from_raw(current);
                current = boxed.next.load(Ordering::Relaxed);
            }
        }
    }
}
