use std::fmt;
use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::occ_partitioned::{Error as OccError, OccTable, VacuumReclaimedRow};
use crate::{ShmArena, TxId};

const DEFAULT_VACUUM_INTERVAL: Duration = Duration::from_secs(1);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub type VacuumReclaimCallback<T> = Arc<dyn Fn(&[VacuumReclaimedRow<T>]) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct VacuumDaemonConfig<T: Copy + Send + Sync + 'static> {
    pub interval: Duration,
    pub reclaim_callback: Option<VacuumReclaimCallback<T>>,
}

impl<T: Copy + Send + Sync + 'static> Default for VacuumDaemonConfig<T> {
    fn default() -> Self {
        Self {
            interval: DEFAULT_VACUUM_INTERVAL,
            reclaim_callback: None,
        }
    }
}

impl<T: Copy + Send + Sync + 'static> VacuumDaemonConfig<T> {
    #[inline]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    #[inline]
    pub fn with_reclaim_callback(mut self, callback: VacuumReclaimCallback<T>) -> Self {
        self.reclaim_callback = Some(callback);
        self
    }
}

#[derive(Debug)]
pub enum VacuumError {
    Occ(OccError),
    Spawn(std::io::Error),
    Join(String),
}

impl fmt::Display for VacuumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VacuumError::Occ(err) => write!(f, "vacuum OCC error: {}", err),
            VacuumError::Spawn(err) => write!(f, "failed to spawn vacuum daemon thread: {}", err),
            VacuumError::Join(msg) => write!(f, "vacuum daemon thread join failed: {}", msg),
        }
    }
}

impl std::error::Error for VacuumError {}

impl From<OccError> for VacuumError {
    fn from(value: OccError) -> Self {
        VacuumError::Occ(value)
    }
}

pub struct VacuumDaemon<T: Copy + Send + Sync + 'static> {
    owns_leader_slot: bool,
    leader_pid: i32,
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    _marker: PhantomData<T>,
}

impl<T: Copy + Send + Sync + 'static> VacuumDaemon<T> {
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.owns_leader_slot
    }

    #[inline]
    pub fn leader_pid(&self) -> i32 {
        self.leader_pid
    }

    pub fn stop(&self) -> Result<(), VacuumError> {
        if !self.owns_leader_slot {
            return Ok(());
        }

        self.stop.store(true, Ordering::Release);
        let handle = self
            .thread
            .lock()
            .expect("vacuum daemon thread lock poisoned")
            .take();
        if let Some(handle) = handle {
            match handle.join() {
                Ok(()) => Ok(()),
                Err(payload) => {
                    let message = if let Some(msg) = payload.downcast_ref::<String>() {
                        msg.clone()
                    } else if let Some(msg) = payload.downcast_ref::<&'static str>() {
                        msg.to_string()
                    } else {
                        "unknown panic payload".to_string()
                    };
                    Err(VacuumError::Join(message))
                }
            }
        } else {
            Ok(())
        }
    }
}

impl<T: Copy + Send + Sync + 'static> Drop for VacuumDaemon<T> {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub fn spawn_vacuum_daemon<T>(table: Arc<OccTable<T>>) -> Result<VacuumDaemon<T>, VacuumError>
where
    T: Copy + Send + Sync + 'static,
{
    spawn_vacuum_daemon_with_config(table, VacuumDaemonConfig::default())
}

pub fn spawn_vacuum_daemon_with_callback<T>(
    table: Arc<OccTable<T>>,
    callback: VacuumReclaimCallback<T>,
) -> Result<VacuumDaemon<T>, VacuumError>
where
    T: Copy + Send + Sync + 'static,
{
    spawn_vacuum_daemon_with_config(
        table,
        VacuumDaemonConfig::default().with_reclaim_callback(callback),
    )
}

pub fn spawn_vacuum_daemon_with_config<T>(
    table: Arc<OccTable<T>>,
    mut config: VacuumDaemonConfig<T>,
) -> Result<VacuumDaemon<T>, VacuumError>
where
    T: Copy + Send + Sync + 'static,
{
    if config.interval.is_zero() {
        config.interval = DEFAULT_VACUUM_INTERVAL;
    }

    let shm = Arc::clone(table.shared_arena());
    let this_pid = current_pid();

    loop {
        let observed = shm.vacuum_daemon_pid();
        if observed == this_pid && observed != 0 {
            return Ok(VacuumDaemon {
                owns_leader_slot: false,
                leader_pid: observed,
                stop: Arc::new(AtomicBool::new(true)),
                thread: Mutex::new(None),
                _marker: PhantomData,
            });
        }

        if observed == 0 {
            match shm.compare_exchange_vacuum_daemon_pid(0, this_pid) {
                Ok(_) => {
                    let stop = Arc::new(AtomicBool::new(false));
                    let stop_signal = Arc::clone(&stop);
                    let table_clone = Arc::clone(&table);
                    let shm_clone = Arc::clone(&shm);
                    let interval = config.interval;
                    let callback = config.reclaim_callback.clone();
                    let thread = std::thread::Builder::new()
                        .name("aerostore-vacuum".to_string())
                        .spawn(move || {
                            vacuum_loop(table_clone, interval, callback, Arc::clone(&stop_signal));
                            clear_leader_if_current(shm_clone.as_ref(), this_pid);
                        })
                        .map_err(VacuumError::Spawn)?;

                    return Ok(VacuumDaemon {
                        owns_leader_slot: true,
                        leader_pid: this_pid,
                        stop,
                        thread: Mutex::new(Some(thread)),
                        _marker: PhantomData,
                    });
                }
                Err(_) => continue,
            }
        }

        if is_process_alive(observed) {
            return Ok(VacuumDaemon {
                owns_leader_slot: false,
                leader_pid: observed,
                stop: Arc::new(AtomicBool::new(true)),
                thread: Mutex::new(None),
                _marker: PhantomData,
            });
        }

        let _ = shm.compare_exchange_vacuum_daemon_pid(observed, 0);
    }
}

pub fn run_vacuum_pass<T>(table: &OccTable<T>) -> Result<Vec<VacuumReclaimedRow<T>>, VacuumError>
where
    T: Copy + Send + Sync + 'static,
{
    let global_xmin = compute_global_xmin(table.shared_arena().as_ref());
    table
        .vacuum_reclaim_once(global_xmin)
        .map_err(VacuumError::Occ)
}

pub fn compute_global_xmin(shm: &ShmArena) -> TxId {
    let mut xmin = shm.global_txid().load(Ordering::Acquire);
    let proc_array = shm.proc_array();
    for slot_idx in 0..proc_array.slots_len() {
        let txid = proc_array.slot_txid(slot_idx).unwrap_or(0);
        if txid != 0 && txid < xmin {
            xmin = txid;
        }
    }
    xmin
}

fn vacuum_loop<T>(
    table: Arc<OccTable<T>>,
    interval: Duration,
    callback: Option<VacuumReclaimCallback<T>>,
    stop: Arc<AtomicBool>,
) where
    T: Copy + Send + Sync + 'static,
{
    while !stop.load(Ordering::Acquire) {
        if let Ok(reclaimed) = run_vacuum_pass(table.as_ref()) {
            if !reclaimed.is_empty() {
                if let Some(cb) = callback.as_ref() {
                    let _ = panic::catch_unwind(AssertUnwindSafe(|| cb(reclaimed.as_slice())));
                }
            }
        }
        sleep_interruptible(interval, stop.as_ref());
    }
}

fn sleep_interruptible(interval: Duration, stop: &AtomicBool) {
    if interval <= STOP_POLL_INTERVAL {
        std::thread::sleep(interval);
        return;
    }

    let mut remaining = interval;
    while remaining > Duration::ZERO {
        if stop.load(Ordering::Acquire) {
            break;
        }

        let slice = remaining.min(STOP_POLL_INTERVAL);
        std::thread::sleep(slice);
        remaining = remaining.saturating_sub(slice);
    }
}

#[inline]
fn clear_leader_if_current(shm: &ShmArena, expected_pid: i32) {
    let _ = shm.compare_exchange_vacuum_daemon_pid(expected_pid, 0);
}

#[inline]
fn current_pid() -> i32 {
    // SAFETY:
    // process id lookup has no side effects.
    unsafe { libc::getpid() as i32 }
}

#[inline]
fn is_process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }

    // SAFETY:
    // signal 0 performs liveness check without delivering a signal.
    let rc = unsafe { libc::kill(pid, 0) };
    if rc == 0 {
        true
    } else {
        let err = std::io::Error::last_os_error();
        matches!(err.raw_os_error(), Some(libc::EPERM))
    }
}
