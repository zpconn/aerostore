//! Experimental database-owned execution. Application handlers remain clients;
//! every Store operation and transaction resource belongs to a service executor.
//! Commit outcomes are volatile and bounded, not a durable exactly-once log.
use super::storage::{DbError, Query, Record, Store};
use crate::extended_crucible::metrics::StoreMetrics;
use bincode::Options;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const VERSION: u32 = 1;
const FRAME_BYTES: usize = 8 << 20;

fn connect_unix(path: &std::path::Path, timeout: Duration) -> Result<UnixStream, String> {
    let bytes = path.as_os_str().as_bytes();
    // SAFETY: zero is a valid initialization of this plain C socket address.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.len() >= address.sun_path.len() || bytes.contains(&0) {
        return Err("Unix socket path is too long or contains NUL".into());
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (to, from) in address.sun_path.iter_mut().zip(bytes) {
        *to = *from as libc::c_char;
    }
    // SAFETY: socket has no borrowed buffers; the returned descriptor is adopted
    // exactly once by OwnedFd. Nonblocking connect keeps a full backlog bounded.
    let raw = unsafe {
        libc::socket(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if raw < 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    let descriptor = unsafe { OwnedFd::from_raw_fd(raw) };
    let length =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1) as libc::socklen_t;
    let deadline = Instant::now() + timeout;
    loop {
        // SAFETY: address is initialized and length is within sockaddr_un.
        let result = unsafe {
            libc::connect(
                descriptor.as_raw_fd(),
                (&address as *const libc::sockaddr_un).cast(),
                length,
            )
        };
        if result == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EISCONN) {
            break;
        }
        if error.raw_os_error() == Some(libc::EAGAIN) {
            if Instant::now() >= deadline {
                return Err("Unix connect deadline".into());
            }
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        if !matches!(
            error.raw_os_error(),
            Some(libc::EINPROGRESS | libc::EALREADY | libc::EINTR)
        ) {
            return Err(error.to_string());
        }
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or("Unix connect deadline")?;
            let mut poll = libc::pollfd {
                fd: descriptor.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            };
            // SAFETY: poll points to one live pollfd; timeout is finite.
            let result = unsafe {
                libc::poll(
                    &mut poll,
                    1,
                    remaining.as_millis().clamp(1, i32::MAX as u128) as i32,
                )
            };
            if result == 0 {
                return Err("Unix connect deadline".into());
            }
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error.to_string());
            }
            let mut code: libc::c_int = 0;
            let mut len = std::mem::size_of_val(&code) as libc::socklen_t;
            // SAFETY: SO_ERROR writes an integer to the supplied live buffer.
            let result = unsafe {
                libc::getsockopt(
                    descriptor.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_ERROR,
                    (&mut code as *mut libc::c_int).cast(),
                    &mut len,
                )
            };
            if result != 0 {
                return Err(io::Error::last_os_error().to_string());
            }
            if code != 0 {
                return Err(io::Error::from_raw_os_error(code).to_string());
            }
            break;
        }
        break;
    }
    let stream = UnixStream::from(descriptor);
    stream.set_nonblocking(false).map_err(|e| e.to_string())?;
    Ok(stream)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Endpoint {
    Unix(PathBuf),
    Tcp(SocketAddr),
}
pub type BindEndpoint = Endpoint;

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_sessions: usize,
    pub idle_timeout: Duration,
    pub transaction_timeout: Duration,
    pub max_transaction_operations: u64,
    pub outcome_retention: Duration,
    pub max_outcomes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_sessions: 128,
            idle_timeout: Duration::from_secs(10),
            transaction_timeout: Duration::from_secs(60),
            max_transaction_operations: 100_000,
            outcome_retention: Duration::from_secs(60),
            max_outcomes: 4096,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SessionId {
    epoch: [u8; 16],
    number: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CommitToken {
    pub session: SessionId,
    pub sequence: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CommitOutcome {
    Pending,
    Committed,
    AbortedConflict,
    Indeterminate(String),
    /// Expired, acknowledged, never accepted, or another service incarnation.
    /// This must never be interpreted as permission to replay a transaction.
    Unknown,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BackendMetrics {
    pub metrics: StoreMetrics,
    pub retry_causes: BTreeMap<String, u64>,
    pub diagnostics: BTreeMap<String, u64>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ServerStats {
    pub accepted_sessions: u64,
    pub active_sessions: usize,
    pub completed_sessions: u64,
    pub rejected_sessions: u64,
    pub disconnect_aborts: u64,
    pub protocol_errors: u64,
    pub backend_failures: Vec<String>,
    pub retained_outcomes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum WireError {
    Conflict,
    Fatal(String),
}
impl From<DbError> for WireError {
    fn from(e: DbError) -> Self {
        match e {
            DbError::Conflict => Self::Conflict,
            DbError::Fatal(s) => Self::Fatal(s),
        }
    }
}
impl From<WireError> for DbError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::Conflict => Self::Conflict,
            WireError::Fatal(s) => Self::Fatal(s),
        }
    }
}
#[derive(Serialize, Deserialize)]
struct Welcome {
    version: u32,
    session: SessionId,
}
#[derive(Serialize, Deserialize)]
enum Operation {
    Begin(Vec<usize>),
    Read(usize),
    Query(Query),
    Write(Record),
    Savepoint,
    Rollback(usize),
    Commit,
    Abort,
    Metrics,
    Resolve(CommitToken),
    Close,
}
#[derive(Serialize, Deserialize)]
struct Request {
    session: SessionId,
    sequence: u64,
    acknowledge: Option<CommitToken>,
    operation: Operation,
}
#[derive(Serialize, Deserialize)]
enum Value {
    Unit,
    Row(Record),
    Rows(Vec<Record>),
    Savepoint(usize),
    Outcome(CommitOutcome),
}
#[derive(Serialize, Deserialize)]
struct Reply {
    session: SessionId,
    sequence: u64,
    result: Result<Value, WireError>,
    metrics: Option<BackendMetrics>,
}

enum Socket {
    Unix(UnixStream),
    Tcp(TcpStream),
}
impl Socket {
    fn clone_socket(&self) -> io::Result<Self> {
        match self {
            Self::Unix(s) => s.try_clone().map(Self::Unix),
            Self::Tcp(s) => s.try_clone().map(Self::Tcp),
        }
    }
    fn timeout(&self, time: Duration, write: bool) -> io::Result<()> {
        let time = Some(time.max(Duration::from_micros(1)));
        match (self, write) {
            (Self::Unix(s), false) => s.set_read_timeout(time),
            (Self::Unix(s), true) => s.set_write_timeout(time),
            (Self::Tcp(s), false) => s.set_read_timeout(time),
            (Self::Tcp(s), true) => s.set_write_timeout(time),
        }
    }
    fn shutdown(&self) {
        let _ = match self {
            Self::Unix(s) => s.shutdown(Shutdown::Both),
            Self::Tcp(s) => s.shutdown(Shutdown::Both),
        };
    }
}
impl Read for Socket {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Unix(s) => s.read(b),
            Self::Tcp(s) => s.read(b),
        }
    }
}
impl Write for Socket {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        match self {
            Self::Unix(s) => s.write(b),
            Self::Tcp(s) => s.write(b),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn deadline_io(socket: &mut Socket, bytes: &mut [u8], deadline: Instant) -> io::Result<()> {
    let mut used = 0;
    while used < bytes.len() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "frame read deadline"))?;
        socket.timeout(remaining, false)?;
        match socket.read(&mut bytes[used..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "peer closed frame",
                ))
            }
            Ok(n) => used += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => (),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
fn read_frame<T: DeserializeOwned>(socket: &mut Socket, deadline: Instant) -> Result<T, String> {
    let mut length = [0; 4];
    deadline_io(socket, &mut length, deadline).map_err(|e| e.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > FRAME_BYTES {
        return Err("invalid or oversized frame".into());
    }
    let mut bytes = vec![0; length];
    deadline_io(socket, &mut bytes, deadline).map_err(|e| e.to_string())?;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(FRAME_BYTES as u64)
        .reject_trailing_bytes()
        .deserialize(&bytes)
        .map_err(|e| e.to_string())
}
fn write_frame<T: Serialize>(
    socket: &mut Socket,
    value: &T,
    deadline: Instant,
) -> Result<(), String> {
    let bytes = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(FRAME_BYTES as u64)
        .serialize(value)
        .map_err(|e| e.to_string())?;
    if bytes.is_empty() || bytes.len() > FRAME_BYTES {
        return Err("oversized outgoing frame".into());
    }
    let prefix = (bytes.len() as u32).to_be_bytes();
    for part in [&prefix[..], &bytes[..]] {
        let mut used = 0;
        while used < part.len() {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or("frame write deadline")?;
            socket.timeout(remaining, true).map_err(|e| e.to_string())?;
            match socket.write(&part[used..]) {
                Ok(0) => return Err("peer closed frame write".into()),
                Ok(n) => used += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => (),
                Err(e) => return Err(e.to_string()),
            }
        }
    }
    Ok(())
}

struct OutcomeEntry {
    outcome: CommitOutcome,
    finished: Option<Instant>,
}
struct State {
    stats: ServerStats,
    sockets: BTreeMap<u64, Socket>,
    outcomes: BTreeMap<CommitToken, OutcomeEntry>,
}
struct Control {
    epoch: [u8; 16],
    limits: Limits,
    stop: AtomicBool,
    next_session: AtomicU64,
    state: Mutex<State>,
    observer: Option<Observer>,
}
impl Control {
    fn fail_locked(&self, state: &mut State, error: String) {
        state.stats.backend_failures.push(error);
        self.stop.store(true, Ordering::Release);
        // Wake existing idle executors as well as stopping new admission. An
        // already executing native operation still has to finish or be bounded
        // by terminating the disposable owner process.
        for socket in state.sockets.values() {
            socket.shutdown();
        }
    }
    fn prune(&self, state: &mut State) {
        state.outcomes.retain(|_, entry| {
            entry
                .finished
                .map_or(true, |t| t.elapsed() < self.limits.outcome_retention)
        });
    }
    fn event(&self, session: SessionId, sequence: u64, stage: Stage) {
        if let Some(observer) = &self.observer {
            observer(Event {
                session,
                sequence,
                stage,
            });
        }
    }
}
/// Diagnostic hooks run inside the executor, and must themselves be bounded.
/// They are absent from measured runs; they do not instrument engine internals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stage {
    RequestAccepted,
    QueryReturned,
    WriteReturned,
    CommitAccepted,
    CommitRecorded,
}
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub session: SessionId,
    pub sequence: u64,
    pub stage: Stage,
}
pub type Observer = Arc<dyn Fn(Event) + Send + Sync>;

enum Listener {
    Unix(UnixListener),
    Tcp(TcpListener),
}
impl Listener {
    fn accept(&self) -> io::Result<Socket> {
        match self {
            Self::Unix(s) => s.accept().map(|(s, _)| Socket::Unix(s)),
            Self::Tcp(s) => s.accept().and_then(|(s, _)| {
                s.set_nodelay(true)?;
                Ok(Socket::Tcp(s))
            }),
        }
    }
}
pub struct Server {
    endpoint: Endpoint,
    unix_identity: Option<(u64, u64)>,
    control: Arc<Control>,
    listener: Option<JoinHandle<()>>,
    executors: Arc<Mutex<Vec<JoinHandle<()>>>>,
}
impl Server {
    pub fn start<F>(bind: BindEndpoint, limits: Limits, factory: F) -> Result<Self, String>
    where
        F: Fn(Session) -> Result<(), String> + Send + Sync + 'static,
    {
        Self::start_observed(bind, limits, factory, None)
    }
    pub fn start_observed<F>(
        bind: BindEndpoint,
        limits: Limits,
        factory: F,
        observer: Option<Observer>,
    ) -> Result<Self, String>
    where
        F: Fn(Session) -> Result<(), String> + Send + Sync + 'static,
    {
        if limits.max_sessions == 0
            || limits.max_outcomes == 0
            || limits.idle_timeout.is_zero()
            || limits.transaction_timeout.is_zero()
            || limits.outcome_retention.is_zero()
            || limits.max_transaction_operations == 0
        {
            return Err("service limits must be positive".into());
        }
        let (listener, endpoint, unix_identity) = match bind {
            Endpoint::Unix(path) => {
                let listener = UnixListener::bind(&path).map_err(|e| e.to_string())?;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                    .map_err(|e| e.to_string())?;
                let meta = std::fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
                listener.set_nonblocking(true).map_err(|e| e.to_string())?;
                (
                    Listener::Unix(listener),
                    Endpoint::Unix(path),
                    Some((meta.dev(), meta.ino())),
                )
            }
            Endpoint::Tcp(address) => {
                let listener = TcpListener::bind(address).map_err(|e| e.to_string())?;
                let actual = listener.local_addr().map_err(|e| e.to_string())?;
                listener.set_nonblocking(true).map_err(|e| e.to_string())?;
                (Listener::Tcp(listener), Endpoint::Tcp(actual), None)
            }
        };
        let mut epoch = [0; 16];
        File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut epoch))
            .map_err(|e| e.to_string())?;
        let control = Arc::new(Control {
            epoch,
            limits,
            stop: AtomicBool::new(false),
            next_session: AtomicU64::new(1),
            state: Mutex::new(State {
                stats: ServerStats::default(),
                sockets: BTreeMap::new(),
                outcomes: BTreeMap::new(),
            }),
            observer,
        });
        let executors = Arc::new(Mutex::new(Vec::new()));
        let worker_control = Arc::clone(&control);
        let worker_executors = Arc::clone(&executors);
        let factory = Arc::new(factory);
        let listener = thread::Builder::new()
            .name("store-service-listener".into())
            .spawn(move || {
                while !worker_control.stop.load(Ordering::Acquire) {
                    // Reap finished executors during service life, not just shutdown.
                    worker_executors
                        .lock()
                        .unwrap()
                        .retain(|h: &JoinHandle<()>| !h.is_finished());
                    let socket = match listener.accept() {
                        Ok(s) => s,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(e) => {
                            let mut state = worker_control.state.lock().unwrap();
                            worker_control.fail_locked(&mut state, format!("listener: {e}"));
                            break;
                        }
                    };
                    let number = match worker_control.next_session.fetch_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |number| number.checked_add(1),
                    ) {
                        Ok(number) => number,
                        Err(_) => {
                            let mut state = worker_control.state.lock().unwrap();
                            state.stats.rejected_sessions += 1;
                            worker_control.fail_locked(
                                &mut state,
                                "session identifier space exhausted".into(),
                            );
                            socket.shutdown();
                            break;
                        }
                    };
                    let mut state = worker_control.state.lock().unwrap();
                    if state.sockets.len() >= worker_control.limits.max_sessions
                        || worker_control.stop.load(Ordering::Acquire)
                    {
                        state.stats.rejected_sessions += 1;
                        socket.shutdown();
                        continue;
                    }
                    let cloned = match socket.clone_socket() {
                        Ok(s) => s,
                        Err(_) => {
                            state.stats.rejected_sessions += 1;
                            continue;
                        }
                    };
                    state.sockets.insert(number, cloned);
                    state.stats.accepted_sessions += 1;
                    drop(state);
                    let control = Arc::clone(&worker_control);
                    let factory = Arc::clone(&factory);
                    let spawned = thread::Builder::new()
                        .name(format!("store-session-{number}"))
                        .stack_size(8 << 20)
                        .spawn(move || {
                            let session = Session {
                                id: SessionId {
                                    epoch: control.epoch,
                                    number,
                                },
                                socket,
                                control: Arc::clone(&control),
                            };
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    factory(session)
                                }));
                            let mut state = control.state.lock().unwrap();
                            state.sockets.remove(&number);
                            state.stats.completed_sessions += 1;
                            let error = match result {
                                Ok(Ok(())) => None,
                                Ok(Err(e)) => Some(e),
                                Err(_) => {
                                    Some("executor panicked; engine health is unknown".into())
                                }
                            };
                            if let Some(error) = error {
                                control.fail_locked(&mut state, error);
                            }
                        });
                    match spawned {
                        Ok(handle) => worker_executors.lock().unwrap().push(handle),
                        Err(e) => {
                            let mut state = worker_control.state.lock().unwrap();
                            state.sockets.remove(&number);
                            worker_control.fail_locked(&mut state, format!("spawn: {e}"));
                        }
                    }
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            endpoint,
            unix_identity,
            control,
            listener: Some(listener),
            executors,
        })
    }
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }
    pub fn stats(&self) -> ServerStats {
        let mut state = self.control.state.lock().unwrap();
        self.control.prune(&mut state);
        let mut stats = state.stats.clone();
        stats.active_sessions = state.sockets.len();
        stats.retained_outcomes = state.outcomes.len();
        stats
    }
    pub fn stop(&mut self, timeout: Duration) -> Result<ServerStats, String> {
        self.control.stop.store(true, Ordering::Release);
        for socket in self.control.state.lock().unwrap().sockets.values() {
            socket.shutdown();
        }
        let deadline = Instant::now() + timeout;
        loop {
            let listener_done = self.listener.as_ref().map_or(true, |h| h.is_finished());
            let executors_done = self
                .executors
                .lock()
                .unwrap()
                .iter()
                .all(JoinHandle::is_finished);
            if listener_done && executors_done {
                break;
            }
            if Instant::now() >= deadline {
                return Err("service shutdown deadline; an engine operation may still be executing; terminate the isolated owner process".into());
            }
            thread::sleep(Duration::from_millis(1));
        }
        if let Some(handle) = self.listener.take() {
            handle.join().map_err(|_| "listener panicked")?;
        }
        for handle in self.executors.lock().unwrap().drain(..) {
            handle.join().map_err(|_| "executor panicked")?;
        }
        self.remove_socket();
        let stats = self.stats();
        if !stats.backend_failures.is_empty() {
            return Err(format!(
                "service backend failure: {:?}",
                stats.backend_failures
            ));
        }
        Ok(stats)
    }
    fn remove_socket(&mut self) {
        if let (Endpoint::Unix(path), Some(identity)) = (&self.endpoint, self.unix_identity.take())
        {
            if std::fs::symlink_metadata(path)
                .map(|m| (m.dev(), m.ino()) == identity)
                .unwrap_or(false)
            {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.control.stop.store(true, Ordering::Release);
        for socket in self.control.state.lock().unwrap().sockets.values() {
            socket.shutdown();
        }
        self.remove_socket();
    }
}

pub struct Session {
    id: SessionId,
    socket: Socket,
    control: Arc<Control>,
}
impl Session {
    pub fn serve<S: Store, M: Fn(&S) -> BackendMetrics>(
        mut self,
        store: &mut S,
        metrics: M,
    ) -> Result<(), String> {
        let welcome = Welcome {
            version: VERSION,
            session: self.id,
        };
        if write_frame(
            &mut self.socket,
            &welcome,
            Instant::now() + self.control.limits.idle_timeout,
        )
        .is_err()
        {
            return Ok(());
        }
        let mut expected = 1_u64;
        let mut transaction: Option<Instant> = None;
        let mut operations = 0_u64;
        loop {
            if self.control.stop.load(Ordering::Acquire) {
                break;
            }
            let idle_deadline = Instant::now() + self.control.limits.idle_timeout;
            let deadline = transaction.map_or(idle_deadline, |started| {
                idle_deadline.min(started + self.control.limits.transaction_timeout)
            });
            let request: Request = match read_frame(&mut self.socket, deadline) {
                Ok(request) => request,
                Err(_) => break,
            };
            if self.control.stop.load(Ordering::Acquire) {
                break;
            }
            if request.session != self.id || request.sequence != expected || expected == u64::MAX {
                self.control.state.lock().unwrap().stats.protocol_errors += 1;
                break;
            }
            expected += 1;
            if let Some(token) = request.acknowledge {
                if token.session != self.id {
                    self.control.state.lock().unwrap().stats.protocol_errors += 1;
                    break;
                }
                let mut state = self.control.state.lock().unwrap();
                if state
                    .outcomes
                    .get(&token)
                    .is_some_and(|e| e.finished.is_some())
                {
                    state.outcomes.remove(&token);
                }
            }
            self.control
                .event(self.id, request.sequence, Stage::RequestAccepted);
            operations += 1;
            if transaction.is_some_and(|t| t.elapsed() >= self.control.limits.transaction_timeout)
                || (transaction.is_some()
                    && operations > self.control.limits.max_transaction_operations)
            {
                break;
            }
            let mut include_metrics = false;
            let mut close = false;
            let result: Result<Value, DbError> = match request.operation {
                Operation::Begin(ids) => {
                    if transaction.is_some() {
                        Err(DbError::Fatal("nested transaction".into()))
                    } else {
                        store.begin(&ids).map(|()| {
                            transaction = Some(Instant::now());
                            operations = 0;
                            Value::Unit
                        })
                    }
                }
                Operation::Read(id) => store.read(id).map(Value::Row),
                Operation::Query(query) => {
                    let result = store.query(&query).map(Value::Rows);
                    self.control
                        .event(self.id, request.sequence, Stage::QueryReturned);
                    result
                }
                Operation::Write(row) => {
                    let result = store.write(row).map(|()| Value::Unit);
                    self.control
                        .event(self.id, request.sequence, Stage::WriteReturned);
                    result
                }
                Operation::Savepoint => store.savepoint().map(Value::Savepoint),
                Operation::Rollback(id) => store.rollback_to(id).map(|()| Value::Unit),
                Operation::Commit if transaction.is_none() => Err(DbError::Fatal(
                    "commit outside transaction: not executed".into(),
                )),
                Operation::Commit => {
                    include_metrics = true;
                    let token = CommitToken {
                        session: self.id,
                        sequence: request.sequence,
                    };
                    let admitted = {
                        let mut state = self.control.state.lock().unwrap();
                        self.control.prune(&mut state);
                        if state.outcomes.len() >= self.control.limits.max_outcomes {
                            false
                        } else {
                            state.outcomes.insert(
                                token,
                                OutcomeEntry {
                                    outcome: CommitOutcome::Pending,
                                    finished: None,
                                },
                            );
                            true
                        }
                    };
                    if !admitted {
                        store
                            .abort()
                            .map_err(|e| format!("abort after outcome capacity rejection: {e}"))?;
                        transaction = None;
                        Err(DbError::Fatal(
                            "commit not executed: outcome capacity exhausted".into(),
                        ))
                    } else {
                        self.control
                            .event(self.id, request.sequence, Stage::CommitAccepted);
                        let result = store.commit();
                        let outcome = match &result {
                            Ok(()) => CommitOutcome::Committed,
                            Err(DbError::Conflict) => CommitOutcome::AbortedConflict,
                            Err(DbError::Fatal(s)) => CommitOutcome::Indeterminate(s.clone()),
                        };
                        // An adapter may leave a failed transaction open; cleanup
                        // completes before publishing a safe conflict outcome.
                        if result.is_err() {
                            store
                                .abort()
                                .map_err(|e| format!("abort after commit error: {e}"))?;
                        }
                        transaction = None;
                        self.control.state.lock().unwrap().outcomes.insert(
                            token,
                            OutcomeEntry {
                                outcome,
                                finished: Some(Instant::now()),
                            },
                        );
                        self.control
                            .event(self.id, request.sequence, Stage::CommitRecorded);
                        result.map(|()| Value::Unit)
                    }
                }
                Operation::Abort => {
                    include_metrics = true;
                    store.abort().map(|()| {
                        transaction = None;
                        Value::Unit
                    })
                }
                Operation::Metrics => {
                    include_metrics = true;
                    Ok(Value::Unit)
                }
                Operation::Resolve(token) => {
                    let mut state = self.control.state.lock().unwrap();
                    self.control.prune(&mut state);
                    Ok(Value::Outcome(
                        state
                            .outcomes
                            .get(&token)
                            .map_or(CommitOutcome::Unknown, |e| e.outcome.clone()),
                    ))
                }
                Operation::Close => {
                    close = true;
                    include_metrics = true;
                    store.abort().map(|()| {
                        transaction = None;
                        Value::Unit
                    })
                }
            };
            let reply = Reply {
                session: self.id,
                sequence: request.sequence,
                result: result.map_err(WireError::from),
                metrics: include_metrics.then(|| metrics(store)),
            };
            // A client that stops reading must not extend an open transaction
            // to the potentially much longer connection idle timeout.
            let idle_deadline = Instant::now() + self.control.limits.idle_timeout;
            let reply_deadline = transaction.map_or(idle_deadline, |started| {
                idle_deadline.min(started + self.control.limits.transaction_timeout)
            });
            if write_frame(&mut self.socket, &reply, reply_deadline).is_err() || close {
                break;
            }
        }
        if transaction.is_some() {
            store
                .abort()
                .map_err(|e| format!("disconnect abort failed: {e}"))?;
            self.control.state.lock().unwrap().stats.disconnect_aborts += 1;
        }
        self.socket.shutdown();
        Ok(())
    }
}

pub struct Client {
    socket: Option<Socket>,
    session: SessionId,
    sequence: u64,
    timeout: Duration,
    acknowledge: Option<CommitToken>,
    pending: Option<CommitToken>,
    pub metrics: StoreMetrics,
    pub retry_causes: BTreeMap<String, u64>,
    pub diagnostics: BTreeMap<String, u64>,
    last_attempted_sequence: Option<u64>,
    last_completed_sequence: Option<u64>,
    last_metrics_sequence: Option<u64>,
    transport_failed: bool,
}
impl Client {
    pub fn connect(endpoint: &Endpoint, timeout: Duration) -> Result<Self, String> {
        if timeout.is_zero() {
            return Err("client timeout must be positive".into());
        }
        let mut socket = match endpoint {
            Endpoint::Unix(path) => connect_unix(path, timeout)
                .map(Socket::Unix)
                .map_err(|e| e.to_string())?,
            Endpoint::Tcp(address) => {
                let stream =
                    TcpStream::connect_timeout(address, timeout).map_err(|e| e.to_string())?;
                stream.set_nodelay(true).map_err(|e| e.to_string())?;
                Socket::Tcp(stream)
            }
        };
        let welcome: Welcome = read_frame(&mut socket, Instant::now() + timeout)?;
        if welcome.version != VERSION {
            return Err("service protocol version mismatch".into());
        }
        Ok(Self {
            socket: Some(socket),
            session: welcome.session,
            sequence: 1,
            timeout,
            acknowledge: None,
            pending: None,
            metrics: StoreMetrics::default(),
            retry_causes: BTreeMap::new(),
            diagnostics: BTreeMap::new(),
            last_attempted_sequence: None,
            last_completed_sequence: None,
            last_metrics_sequence: None,
            transport_failed: false,
        })
    }
    pub fn session_id(&self) -> SessionId {
        self.session
    }
    pub fn next_commit_token(&self) -> CommitToken {
        CommitToken {
            session: self.session,
            sequence: self.sequence,
        }
    }
    pub fn pending_commit(&self) -> Option<CommitToken> {
        self.pending
    }
    pub fn resolve(
        endpoint: &Endpoint,
        token: CommitToken,
        timeout: Duration,
    ) -> Result<CommitOutcome, String> {
        let mut client = Self::connect(endpoint, timeout)?;
        match client
            .call(Operation::Resolve(token))
            .map_err(|e| e.to_string())?
        {
            Value::Outcome(outcome) => Ok(outcome),
            _ => Err("invalid outcome reply".into()),
        }
    }
    pub fn refresh_metrics(&mut self) -> Result<(), DbError> {
        self.unit(Operation::Metrics)
    }
    /// Describes cached cumulative counters without issuing a network request.
    /// A disconnected or unacknowledged operation may have done more work than
    /// this snapshot records; absence of a fresh reply is not a zero counter.
    pub fn metrics_status(&self) -> serde_json::Value {
        serde_json::json!({
            "source":"service_cache",
            "complete": self.last_metrics_sequence.is_some()
                && self.last_metrics_sequence == self.last_completed_sequence
                && self.last_completed_sequence == self.last_attempted_sequence
                && !self.transport_failed,
            "session":self.session,
            "last_attempted_sequence":self.last_attempted_sequence,
            "last_completed_sequence":self.last_completed_sequence,
            "last_metrics_sequence":self.last_metrics_sequence,
            "transport_failed":self.transport_failed,
            "connected":self.socket.is_some(),
            "scope":"Cumulative backend counters through the indicated acknowledged operation; does not resolve an indeterminate commit."})
    }
    pub fn close(&mut self) -> Result<(), DbError> {
        let result = self.unit(Operation::Close);
        self.disconnect();
        result
    }
    fn disconnect(&mut self) {
        if let Some(socket) = self.socket.take() {
            socket.shutdown();
        }
    }
    fn call(&mut self, operation: Operation) -> Result<Value, DbError> {
        let committing = matches!(operation, Operation::Commit);
        if self.socket.is_none() {
            return Err(DbError::Fatal(format!(
                "service connection closed; unresolved commit {:?}",
                self.pending
            )));
        }
        if self.sequence == u64::MAX {
            self.disconnect();
            return Err(DbError::Fatal("request sequence exhausted".into()));
        }
        let sequence = self.sequence;
        self.sequence += 1;
        self.last_attempted_sequence = Some(sequence);
        let token = CommitToken {
            session: self.session,
            sequence,
        };
        if committing {
            self.pending = Some(token);
        }
        let request = Request {
            session: self.session,
            sequence,
            acknowledge: self.acknowledge.take(),
            operation,
        };
        let deadline = Instant::now() + self.timeout;
        let transport: Result<Reply, String> = (|| {
            let socket = self.socket.as_mut().unwrap();
            write_frame(socket, &request, deadline)?;
            let reply: Reply = read_frame(socket, deadline)?;
            if reply.session != self.session || reply.sequence != sequence {
                return Err("service response identity/sequence mismatch".into());
            }
            Ok(reply)
        })();
        let reply = match transport {
            Ok(reply) => reply,
            Err(error) => {
                self.transport_failed = true;
                self.disconnect();
                return Err(DbError::Fatal(format!("service transport: {error}; unresolved commit {:?}; do not replay without a known aborted outcome", self.pending)));
            }
        };
        self.last_completed_sequence = Some(sequence);
        if let Some(metrics) = reply.metrics {
            self.metrics = metrics.metrics;
            self.retry_causes = metrics.retry_causes;
            self.diagnostics = metrics.diagnostics;
            self.last_metrics_sequence = Some(sequence);
        }
        if committing {
            if !matches!(reply.result, Err(WireError::Fatal(_))) {
                self.pending = None;
            } else {
                // Keep the unresolved token immutable. The caller can resolve
                // it over a new connection; reusing this client could silently
                // overwrite the only handle to an indeterminate operation.
                self.disconnect();
            }
            if self.pending.is_none() {
                self.acknowledge = Some(token);
            }
        }
        reply.result.map_err(DbError::from)
    }
    fn unit(&mut self, operation: Operation) -> Result<(), DbError> {
        match self.call(operation)? {
            Value::Unit => Ok(()),
            _ => Err(DbError::Fatal("invalid service unit reply".into())),
        }
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.disconnect();
    }
}
impl Store for Client {
    fn begin(&mut self, ids: &[usize]) -> Result<(), DbError> {
        self.unit(Operation::Begin(ids.to_vec()))
    }
    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        match self.call(Operation::Read(id))? {
            Value::Row(row) => Ok(row),
            _ => Err(DbError::Fatal("invalid row reply".into())),
        }
    }
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        match self.call(Operation::Query(query.clone()))? {
            Value::Rows(rows) => Ok(rows),
            _ => Err(DbError::Fatal("invalid query reply".into())),
        }
    }
    fn write(&mut self, row: Record) -> Result<(), DbError> {
        self.unit(Operation::Write(row))
    }
    fn savepoint(&mut self) -> Result<usize, DbError> {
        match self.call(Operation::Savepoint)? {
            Value::Savepoint(id) => Ok(id),
            _ => Err(DbError::Fatal("invalid savepoint reply".into())),
        }
    }
    fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
        self.unit(Operation::Rollback(id))
    }
    fn commit(&mut self) -> Result<(), DbError> {
        self.unit(Operation::Commit)
    }
    fn abort(&mut self) -> Result<(), DbError> {
        self.unit(Operation::Abort)
    }
}

#[cfg(test)]
mod protocol_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    // This fixture checks protocol bookkeeping only. The integration target
    // separately exercises actual native transactions, indexes and WAL.
    struct Memory {
        value: Arc<AtomicU64>,
        active: Arc<AtomicUsize>,
        commits: Arc<AtomicUsize>,
        transaction: Option<Record>,
    }
    impl Store for Memory {
        fn begin(&mut self, _: &[usize]) -> Result<(), DbError> {
            assert!(self.transaction.is_none());
            self.transaction = Some(Record {
                revision: self.value.load(Ordering::Acquire) as i64,
                ..Record::default()
            });
            self.active.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn read(&mut self, _: usize) -> Result<Record, DbError> {
            self.transaction
                .ok_or_else(|| DbError::Fatal("no transaction".into()))
        }
        fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
            self.read(0).map(|r| {
                vec![
                    r;
                    if matches!(query, Query::All) {
                        20_000
                    } else {
                        1
                    }
                ]
            })
        }
        fn write(&mut self, row: Record) -> Result<(), DbError> {
            self.read(0)?;
            self.transaction = Some(row);
            Ok(())
        }
        fn savepoint(&mut self) -> Result<usize, DbError> {
            Err(DbError::Fatal("not a protocol fixture operation".into()))
        }
        fn rollback_to(&mut self, _: usize) -> Result<(), DbError> {
            Err(DbError::Fatal("not a protocol fixture operation".into()))
        }
        fn commit(&mut self) -> Result<(), DbError> {
            let row = self.read(0)?;
            if row.revision < 0 {
                return Err(DbError::Fatal("injected indeterminate result".into()));
            }
            self.value.store(row.revision as u64, Ordering::Release);
            self.commits.fetch_add(1, Ordering::AcqRel);
            self.abort()
        }
        fn abort(&mut self) -> Result<(), DbError> {
            if self.transaction.take().is_some() {
                self.active.fetch_sub(1, Ordering::AcqRel);
            }
            Ok(())
        }
    }
    impl Drop for Memory {
        fn drop(&mut self) {
            self.abort().unwrap();
        }
    }
    fn until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn start(
        limits: Limits,
        observer: Option<Observer>,
    ) -> (Server, Arc<AtomicU64>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let value = Arc::new(AtomicU64::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let (v, a, c) = (
            Arc::clone(&value),
            Arc::clone(&active),
            Arc::clone(&commits),
        );
        let server = Server::start_observed(
            Endpoint::Tcp("127.0.0.1:0".parse().unwrap()),
            limits,
            move |session| {
                let mut store = Memory {
                    value: Arc::clone(&v),
                    active: Arc::clone(&a),
                    commits: Arc::clone(&c),
                    transaction: None,
                };
                session.serve(&mut store, |_| BackendMetrics::default())
            },
            observer,
        )
        .unwrap();
        (server, value, active, commits)
    }
    #[test]
    fn lost_commit_reply_remains_resolvable_without_reexecution_then_expires() {
        for stage in [Stage::CommitAccepted, Stage::CommitRecorded] {
            let reached = Arc::new(AtomicBool::new(false));
            let release = Arc::new(AtomicBool::new(false));
            let (r, g) = (Arc::clone(&reached), Arc::clone(&release));
            let observer: Observer = Arc::new(move |event| {
                if event.stage == stage {
                    r.store(true, Ordering::Release);
                    until(|| g.load(Ordering::Acquire));
                }
            });
            let (mut server, value, active, commits) = start(
                Limits {
                    outcome_retention: Duration::from_millis(250),
                    ..Limits::default()
                },
                Some(observer),
            );
            let endpoint = server.endpoint();
            let mut client = Client::connect(&endpoint, Duration::from_millis(80)).unwrap();
            client.begin(&[]).unwrap();
            client
                .write(Record {
                    revision: 17,
                    ..Record::default()
                })
                .unwrap();
            let token = client.next_commit_token();
            assert!(matches!(client.commit(), Err(DbError::Fatal(_))));
            assert!(reached.load(Ordering::Acquire));
            assert_eq!(client.pending_commit(), Some(token));
            assert!(
                client.begin(&[]).is_err(),
                "broken connection silently reused"
            );
            let expected = if stage == Stage::CommitAccepted {
                CommitOutcome::Pending
            } else {
                CommitOutcome::Committed
            };
            assert_eq!(
                Client::resolve(&endpoint, token, Duration::from_secs(1)).unwrap(),
                expected
            );
            release.store(true, Ordering::Release);
            until(|| active.load(Ordering::Acquire) == 0 && server.stats().active_sessions == 0);
            assert_eq!(
                Client::resolve(&endpoint, token, Duration::from_secs(1)).unwrap(),
                CommitOutcome::Committed
            );
            assert_eq!(value.load(Ordering::Acquire), 17);
            assert_eq!(commits.load(Ordering::Acquire), 1);
            until(|| server.stats().retained_outcomes == 0);
            assert_eq!(
                Client::resolve(&endpoint, token, Duration::from_secs(1)).unwrap(),
                CommitOutcome::Unknown
            );
            server.stop(Duration::from_secs(2)).unwrap();
            let (mut replacement, _, _, _) = start(Limits::default(), None);
            assert_eq!(
                Client::resolve(&replacement.endpoint(), token, Duration::from_secs(1)).unwrap(),
                CommitOutcome::Unknown
            );
            replacement.stop(Duration::from_secs(2)).unwrap();
        }
    }
    #[test]
    fn malformed_replayed_and_slow_partial_frames_release_transaction_resources() {
        let (mut server, _, active, _) = start(
            Limits {
                idle_timeout: Duration::from_millis(100),
                ..Limits::default()
            },
            None,
        );
        let endpoint = server.endpoint();
        for fault in ["oversize", "replay", "partial"] {
            let mut client = Client::connect(&endpoint, Duration::from_secs(1)).unwrap();
            client.begin(&[]).unwrap();
            let socket = client.socket.as_mut().unwrap();
            if fault == "oversize" {
                socket
                    .write_all(&((FRAME_BYTES + 1) as u32).to_be_bytes())
                    .unwrap();
            } else if fault == "replay" {
                write_frame(
                    socket,
                    &Request {
                        session: client.session,
                        sequence: 1,
                        acknowledge: None,
                        operation: Operation::Commit,
                    },
                    Instant::now() + Duration::from_secs(1),
                )
                .unwrap();
            } else {
                socket.write_all(&[0, 0]).unwrap();
            }
            until(|| server.stats().active_sessions == 0);
            assert_eq!(active.load(Ordering::Acquire), 0);
        }
        let stats = server.stop(Duration::from_secs(1)).unwrap();
        assert_eq!(stats.disconnect_aborts, 3);
        assert_eq!(stats.protocol_errors, 1);
    }
    #[test]
    fn admission_and_outcome_retention_are_bounded_and_acknowledgement_frees_capacity() {
        let (mut server, _, active, commits) = start(
            Limits {
                max_sessions: 2,
                max_outcomes: 1,
                ..Limits::default()
            },
            None,
        );
        let endpoint = server.endpoint();
        let timeout = Duration::from_secs(1);
        let mut first = Client::connect(&endpoint, timeout).unwrap();
        let mut second = Client::connect(&endpoint, timeout).unwrap();
        assert!(Client::connect(&endpoint, timeout).is_err());
        assert_eq!(server.stats().active_sessions, 2);
        first.begin(&[]).unwrap();
        let token = first.next_commit_token();
        first.commit().unwrap();
        assert_eq!(server.stats().retained_outcomes, 1);
        second.begin(&[]).unwrap();
        assert!(matches!(second.commit(), Err(DbError::Fatal(_))));
        assert_eq!(commits.load(Ordering::Acquire), 1);
        assert_eq!(active.load(Ordering::Acquire), 0);
        first.refresh_metrics().unwrap();
        assert_eq!(server.stats().retained_outcomes, 0);
        drop(second);
        first.close().unwrap();
        until(|| server.stats().active_sessions == 0);
        assert_eq!(
            Client::resolve(&endpoint, token, timeout).unwrap(),
            CommitOutcome::Unknown
        );
        let mut third = Client::connect(&endpoint, timeout).unwrap();
        third.begin(&[]).unwrap();
        third.commit().unwrap();
        third.close().unwrap();
        assert_eq!(commits.load(Ordering::Acquire), 2);
        server.stop(timeout).unwrap();
    }
    #[test]
    fn idle_session_does_not_use_transaction_deadline_and_misuse_does_not_break_owner() {
        let (mut server, _, active, commits) = start(
            Limits {
                idle_timeout: Duration::from_secs(1),
                transaction_timeout: Duration::from_millis(30),
                ..Limits::default()
            },
            None,
        );
        let mut client = Client::connect(&server.endpoint(), Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(80));
        client.refresh_metrics().unwrap();
        assert!(matches!(client.commit(), Err(DbError::Fatal(_))));
        assert_eq!(commits.load(Ordering::Acquire), 0);
        assert_eq!(server.stats().retained_outcomes, 0);
        assert!(server.stats().backend_failures.is_empty());
        drop(client);
        let mut client = Client::connect(&server.endpoint(), Duration::from_secs(1)).unwrap();
        client.begin(&[]).unwrap();
        client.abort().unwrap();
        client.close().unwrap();
        assert_eq!(active.load(Ordering::Acquire), 0);
        server.stop(Duration::from_secs(1)).unwrap();
    }
    #[test]
    fn executor_panic_closes_existing_sessions_and_fails_owner_shutdown() {
        let armed = Arc::new(AtomicBool::new(true));
        let observer: Observer = Arc::new(move |event| {
            if event.stage == Stage::WriteReturned && armed.swap(false, Ordering::AcqRel) {
                panic!("injected executor failure");
            }
        });
        let (mut server, _, active, commits) = start(Limits::default(), Some(observer));
        let mut first = Client::connect(&server.endpoint(), Duration::from_secs(1)).unwrap();
        let mut second = Client::connect(&server.endpoint(), Duration::from_secs(1)).unwrap();
        first.begin(&[]).unwrap();
        second.begin(&[]).unwrap();
        assert!(first.write(Record::default()).is_err());
        until(|| server.stats().active_sessions == 0);
        assert!(second.write(Record::default()).is_err());
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert_eq!(commits.load(Ordering::Acquire), 0);
        assert_eq!(server.stats().backend_failures.len(), 1);
        assert!(server.stop(Duration::from_secs(1)).is_err());
    }
    #[test]
    fn exhausted_session_identifiers_fail_closed_without_wrapping() {
        let (mut server, _, active, _) = start(Limits::default(), None);
        let mut first = Client::connect(&server.endpoint(), Duration::from_secs(1)).unwrap();
        first.begin(&[]).unwrap();
        server
            .control
            .next_session
            .store(u64::MAX, Ordering::Release);
        assert!(Client::connect(&server.endpoint(), Duration::from_secs(1)).is_err());
        until(|| server.stats().active_sessions == 0);
        assert_eq!(
            server.control.next_session.load(Ordering::Acquire),
            u64::MAX
        );
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(server.stop(Duration::from_secs(1)).is_err());
    }
    #[test]
    fn nonreading_client_reply_is_bounded_by_open_transaction_deadline() {
        let (mut server, _, active, _) = start(
            Limits {
                idle_timeout: Duration::from_secs(4),
                transaction_timeout: Duration::from_millis(250),
                ..Limits::default()
            },
            None,
        );
        let mut client = Client::connect(&server.endpoint(), Duration::from_secs(1)).unwrap();
        client.begin(&[]).unwrap();
        // A reply larger than both explicitly reduced socket buffers cannot
        // complete while the client reads nothing. It remains below FRAME_BYTES.
        fn small_buffer(socket: &Socket, option: libc::c_int) {
            let Socket::Tcp(stream) = socket else {
                panic!("TCP fixture")
            };
            let size: libc::c_int = 4096;
            // SAFETY: the descriptor is live and size is a properly sized int.
            assert_eq!(
                unsafe {
                    libc::setsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        option,
                        (&size as *const libc::c_int).cast(),
                        std::mem::size_of_val(&size) as libc::socklen_t,
                    )
                },
                0
            );
        }
        small_buffer(client.socket.as_ref().unwrap(), libc::SO_RCVBUF);
        {
            let state = server.control.state.lock().unwrap();
            small_buffer(
                state.sockets.get(&client.session.number).unwrap(),
                libc::SO_SNDBUF,
            );
        }
        let request = Request {
            session: client.session,
            sequence: client.sequence,
            acknowledge: None,
            operation: Operation::Query(Query::All),
        };
        let started = Instant::now();
        write_frame(
            client.socket.as_mut().unwrap(),
            &request,
            started + Duration::from_secs(1),
        )
        .unwrap();
        until(|| server.stats().active_sessions == 0);
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "non-reading reply retained transaction until idle timeout: {:?}",
            started.elapsed()
        );
        assert_eq!(server.stats().disconnect_aborts, 1);
        server.stop(Duration::from_secs(1)).unwrap();
    }
    #[test]
    fn received_indeterminate_commit_reply_preserves_token_and_disables_client_reuse() {
        let (mut server, _, active, _) = start(Limits::default(), None);
        let endpoint = server.endpoint();
        let mut client = Client::connect(&endpoint, Duration::from_secs(1)).unwrap();
        client.begin(&[]).unwrap();
        client
            .write(Record {
                revision: -1,
                ..Record::default()
            })
            .unwrap();
        let token = client.next_commit_token();
        assert!(matches!(client.commit(), Err(DbError::Fatal(_))));
        assert_eq!(client.pending_commit(), Some(token));
        assert!(client.begin(&[]).is_err());
        assert!(client.commit().is_err());
        assert_eq!(client.pending_commit(), Some(token));
        assert_eq!(
            Client::resolve(&endpoint, token, Duration::from_secs(1)).unwrap(),
            CommitOutcome::Indeterminate("injected indeterminate result".into())
        );
        until(|| server.stats().active_sessions == 0);
        assert_eq!(active.load(Ordering::Acquire), 0);
        server.stop(Duration::from_secs(1)).unwrap();
    }
}

#[cfg(test)]
mod retry_metric_tests {
    use super::*;

    struct ConflictingBackend {
        metrics: StoreMetrics,
        queries: u64,
        failed_aborts: u64,
        fail_abort_once: bool,
    }
    impl Store for ConflictingBackend {
        fn begin(&mut self, _: &[usize]) -> Result<(), DbError> {
            self.metrics.begins += 1;
            Ok(())
        }
        fn read(&mut self, _: usize) -> Result<Record, DbError> {
            unreachable!()
        }
        fn query(&mut self, _: &Query) -> Result<Vec<Record>, DbError> {
            self.queries += 1;
            Err(DbError::Conflict)
        }
        fn write(&mut self, _: Record) -> Result<(), DbError> {
            unreachable!()
        }
        fn savepoint(&mut self) -> Result<usize, DbError> {
            unreachable!()
        }
        fn rollback_to(&mut self, _: usize) -> Result<(), DbError> {
            unreachable!()
        }
        fn commit(&mut self) -> Result<(), DbError> {
            unreachable!()
        }
        fn abort(&mut self) -> Result<(), DbError> {
            self.metrics.aborts += 1;
            if std::mem::take(&mut self.fail_abort_once) {
                self.failed_aborts += 1;
                Err(DbError::Fatal("injected cleanup failure".into()))
            } else {
                Ok(())
            }
        }
    }
    fn start(fail_abort_once: bool) -> Server {
        Server::start(
            Endpoint::Tcp("127.0.0.1:0".parse().unwrap()),
            Limits::default(),
            move |session| {
                let mut store = ConflictingBackend {
                    metrics: StoreMetrics::default(),
                    queries: 0,
                    failed_aborts: 0,
                    fail_abort_once,
                };
                session.serve(&mut store, |store| BackendMetrics {
                    metrics: store.metrics.clone(),
                    retry_causes: BTreeMap::from([
                        ("query:synthetic_conflict".into(), store.queries),
                        ("abort:synthetic_failure".into(), store.failed_aborts),
                    ]),
                    diagnostics: BTreeMap::from([("query:calls".into(), store.queries)]),
                })
            },
        )
        .unwrap()
    }
    #[test]
    fn abort_reply_refreshes_conflict_counters_without_an_extra_metrics_request() {
        let mut server = start(false);
        let mut client = Client::connect(&server.endpoint(), Duration::from_secs(2)).unwrap();
        assert_eq!(client.metrics_status()["complete"], false);
        for expected in 1..=2 {
            client.begin(&[]).unwrap();
            assert!(matches!(
                client.query(&Query::GlobalDue { at: 1 }),
                Err(DbError::Conflict)
            ));
            assert_eq!(client.metrics_status()["complete"], false);
            client.abort().unwrap();
            let status = client.metrics_status();
            assert_eq!(status["complete"], true);
            assert_eq!(status["last_attempted_sequence"], expected * 3);
            assert_eq!(status["last_metrics_sequence"], expected * 3);
            assert_eq!(client.retry_causes["query:synthetic_conflict"], expected);
            assert_eq!(client.diagnostics["query:calls"], expected);
        }
        client.close().unwrap();
        server.stop(Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn failed_abort_returns_fresh_counters_but_does_not_claim_successful_cleanup() {
        let mut server = start(true);
        let mut client = Client::connect(&server.endpoint(), Duration::from_secs(2)).unwrap();
        client.begin(&[]).unwrap();
        assert!(matches!(
            client.query(&Query::GlobalDue { at: 1 }),
            Err(DbError::Conflict)
        ));
        assert!(
            matches!(client.abort(),Err(DbError::Fatal(message)) if message=="injected cleanup failure")
        );
        assert_eq!(client.metrics_status()["complete"], true);
        assert_eq!(client.retry_causes["query:synthetic_conflict"], 1);
        assert_eq!(client.retry_causes["abort:synthetic_failure"], 1);
        // A fresh metrics snapshot describes work performed; its freshness is
        // deliberately distinct from the abort result delivered to the caller.
        client.close().unwrap();
        server.stop(Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn broken_connection_preserves_cached_values_with_explicit_incomplete_status() {
        let mut server = start(false);
        let mut client = Client::connect(&server.endpoint(), Duration::from_secs(2)).unwrap();
        client.begin(&[]).unwrap();
        assert!(matches!(
            client.query(&Query::GlobalDue { at: 1 }),
            Err(DbError::Conflict)
        ));
        client.abort().unwrap();
        let last = client.metrics_status()["last_metrics_sequence"].clone();
        client.begin(&[]).unwrap();
        server.stop(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            client.query(&Query::GlobalDue { at: 1 }),
            Err(DbError::Fatal(_))
        ));
        let status = client.metrics_status();
        assert_eq!(status["complete"], false);
        assert_eq!(status["transport_failed"], true);
        assert_eq!(status["connected"], false);
        assert_eq!(status["last_metrics_sequence"], last);
        assert_eq!(client.retry_causes["query:synthetic_conflict"], 1);
    }
}
