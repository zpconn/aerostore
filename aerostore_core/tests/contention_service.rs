#![cfg(target_os = "linux")]
#![allow(dead_code, unused_imports)]
//! Process-isolated application-client death tests against the real contention
//! native adapter. The fixture only initializes its table/indexes/WAL; it does
//! not replace transaction execution. No live mapping is reset after a kill.
#[path = "../benches/extended_crucible/metrics.rs"]
pub mod existing_metrics;
#[path = "../benches/extended_crucible/model.rs"]
pub mod existing_model;
mod extended_crucible {
    pub use crate::existing_metrics as metrics;
    pub use crate::existing_model as model;
    pub mod aerostore {
        pub use crate::fixture::{Attachment, Shared, WAL_SLOTS, WAL_SLOT_BYTES};
    }
}
#[path = "../benches/contention_crucible/aerostore.rs"]
mod native;
#[path = "../benches/contention_crucible/service.rs"]
mod service;
#[path = "../benches/contention_crucible/storage.rs"]
mod storage;

use service::{
    BackendMetrics, Client, CommitOutcome, CommitToken, Endpoint, Limits, Server, SessionId, Stage,
};
use std::fs::File;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use storage::{Query, Record, Store};

const TIMEOUT: Duration = Duration::from_secs(10);
const ROLE: &str = "AEROSTORE_SERVICE_TEST_ROLE";
const DIRECTORY: &str = "AEROSTORE_SERVICE_TEST_DIRECTORY";

mod fixture {
    use super::*;
    use aerostore_core::{
        map_tmpfs_shared, IndexValue, OccTable, SecondaryIndex, SharedWalRing, ShmArena,
        WalDeltaCodec,
    };
    pub const WAL_SLOTS: usize = 16;
    pub const WAL_SLOT_BYTES: usize = 65536;
    pub struct Attachment;
    pub struct Shared {
        pub arena: Arc<ShmArena>,
        pub table: Arc<OccTable<Record>>,
        pub indexes: Vec<SecondaryIndex<usize>>,
        pub ring: SharedWalRing<WAL_SLOTS, WAL_SLOT_BYTES>,
    }
    impl WalDeltaCodec for Record {}
    pub fn keys(row: &Record) -> [Option<i64>; 5] {
        if !row.active {
            return [None; 5];
        }
        [
            (row.kind == existing_model::FLIGHT).then_some(row.callsign),
            (row.kind == existing_model::FLIGHT && row.tail != 0).then_some(row.tail),
            Some(row.family),
            (row.kind == existing_model::SCHEDULED).then_some(row.due),
            Some(row.event_time),
        ]
    }
    pub fn create(path: &Path) -> Shared {
        let arena = Arc::new(map_tmpfs_shared(path, 32 << 20).unwrap().arena);
        let mut table = OccTable::new(Arc::clone(&arena), 16).unwrap();
        let indexes: Vec<_> = ["callsign", "tail", "family", "due", "event_time"]
            .into_iter()
            .map(|name| SecondaryIndex::new_in_shared(name, Arc::clone(&arena)))
            .collect();
        for id in 0..16 {
            let row = Record {
                id,
                active: true,
                kind: existing_model::FLIGHT,
                family: id as i64 / 4,
                callsign: 100 + id as i64,
                ..Record::default()
            };
            table.seed_row(id, row).unwrap();
            for (index, key) in indexes.iter().zip(keys(&row)) {
                if let Some(key) = key {
                    index.try_insert(IndexValue::I64(key), id).unwrap();
                }
            }
        }
        let functions: [fn(&Record) -> Option<IndexValue>; 5] = [
            |r| keys(r)[0].map(IndexValue::I64),
            |r| keys(r)[1].map(IndexValue::I64),
            |r| keys(r)[2].map(IndexValue::I64),
            |r| keys(r)[3].map(IndexValue::I64),
            |r| keys(r)[4].map(IndexValue::I64),
        ];
        for (index, key) in indexes.iter().zip(functions) {
            table.bind_index(index.clone(), key).unwrap();
        }
        let ring = SharedWalRing::create(Arc::clone(&arena)).unwrap();
        Shared {
            arena,
            table: Arc::new(table),
            indexes,
            ring,
        }
    }
    pub fn audit(shared: &Shared) {
        assert_eq!(
            shared.arena.create_snapshot().len(),
            0,
            "orphan transaction registration"
        );
        let rows = shared.table.snapshot_latest_rows().unwrap();
        for (number, index) in shared.indexes.iter().enumerate() {
            let mut expected: Vec<_> = rows
                .iter()
                .filter_map(|(id, row)| keys(row)[number].map(|key| (IndexValue::I64(key), *id)))
                .collect();
            let mut actual = index.try_entries().unwrap();
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected);
            for _ in 0..8 {
                index.collect_garbage_once(usize::MAX);
            }
        }
    }
}

struct Process {
    child: Child,
    group: bool,
    reaped: bool,
}
impl Process {
    fn spawn(directory: &Path, role: &str, test: &str, log: &str) -> Self {
        let output = File::create(directory.join(log)).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", test, "--nocapture"])
            .env(ROLE, role)
            .env(DIRECTORY, directory)
            .stdout(output.try_clone().unwrap())
            .stderr(output);
        // Only the owner starts a group. Its application clients inherit that
        // group, so the outer deadline also contains a stuck grandchild.
        use std::os::unix::process::CommandExt;
        let group = role.starts_with("owner-");
        if group {
            command.process_group(0);
        }
        let parent = std::process::id() as libc::pid_t;
        // SAFETY: the pre-exec closure uses only async-signal-safe system calls.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != parent {
                    return Err(std::io::Error::from_raw_os_error(libc::ECHILD));
                }
                Ok(())
            });
        }
        Self {
            child: command.spawn().unwrap(),
            group,
            reaped: false,
        }
    }
    fn kill(&mut self) {
        self.signal();
        let status = self.child.wait().unwrap();
        self.reaped = true;
        assert!(!status.success());
    }
    fn signal(&self) {
        let pid = self.child.id() as libc::pid_t;
        // SAFETY: an unreaped owned child pins its PID against reuse.
        unsafe {
            libc::kill(if self.group { -pid } else { pid }, libc::SIGKILL);
        }
    }
    fn success(&mut self, timeout: Duration) {
        let start = Instant::now();
        loop {
            // Observe exit without reaping, then clean the owned group before
            // its leader PID can be reused by an unrelated process.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.child.id(),
                    &mut info,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            if result < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
                continue;
            }
            assert_eq!(result, 0, "waitid failed");
            if unsafe { info.si_pid() } != 0 {
                if self.group {
                    self.signal();
                }
                let status = self.child.wait().unwrap();
                self.reaped = true;
                assert!(status.success(), "child failed: {status}");
                return;
            }
            assert!(
                start.elapsed() < timeout,
                "isolated service owner exceeded deadline"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if !self.reaped {
            self.signal();
            let _ = self.child.wait();
            self.reaped = true;
        }
    }
}
fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !predicate() {
        assert!(Instant::now() < deadline, "bounded wait expired");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn connect(endpoint: &Endpoint) -> Client {
    Client::connect(endpoint, TIMEOUT).unwrap()
}
fn change(endpoint: &Endpoint, id: usize) -> i64 {
    let mut client = connect(endpoint);
    let value = change_client(&mut client, id);
    client.close().unwrap();
    value
}
fn change_client(client: &mut Client, id: usize) -> i64 {
    for _ in 0..128 {
        client.begin(&[]).unwrap();
        let family = id as i64 / 4;
        let rows = client
            .query(&Query::Family {
                family,
                kind: existing_model::FLIGHT,
            })
            .unwrap();
        assert!(rows.iter().any(|row| row.id == id));
        let mut row = client.read(id).unwrap();
        row.revision += 1;
        client.write(row).unwrap();
        match client.commit() {
            Ok(()) => {
                return row.revision;
            }
            Err(storage::DbError::Conflict) => client.abort().unwrap(),
            Err(e) => panic!("survivor failed: {e}"),
        }
    }
    panic!("survivor retry limit");
}

#[test]
fn service_client_helper() {
    if std::env::var(ROLE).as_deref() != Ok("victim") {
        return;
    }
    let directory = std::path::PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
    let endpoint: Endpoint =
        serde_json::from_slice(&std::fs::read(directory.join("endpoint.json")).unwrap()).unwrap();
    let mut client = connect(&endpoint);
    std::fs::write(
        directory.join("ready.json"),
        serde_json::to_vec(&client.session_id()).unwrap(),
    )
    .unwrap();
    until(|| directory.join("go").exists());
    client.begin(&[]).unwrap();
    client
        .query(&Query::Family {
            family: 0,
            kind: existing_model::FLIGHT,
        })
        .unwrap();
    let mut row = client.read(0).unwrap();
    row.revision = 999;
    client.write(row).unwrap();
    std::fs::write(
        directory.join("commit-token.json"),
        serde_json::to_vec(&client.next_commit_token()).unwrap(),
    )
    .unwrap();
    client.commit().unwrap();
    // The parent kills us at a deterministic service cut, never based on sleep.
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn service_survivor_helper() {
    if std::env::var(ROLE).as_deref() != Ok("survivor") {
        return;
    }
    let directory = std::path::PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
    let endpoint: Endpoint =
        serde_json::from_slice(&std::fs::read(directory.join("endpoint.json")).unwrap()).unwrap();
    let mut client = connect(&endpoint);
    std::fs::write(directory.join("survivor-ready"), []).unwrap();
    for index in 0..5 {
        until(|| directory.join(format!("survivor-go-{index}")).exists());
        let same = change_client(&mut client, 0);
        let disjoint = change_client(&mut client, 12);
        let bytes = serde_json::to_vec(&(same, disjoint)).unwrap();
        let temp = directory.join("survivor-response.tmp");
        std::fs::write(&temp, bytes).unwrap();
        std::fs::rename(temp, directory.join(format!("survivor-done-{index}"))).unwrap();
    }
    client.close().unwrap();
}

fn owner(directory: &Path, transport: &str) {
    let shared = Arc::new(fixture::create(&directory.join("arena")));
    let writer =
        aerostore_core::spawn_wal_writer_daemon(shared.ring.clone(), directory.join("wal"))
            .unwrap();
    let armed = Arc::new(Mutex::new(None::<(SessionId, Stage)>));
    let release = Arc::new(AtomicBool::new(false));
    let (events, event_rx) = mpsc::channel();
    let hook = {
        let armed = Arc::clone(&armed);
        let release = Arc::clone(&release);
        Arc::new(move |event: service::Event| {
            if armed.lock().unwrap().as_ref() == Some(&(event.session, event.stage)) {
                events.send(event).unwrap();
                until(|| release.load(Ordering::Acquire));
            }
        }) as service::Observer
    };
    let endpoint = if transport == "unix" {
        Endpoint::Unix(directory.join("service.sock"))
    } else {
        Endpoint::Tcp("127.0.0.1:0".parse().unwrap())
    };
    let backend = Arc::clone(&shared);
    let limits = Limits {
        idle_timeout: Duration::from_secs(2),
        ..Limits::default()
    };
    let mut server = Server::start_observed(
        endpoint,
        limits,
        move |session| {
            let mut store = native::Adapter::new(&backend, false);
            session.serve(&mut store, |store| BackendMetrics {
                metrics: store.metrics.clone(),
                retry_causes: store.retry_causes.clone(),
                diagnostics: store.diagnostics.clone(),
            })
        },
        Some(hook),
    )
    .unwrap();
    let endpoint = server.endpoint();
    std::fs::write(
        directory.join("endpoint.json"),
        serde_json::to_vec(&endpoint).unwrap(),
    )
    .unwrap();
    let mut survivor = Process::spawn(
        directory,
        "survivor",
        "service_survivor_helper",
        "survivor.log",
    );
    until(|| directory.join("survivor-ready").exists());
    let mut observations = Vec::new();
    for (index, stage) in [
        Stage::RequestAccepted,
        Stage::QueryReturned,
        Stage::WriteReturned,
        Stage::CommitAccepted,
        Stage::CommitRecorded,
    ]
    .into_iter()
    .enumerate()
    {
        for name in ["ready.json", "go", "commit-token.json"] {
            let _ = std::fs::remove_file(directory.join(name));
        }
        release.store(false, Ordering::Release);
        let mut victim = Process::spawn(
            directory,
            "victim",
            "service_client_helper",
            &format!("victim-{index}.log"),
        );
        until(|| directory.join("ready.json").exists());
        let session: SessionId =
            serde_json::from_slice(&std::fs::read(directory.join("ready.json")).unwrap()).unwrap();
        *armed.lock().unwrap() = Some((session, stage));
        std::fs::write(directory.join("go"), []).unwrap();
        let cut = event_rx.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(cut.session, session);
        let started = Instant::now();
        victim.kill();
        release.store(true, Ordering::Release);
        *armed.lock().unwrap() = None;
        until(|| server.stats().active_sessions == 1);
        assert_eq!(shared.arena.create_snapshot().len(), 0);
        let outcome = if matches!(stage, Stage::CommitAccepted | Stage::CommitRecorded) {
            let token: CommitToken = serde_json::from_slice(
                &std::fs::read(directory.join("commit-token.json")).unwrap(),
            )
            .unwrap();
            let outcome = Client::resolve(&endpoint, token, TIMEOUT).unwrap();
            assert_eq!(outcome, CommitOutcome::Committed);
            Some(outcome)
        } else {
            None
        };
        std::fs::write(directory.join(format!("survivor-go-{index}")), []).unwrap();
        let response = directory.join(format!("survivor-done-{index}"));
        until(|| response.exists());
        let (same, disjoint): (i64, i64) =
            serde_json::from_slice(&std::fs::read(response).unwrap()).unwrap();
        if outcome.is_some() {
            assert_eq!(same, 1000);
        }
        until(|| server.stats().active_sessions == usize::from(index < 4));
        fixture::audit(&shared);
        observations.push(serde_json::json!({"stage":format!("{stage:?}"),"survivor_same_revision":same,"survivor_disjoint_revision":disjoint,"kill_to_both_survivors_ms":started.elapsed().as_secs_f64()*1000.0,"commit_outcome":outcome,"retention":native::retention(&shared).unwrap()}));
    }
    survivor.success(TIMEOUT);
    // Savepoint rollback is executed by the owner, across individual RPCs.
    let mut client = connect(&endpoint);
    client.begin(&[]).unwrap();
    let original = client.read(1).unwrap();
    let save = client.savepoint().unwrap();
    let mut changed = original;
    changed.callsign += 10000;
    changed.revision = 42;
    client.write(changed).unwrap();
    client.rollback_to(save).unwrap();
    assert_eq!(client.read(1).unwrap(), original);
    client.commit().unwrap();
    client.close().unwrap();
    // Repeated disconnected private writes must release ProcArray slots and
    // recycle private row allocations. Measure after allocator warm-up.
    let mut high_water = Vec::new();
    for _ in 0..24 {
        let mut client = connect(&endpoint);
        client.begin(&[]).unwrap();
        let mut row = client.read(2).unwrap();
        row.revision += 200;
        client.write(row).unwrap();
        drop(client);
        until(|| server.stats().active_sessions == 0);
        fixture::audit(&shared);
        high_water.push(shared.arena.chunked_arena().head_offset());
    }
    assert_eq!(
        &high_water[12..],
        vec![high_water[12]; 12].as_slice(),
        "repeated client disconnect allocated without plateau"
    );
    // A living but abandoned client also loses its private transaction on idle
    // timeout, without closing the engine or resetting its mappings.
    let mut idle = connect(&endpoint);
    idle.begin(&[]).unwrap();
    let row = idle.read(3).unwrap();
    idle.write(Record {
        revision: 77,
        ..row
    })
    .unwrap();
    until(|| server.stats().active_sessions == 0);
    assert!(idle.commit().is_err());
    change(&endpoint, 3);
    until(|| server.stats().active_sessions == 0);
    fixture::audit(&shared);
    let stats = server.stop(TIMEOUT).unwrap();
    assert!(stats.backend_failures.is_empty());
    assert_eq!(stats.active_sessions, 0);
    shared.ring.close().unwrap();
    writer.join().unwrap();
    std::fs::write(directory.join("report.json"),serde_json::to_vec_pretty(&serde_json::json!({"passed":true,"transport":transport,"kill_cuts":observations,"disconnect_high_water":high_water,"stats":stats,"scope":"application client SIGKILL and disconnect; no service/engine crash guarantee"})).unwrap()).unwrap();
}

#[test]
fn service_owner_helper() {
    let Ok(role) = std::env::var(ROLE) else {
        return;
    };
    if !role.starts_with("owner-") {
        return;
    }
    let directory = std::path::PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(move || owner(&directory, role.strip_prefix("owner-").unwrap()))
        .unwrap()
        .join()
        .unwrap();
}
#[test]
fn service_client_sigkill_preserves_unix_and_tcp_survivors() {
    for transport in ["unix", "tcp"] {
        let directory = tempfile::Builder::new()
            .prefix("aero-service-test-")
            .tempdir()
            .unwrap();
        let mut owner = Process::spawn(
            directory.path(),
            &format!("owner-{transport}"),
            "service_owner_helper",
            "owner.log",
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            owner.success(Duration::from_secs(60))
        }));
        let log = std::fs::read_to_string(directory.path().join("owner.log")).unwrap();
        assert!(
            result.is_ok(),
            "{transport} owner failed:\n{log}\nfixture retained at {:?}",
            directory.keep()
        );
        let report = std::fs::read_to_string(directory.path().join("report.json")).unwrap();
        if let Some(output) = std::env::var_os("AEROSTORE_SERVICE_EVIDENCE") {
            std::fs::create_dir_all(&output).unwrap();
            std::fs::write(Path::new(&output).join(format!("{transport}.json")), report).unwrap();
            std::fs::write(Path::new(&output).join(format!("{transport}.log")), log).unwrap();
        }
    }
}
