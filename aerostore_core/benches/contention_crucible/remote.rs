//! Disposable TCP service owner for one trusted benchmark client host.
//! This is a harness, not an authenticated or recoverable production service.
use super::supervision::write_json;
use super::{aerostore, calibrated, fixture, maintenance, model, service};
use crate::extended_crucible::model::Record;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrameSetup {
    pub version: u32,
    pub run_id: String,
    pub endpoint: service::Endpoint,
    pub workload: String,
    pub families: usize,
    pub seed: u64,
    pub projection_interval_seconds: u64,
    pub housekeeping_interval_seconds: u64,
    #[serde(default)]
    pub dispatch: calibrated::Dispatch,
    #[serde(default)]
    pub affinity_ttl_ms: u64,
    #[serde(default)]
    pub signature_pattern: calibrated::SignaturePattern,
    #[serde(default)]
    pub maintenance_mode: maintenance::Mode,
    #[serde(default = "calibrated::default_projection_batch_size")]
    pub projection_batch_size: usize,
    #[serde(default = "calibrated::default_housekeeping_batch_size")]
    pub housekeeping_batch_size: usize,
    #[serde(default = "calibrated::default_max_maintenance_batches")]
    pub max_maintenance_batches: u64,
    #[serde(default)]
    pub expiry_index_policy: fixture::ExpiryIndexPolicy,
    #[serde(default)]
    pub retry_diagnostics: bool,
    pub global_time_predicates: bool,
    pub initial_rows: Vec<Record>,
    pub server_pid: u32,
    pub max_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrameFinal {
    pub version: u32,
    pub run_id: String,
    pub passed: bool,
    pub error: Option<String>,
    pub final_rows: Vec<Record>,
    pub audit: Value,
    pub after_drain: Value,
    pub service_stats: service::ServerStats,
    pub completion_drain_seconds: f64,
    pub wal_bytes_after_drain: u64,
    pub registrations_after_stop: usize,
    /// Server-relative elapsed times only. No cross-host monotonic comparison.
    pub retention_samples: Vec<Value>,
}

impl FrameFinal {
    fn failed(run_id: &str, error: String) -> Self {
        Self {
            version: 1,
            run_id: run_id.into(),
            passed: false,
            error: Some(error),
            final_rows: vec![],
            audit: Value::Null,
            after_drain: Value::Null,
            service_stats: service::ServerStats::default(),
            completion_drain_seconds: 0.0,
            wal_bytes_after_drain: 0,
            registrations_after_stop: 0,
            retention_samples: vec![],
        }
    }
}

/// Only child PIDs we actually forked are eligible for abrupt error cleanup.
/// Check waitpid first: a child already reaped by normal shutdown must never
/// become a signal target if its numeric PID has since been reused.
#[derive(Default)]
struct Children(Vec<libc::pid_t>);
impl Drop for Children {
    fn drop(&mut self) {
        for pid in self.0.drain(..) {
            let mut status = 0;
            let result = loop {
                let result = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
                if result >= 0
                    || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
                {
                    break result;
                }
            };
            if result == 0 {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
                loop {
                    let result = unsafe { libc::waitpid(pid, &mut status, 0) };
                    if result >= 0
                        || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
                    {
                        break;
                    }
                }
            }
        }
    }
}

fn prepare_owner_process() -> Result<libc::pid_t, String> {
    let parent = unsafe { libc::getppid() };
    let pid = unsafe { libc::getpid() };
    // This entry point runs only in its own disposable server process. The
    // watchdog can therefore terminate every helper without signalling a shell
    // or unrelated client process, including when invoked without our script.
    if unsafe { libc::getpgrp() } != pid && unsafe { libc::setsid() } < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0
        || unsafe { libc::getppid() } != parent
    {
        return Err("remote server could not establish parent-death supervision".into());
    }
    Ok(pid)
}

fn sample(shared: &aerostore::Shared, wal: &Path, started: Instant) -> Result<Value, String> {
    Ok(
        json!({"server_elapsed_seconds":started.elapsed().as_secs_f64(),
        "storage":aerostore::retention(shared)?,
        "wal_file_bytes":fs::metadata(wal).map(|m|m.len()).unwrap_or(0)}),
    )
}

fn wait_for_finish(
    server: &service::Server,
    shared: &aerostore::Shared,
    wal: &Path,
    max_seconds: u64,
    samples: &mut Vec<Value>,
) -> Result<(), String> {
    let started = Instant::now();
    let mut input = Vec::new();
    let mut last_sample = started;
    samples.push(sample(shared, wal, started)?);
    loop {
        if started.elapsed() >= Duration::from_secs(max_seconds) {
            return Err("remote server finish deadline expired".into());
        }
        let stats = server.stats();
        if !stats.backend_failures.is_empty() {
            return Err(format!(
                "remote backend failure: {:?}",
                stats.backend_failures
            ));
        }
        let mut poll = libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut poll, 1, 100) };
        if result < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(std::io::Error::last_os_error().to_string());
        }
        if result > 0 {
            let mut bytes = [0_u8; 256];
            let count =
                unsafe { libc::read(libc::STDIN_FILENO, bytes.as_mut_ptr().cast(), bytes.len()) };
            if count == 0 {
                return Err("remote server stdin closed before finish".into());
            }
            if count < 0 {
                if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(std::io::Error::last_os_error().to_string());
            }
            input.extend_from_slice(&bytes[..count as usize]);
            if input.len() > 1024 {
                return Err("remote finish command exceeds 1024 bytes".into());
            }
            if input.contains(&b'\n') {
                return if input == b"finish\n" || input == b"finish\r\n" {
                    Ok(())
                } else {
                    Err("remote server expects exactly one finish line".into())
                };
            }
        }
        if last_sample.elapsed() >= Duration::from_secs(1) {
            samples.push(sample(shared, wal, started)?);
            last_sample = Instant::now();
        }
    }
}

/// Serve one fixture until `finish\n` arrives on stdin. `max_seconds` bounds the
/// workload wait; an additional 90-second watchdog bounds stop/drain/audit and
/// kills this isolated owner group if an engine operation cannot be cancelled.
pub fn serve(
    output: &Path,
    bind: SocketAddr,
    workload: &str,
    families: usize,
    seed: u64,
    shm_mib: usize,
    global_time: bool,
    max_seconds: u64,
    projection_interval_seconds: u64,
    housekeeping_interval_seconds: u64,
    dispatch: calibrated::Dispatch,
    affinity_ttl_ms: u64,
    signature_pattern: calibrated::SignaturePattern,
    maintenance_mode: maintenance::Mode,
    projection_batch_size: usize,
    housekeeping_batch_size: usize,
    max_maintenance_batches: u64,
    expiry_index_policy: fixture::ExpiryIndexPolicy,
    retry_diagnostics: bool,
) -> Result<(), String> {
    if retry_diagnostics && !aerostore_core::retry_diagnostics::compiled() {
        return Err("retry diagnostics require the retry-diagnostics build feature".into());
    }
    if !["legacy", "lifecycle", "fleet", "calibrated"].contains(&workload)
        || !(1..=1024).contains(&families)
        || (workload == "fleet" && families < 16)
        || (workload == "calibrated" && families < 4)
        || !(32..=3584).contains(&shm_mib)
        || !(1..=3600).contains(&max_seconds)
        || !(1..=3600).contains(&projection_interval_seconds)
        || !(1..=3600).contains(&housekeeping_interval_seconds)
        || (dispatch == calibrated::Dispatch::SignatureAffinity
            && !(1..=3_600_000).contains(&affinity_ttl_ms))
        || (dispatch == calibrated::Dispatch::Identity && affinity_ttl_ms != 0)
        || (workload != "calibrated"
            && (dispatch != calibrated::Dispatch::Identity
                || affinity_ttl_ms != 0
                || signature_pattern != calibrated::SignaturePattern::Both
                || maintenance_mode != maintenance::Mode::Batch
                || projection_batch_size != 4
                || housekeeping_batch_size != 32
                || max_maintenance_batches != 4096))
        || !(1..=16).contains(&projection_batch_size)
        || !(1..=64).contains(&housekeeping_batch_size)
        || !(1..=4096).contains(&max_maintenance_batches)
    {
        return Err("invalid bounded remote-server fixture configuration".into());
    }
    let process_group = prepare_owner_process()?;
    let run_id = format!(
        "{}-{}",
        process_group,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos()
    );
    let final_path = output.with_extension("final.json");
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    for stale in [output, &final_path] {
        match fs::remove_file(stale) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.to_string()),
        }
    }
    let directory = parent.join(format!("remote-server-{run_id}"));
    fs::create_dir(&directory).map_err(|e| e.to_string())?;
    let arena = directory.join("arena.mmap");
    let wal = directory.join("wal");
    let initial = if workload == "calibrated" {
        calibrated::initial_records_for(&calibrated::Config {
            duration_ns: 1_000_000_000,
            foreground_rate: 1,
            foreground_workers: 1,
            families,
            seed,
            projection_interval_seconds,
            housekeeping_interval_seconds,
            dispatch,
            affinity_ttl_ms,
            signature_pattern,
            maintenance_mode,
            projection_batch_size,
            housekeeping_batch_size,
            max_maintenance_batches,
        })?
    } else {
        model::sustained_initial_for(workload, families, seed)
    };
    let result = (|| {
        let shared = aerostore::Shared::create_with_policy(
            &arena,
            shm_mib << 20,
            &initial,
            expiry_index_policy,
        )?;
        let mut children = Children::default();
        // All forks precede watchdog, vacuum and service executor threads.
        let writer = aerostore_core::spawn_wal_writer_daemon(shared.ring.clone(), &wal)
            .map_err(|e| e.to_string())?;
        children.0.push(writer.pid());
        let mut collectors = Vec::new();
        for index in &shared.indexes {
            let collector = index
                .spawn_gc_daemon(Duration::from_millis(25))
                .map_err(|e| e.to_string())?;
            children.0.push(collector.pid());
            collectors.push(collector);
        }
        let (cancel_watchdog, watchdog) = mpsc::channel::<()>();
        let watchdog_path = final_path.clone();
        let watchdog_id = run_id.clone();
        std::thread::Builder::new()
            .name("remote-deadline".into())
            .spawn(move || {
                if matches!(
                    watchdog.recv_timeout(Duration::from_secs(max_seconds + 90)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    let failure = FrameFinal::failed(
                        &watchdog_id,
                        "remote owner hard deadline: shutdown/drain may be incomplete".into(),
                    );
                    let _ = write_json(&watchdog_path, &failure);
                    unsafe {
                        libc::kill(-process_group, libc::SIGKILL);
                    }
                }
            })
            .map_err(|e| e.to_string())?;
        // Error returns may follow an abandoned engine guard. Never let a
        // destructor attempt an unbounded vacuum join; this owner is disposable.
        let vacuum = std::mem::ManuallyDrop::new(
            aerostore_core::spawn_vacuum_daemon_with_config(
                Arc::clone(&shared.table),
                aerostore_core::VacuumDaemonConfig::default()
                    .with_interval(Duration::from_millis(25)),
            )
            .map_err(|e| e.to_string())?,
        );
        let attachment = shared.attachment(&arena);
        let mut limits = service::Limits::default();
        limits.idle_timeout = Duration::from_secs(max_seconds + 60);
        let mut server =
            service::Server::start(service::Endpoint::Tcp(bind), limits, move |session| {
                let mapping = aerostore::Shared::attach(&attachment)?;
                let mut adapter = aerostore::Adapter::new_with_diagnostics(
                    &mapping,
                    global_time,
                    retry_diagnostics,
                );
                session.serve(&mut adapter, |adapter| service::BackendMetrics {
                    metrics: adapter.metrics.clone(),
                    retry_causes: adapter.retry_causes.clone(),
                    diagnostics: adapter.diagnostics.clone(),
                })
            })?;
        let setup = FrameSetup {
            version: 1,
            run_id: run_id.clone(),
            endpoint: server.endpoint(),
            workload: workload.into(),
            families,
            seed,
            projection_interval_seconds,
            housekeeping_interval_seconds,
            dispatch,
            affinity_ttl_ms,
            signature_pattern,
            maintenance_mode,
            projection_batch_size,
            housekeeping_batch_size,
            max_maintenance_batches,
            expiry_index_policy,
            retry_diagnostics,
            global_time_predicates: global_time,
            initial_rows: initial.clone(),
            server_pid: std::process::id(),
            max_seconds,
        };
        write_json(output, &setup)?;
        let mut samples = Vec::new();
        let completion = wait_for_finish(&server, &shared, &wal, max_seconds, &mut samples);
        let drain_started = Instant::now();
        let stats = server.stop(Duration::from_secs(10))?;
        vacuum.stop().map_err(|e| e.to_string())?;
        for collector in &collectors {
            collector.stop().map_err(|e| e.to_string())?;
        }
        shared.ring.close().map_err(|e| e.to_string())?;
        writer.join().map_err(|e| e.to_string())?;
        aerostore_core::run_vacuum_pass(&shared.table).map_err(|e| e.to_string())?;
        // Match the local-owner timing boundary: final snapshot/invariant/index
        // audits are evidence work after the completion drain measurement.
        let drain_seconds = drain_started.elapsed().as_secs_f64();
        let final_rows = shared.snapshot()?;
        model::validate_snapshot(&final_rows)?;
        let audit = shared.audit()?;
        let registrations = shared.arena.create_snapshot().len();
        if registrations != 0 {
            return Err(format!(
                "{registrations} registrations remain after remote session shutdown"
            ));
        }
        let after_drain = aerostore::retention(&shared)?;
        let final_frame = FrameFinal {
            version: 1,
            run_id: run_id.clone(),
            passed: completion.is_ok(),
            error: completion.err(),
            final_rows,
            audit,
            after_drain,
            service_stats: stats,
            completion_drain_seconds: drain_seconds,
            wal_bytes_after_drain: fs::metadata(&wal).map(|m| m.len()).unwrap_or(0),
            registrations_after_stop: registrations,
            retention_samples: samples,
        };
        // Heavy scratch storage is discarded only after the successful audit;
        // setup/final evidence remains. All helper processes were reaped above.
        fs::remove_file(&arena).map_err(|e| e.to_string())?;
        fs::remove_file(&wal).map_err(|e| e.to_string())?;
        write_json(&final_path, &final_frame)?;
        let _ = cancel_watchdog.send(());
        if final_frame.passed {
            Ok(())
        } else {
            Err(final_frame
                .error
                .unwrap_or("remote execution failed".into()))
        }
    })();
    if let Err(error) = &result {
        if !final_path.exists() {
            write_json(&final_path, &FrameFinal::failed(&run_id, error.clone()))?;
        }
    }
    result
}
