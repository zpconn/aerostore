use super::{aerostore, contracts, metrics::StoreMetrics, model, postgres};
use model::{DbError, Message, Outcome, Phase, Record, Store};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    engine: String,
    mode: String,
    families: usize,
    cycles: usize,
    workers: usize,
    seed: u64,
    shm_mib: usize,
    output: PathBuf,
    #[serde(skip_serializing)]
    pg_url: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            engine: "both".into(),
            mode: "all".into(),
            families: 32,
            cycles: 2,
            workers: 4,
            seed: 20260922,
            shm_mib: 256,
            output: "target/extended-crucible.json".into(),
            pg_url: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkerConfig {
    engine: String,
    attachment: Option<aerostore::Attachment>,
    pg_url: Option<String>,
    schema: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Task {
    index: usize,
    message: Message,
}
#[derive(Serialize, Deserialize)]
enum Request {
    Phase(Vec<Task>),
    Stop,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Observation {
    index: usize,
    outcome: Outcome,
    elapsed_ns: u64,
    retries: u64,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    ready: bool,
    observations: Vec<Observation>,
    metrics: StoreMetrics,
    error: Option<String>,
}

trait MeasuredStore: Store {
    fn metrics(&self) -> StoreMetrics;
}
impl MeasuredStore for aerostore::Adapter<'_> {
    fn metrics(&self) -> StoreMetrics {
        self.metrics.clone()
    }
}
impl MeasuredStore for postgres::Adapter {
    fn metrics(&self) -> StoreMetrics {
        self.metrics.clone()
    }
}

fn reply(out: &mut impl Write, value: &Reply) -> Result<(), String> {
    serde_json::to_writer(&mut *out, value).map_err(|e| e.to_string())?;
    writeln!(out).map_err(|e| e.to_string())?;
    out.flush().map_err(|e| e.to_string())
}

fn worker_loop(store: &mut impl MeasuredStore) -> Result<(), String> {
    let stdin = std::io::stdin();
    let mut out = BufWriter::new(std::io::stdout());
    reply(
        &mut out,
        &Reply {
            ready: true,
            observations: Vec::new(),
            metrics: store.metrics(),
            error: None,
        },
    )?;
    for line in stdin.lock().lines() {
        let request: Request =
            serde_json::from_str(&line.map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        match request {
            Request::Stop => {
                store.abort().map_err(|e| e.to_string())?;
                return Ok(());
            }
            Request::Phase(tasks) => {
                let mut observations = Vec::new();
                let mut error = None;
                for task in tasks {
                    let start = Instant::now();
                    let mut retries = 0;
                    loop {
                        match model::execute_message(store, &task.message) {
                            Ok(outcome) => {
                                observations.push(Observation {
                                    index: task.index,
                                    outcome,
                                    elapsed_ns: start.elapsed().as_nanos().min(u64::MAX as u128)
                                        as u64,
                                    retries,
                                });
                                break;
                            }
                            Err(DbError::Conflict) if retries < 128 => {
                                store.abort().map_err(|e| e.to_string())?;
                                retries += 1;
                                std::thread::sleep(Duration::from_micros(
                                    (50 * retries).min(10_000),
                                ));
                            }
                            Err(e) => {
                                let _ = store.abort();
                                error = Some(format!(
                                    "message {} {:?}: {e}",
                                    task.index, task.message.kind
                                ));
                                break;
                            }
                        }
                    }
                    if error.is_some() {
                        break;
                    }
                }
                let failed = error.is_some();
                reply(
                    &mut out,
                    &Reply {
                        ready: false,
                        observations,
                        metrics: store.metrics(),
                        error,
                    },
                )?;
                if failed {
                    return Err("worker transaction failed; details sent to parent".into());
                }
            }
        }
    }
    store.abort().map_err(|e| e.to_string())
}

fn internal_worker(path: &Path) -> Result<(), String> {
    // Exec workers do not inherit a multithreaded parent's Rust locks/runtimes.
    // Ensure abandoned workers also exit if the benchmark parent is killed.
    unsafe {
        let parent = libc::getppid();
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 || libc::getppid() != parent {
            return Err("failed to establish worker parent lifetime".into());
        }
    }
    let cfg: WorkerConfig = serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    match cfg.engine.as_str() {
        "aerostore" => {
            let shared = aerostore::Shared::attach(
                cfg.attachment.as_ref().ok_or("missing arena attachment")?,
            )?;
            let mut adapter = aerostore::Adapter::new(&shared);
            worker_loop(&mut adapter)
        }
        "postgres" => worker_loop(&mut postgres::Adapter::connect(
            cfg.pg_url
                .as_deref()
                .ok_or("missing PostgreSQL connection")?,
            &cfg.schema,
        )?),
        _ => Err("invalid worker engine".into()),
    }
}

struct Worker {
    child: Child,
    input: BufWriter<ChildStdin>,
    replies: mpsc::Receiver<Result<Reply, String>>,
    metrics: StoreMetrics,
}
impl Worker {
    fn spawn(config_path: &Path) -> Result<Self, String> {
        let mut child = Command::new(std::env::current_exe().map_err(|e| e.to_string())?)
            .arg("--internal-worker")
            .arg(config_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| e.to_string())?;
        let input = BufWriter::new(child.stdin.take().ok_or("missing worker stdin")?);
        let output = child.stdout.take().ok_or("missing worker stdout")?;
        let (send, replies) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let result = line.map_err(|e| e.to_string()).and_then(|line| {
                    serde_json::from_str(&line).map_err(|e| format!("invalid worker report: {e}"))
                });
                if send.send(result).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            input,
            replies,
            metrics: StoreMetrics::default(),
        })
    }
    fn send(&mut self, request: &Request) -> Result<(), String> {
        serde_json::to_writer(&mut self.input, request).map_err(|e| e.to_string())?;
        writeln!(self.input).map_err(|e| e.to_string())?;
        self.input.flush().map_err(|e| e.to_string())
    }
    fn receive(&mut self) -> Result<Reply, String> {
        let reply = self
            .replies
            .recv_timeout(Duration::from_secs(45))
            .map_err(|e| {
                format!(
                    "worker {} unavailable or exceeded 45s phase budget: {e}",
                    self.child.id()
                )
            })??;
        self.metrics = reply.metrics.clone();
        if let Some(error) = &reply.error {
            return Err(error.clone());
        }
        Ok(reply)
    }
    fn stop(&mut self) -> Result<(), String> {
        self.send(&Request::Stop)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("worker exited {status}"))
                };
            }
            if Instant::now() > deadline {
                return Err("worker failed to stop within 5s".into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        // A killed worker can strand a native lock. The caller must not audit
        // or reuse that arena after an unsuccessful workload run.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PhaseResult {
    name: String,
    messages: usize,
    committed_messages: usize,
    retries: u64,
    outputs: usize,
    outcome_counts: OutcomeCounts,
    elapsed_seconds: f64,
    state_fingerprint: String,
    arena_high_water_bytes: Option<u32>,
    invariants: model::InvariantReport,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct OutcomeCounts {
    duplicates: usize,
    rejected_inputs: usize,
    deliberate_aborts: usize,
    savepoint_rollbacks: usize,
    created_views: usize,
    updated_views: usize,
    ignored_stale_observations: usize,
    expired_records: usize,
}

impl OutcomeCounts {
    fn from_observations(observations: &[Observation]) -> Self {
        let mut counts = Self::default();
        for observation in observations {
            let value = &observation.outcome;
            counts.duplicates += usize::from(value.duplicate);
            counts.rejected_inputs += usize::from(value.rejected);
            counts.deliberate_aborts += usize::from(value.aborted);
            counts.savepoint_rollbacks += usize::from(value.savepoint_rollback);
            counts.created_views += value.created_views;
            counts.updated_views += value.updated_views;
            counts.ignored_stale_observations += value.ignored_stale;
            counts.expired_records += value.expired_records;
        }
        counts
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct ReplayResult {
    engine: String,
    passed: bool,
    error: Option<String>,
    phases: Vec<PhaseResult>,
    messages: usize,
    committed_messages: usize,
    retries: u64,
    message_wall_seconds: f64,
    committed_messages_per_second: f64,
    latency_p50_us: f64,
    latency_p99_us: f64,
    metrics: StoreMetrics,
    index_audit: Option<serde_json::Value>,
}
impl ReplayResult {
    fn new(engine: &str) -> Self {
        Self {
            engine: engine.into(),
            passed: false,
            error: None,
            phases: Vec::new(),
            messages: 0,
            committed_messages: 0,
            retries: 0,
            message_wall_seconds: 0.0,
            committed_messages_per_second: 0.0,
            latency_p50_us: 0.0,
            latency_p99_us: 0.0,
            metrics: StoreMetrics::default(),
            index_audit: None,
        }
    }
}

fn fingerprint<T: Serialize>(value: &T) -> String {
    // Reproducibility checksum, not a cryptographic integrity assertion. Every
    // acceptance comparison uses full records/outcomes before hashing them.
    let bytes = serde_json::to_vec(value).expect("serializable fixture");
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn outcome_groups(messages: &[Message], outcomes: &[Outcome]) -> BTreeMap<String, Vec<String>> {
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (message, outcome) in messages.iter().zip(outcomes) {
        groups
            .entry(serde_json::to_string(message).unwrap())
            .or_default()
            .push(serde_json::to_string(outcome).unwrap());
    }
    for group in groups.values_mut() {
        group.sort();
    }
    groups
}

fn replay(
    cfg: &Config,
    trace: &[Phase],
    worker_cfg: &WorkerConfig,
    config_path: &Path,
    mut snapshot: impl FnMut() -> Result<Vec<Record>, String>,
    mut high_water: impl FnMut() -> Option<u32>,
) -> ReplayResult {
    let mut report = ReplayResult::new(&worker_cfg.engine);
    let mut workers = Vec::new();
    let mut latencies = Vec::new();
    let result = (|| -> Result<(), String> {
        fs::write(
            config_path,
            serde_json::to_vec(worker_cfg).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        for _ in 0..cfg.workers {
            workers.push(Worker::spawn(config_path)?);
        }
        for worker in &mut workers {
            if !worker.receive()?.ready {
                return Err("worker did not report ready".into());
            }
        }
        let mut reference = model::ReferenceStore::new(cfg.families);
        for (phase_number, phase) in trace.iter().enumerate() {
            fs::write(
                config_path.with_extension("progress"),
                phase_number.to_string(),
            )
            .map_err(|e| e.to_string())?;
            // Serial reference work and audits are outside measurement.
            let expected = phase
                .messages
                .iter()
                .map(|message| {
                    model::execute_message(&mut reference, message).map_err(|e| e.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let expected_rows = reference.snapshot();
            model::validate_snapshot(&expected_rows)
                .map_err(|e| format!("reference {}: {e}", phase.name))?;
            let mut assignments = vec![Vec::new(); cfg.workers];
            for (index, message) in phase.messages.iter().enumerate() {
                // Adjacent duplicate deliveries go to different processes;
                // rotation between phases prevents permanent flight ownership.
                assignments[(index + phase_number) % cfg.workers].push(Task {
                    index,
                    message: message.clone(),
                });
            }
            let started = Instant::now();
            for (worker, tasks) in workers.iter_mut().zip(assignments) {
                worker.send(&Request::Phase(tasks))?;
            }
            let mut observed = Vec::new();
            for worker in &mut workers {
                observed.extend(worker.receive()?.observations);
            }
            let elapsed = started.elapsed().as_secs_f64();
            observed.sort_by_key(|o| o.index);
            if observed.len() != expected.len()
                || observed
                    .iter()
                    .enumerate()
                    .any(|(i, value)| value.index != i)
            {
                return Err(format!(
                    "{} missing or duplicate completed messages",
                    phase.name
                ));
            }
            let actual_outcomes = observed
                .iter()
                .map(|o| o.outcome.clone())
                .collect::<Vec<_>>();
            if outcome_groups(&phase.messages, &actual_outcomes)
                != outcome_groups(&phase.messages, &expected)
            {
                return Err(format!("{} outcomes differ from the reference (grouped only by identical input messages)", phase.name));
            }
            let rows = snapshot()?;
            if rows != expected_rows {
                let mismatch = rows.iter().zip(&expected_rows).find(|(a, b)| a != b);
                return Err(format!("{} full state mismatch (actual={}, expected={} rows), first difference={mismatch:?}", phase.name, rows.len(), expected_rows.len()));
            }
            let invariants = model::validate_snapshot(&rows)?;
            let retries = observed.iter().map(|o| o.retries).sum::<u64>();
            let committed = observed.iter().filter(|o| !o.outcome.aborted).count();
            let outputs = observed.iter().map(|o| o.outcome.outputs.len()).sum();
            latencies.extend(observed.iter().map(|o| o.elapsed_ns));
            report.messages += observed.len();
            report.committed_messages += committed;
            report.retries += retries;
            report.message_wall_seconds += elapsed;
            report.phases.push(PhaseResult {
                name: phase.name.clone(),
                messages: observed.len(),
                committed_messages: committed,
                retries,
                outputs,
                outcome_counts: OutcomeCounts::from_observations(&observed),
                elapsed_seconds: elapsed,
                state_fingerprint: fingerprint(&rows),
                arena_high_water_bytes: high_water(),
                invariants,
            });
            if phase_number % 8 == 0 || phase_number + 1 == trace.len() {
                println!("extended_crucible_phase engine={} phase={}/{} name={} messages={} retries={} outputs={} parity=PASS", worker_cfg.engine, phase_number+1, trace.len(), phase.name, observed.len(), retries, outputs);
            }
        }
        for worker in &mut workers {
            worker.stop()?;
        }
        Ok(())
    })();
    for worker in &workers {
        report.metrics.add(&worker.metrics);
    }
    // All children are stopped or killed before the caller can reclaim memory.
    drop(workers);
    report.passed = result.is_ok();
    report.error = result.err();
    latencies.sort_unstable();
    if !latencies.is_empty() {
        report.latency_p50_us = latencies[(latencies.len() - 1) / 2] as f64 / 1000.0;
        report.latency_p99_us =
            latencies[((latencies.len() * 99).div_ceil(100) - 1).min(latencies.len() - 1)] as f64
                / 1000.0;
    }
    report.committed_messages_per_second =
        report.committed_messages as f64 / report.message_wall_seconds.max(f64::EPSILON);
    report
}

fn aerostore_replay(
    cfg: &Config,
    trace: &[Phase],
    directory: &Path,
) -> Result<ReplayResult, String> {
    let path = directory.join("arena.mmap");
    let shared = aerostore::Shared::create(
        &path,
        cfg.shm_mib << 20,
        &model::initial_records(cfg.families),
    )?;
    let wal_path = directory.join("aerostore.wal");
    // These helpers fork; create them before any reader/vacuum threads.
    let writer = aerostore_core::spawn_wal_writer_daemon(shared.ring.clone(), &wal_path)
        .map_err(|e| e.to_string())?;
    let mut collectors = Vec::new();
    for index in &shared.indexes {
        collectors.push(
            index
                .spawn_gc_daemon(Duration::from_millis(25))
                .map_err(|e| e.to_string())?,
        );
    }
    let vacuum = aerostore_core::spawn_vacuum_daemon_with_config(
        std::sync::Arc::clone(&shared.table),
        aerostore_core::VacuumDaemonConfig::default().with_interval(Duration::from_millis(25)),
    )
    .map_err(|e| e.to_string())?;
    let worker_cfg = WorkerConfig {
        engine: "aerostore".into(),
        attachment: Some(shared.attachment(&path)),
        pg_url: None,
        schema: String::new(),
    };
    let mut report = replay(
        cfg,
        trace,
        &worker_cfg,
        &directory.join("aerostore-worker.json"),
        || shared.snapshot(),
        || Some(shared.arena.chunked_arena().head_offset()),
    );
    // If a child is killed while holding an index lock, a graceful collector
    // join could block forever. Kill disposable daemons in the failed arena.
    if !report.passed {
        for collector in &collectors {
            let _ = collector.terminate(libc::SIGKILL);
        }
        for collector in collectors {
            let _ = collector.join();
        }
        // Vacuum has no index callback and workers never get killed while
        // holding an OCC partition lock except on operational timeout. Avoid
        // re-entering any failed arena; the benchmark process ends after report.
        let _ = writer.terminate(libc::SIGKILL);
        let _ = writer.join_any_status();
        // This coordinator is a disposable exec process. An interrupted native
        // critical section may strand vacuum's partition lock. Do not join it;
        // write the failure report and let coordinator process exit release it.
        std::mem::forget(vacuum);
        return Ok(report);
    }
    vacuum.stop().map_err(|e| e.to_string())?;
    for collector in &collectors {
        collector.stop().map_err(|e| e.to_string())?;
    }
    shared.ring.close().map_err(|e| e.to_string())?;
    writer.join().map_err(|e| e.to_string())?;
    let _ = aerostore_core::run_vacuum_pass(&shared.table).map_err(|e| e.to_string())?;
    match shared.audit() {
        Ok(audit) => report.index_audit = Some(audit),
        Err(e) => {
            report.passed = false;
            report.error = Some(e);
        }
    }
    let active = shared.arena.create_snapshot().len();
    if active != 0 {
        report.passed = false;
        report.error = Some(format!(
            "{active} active transactions remained after worker shutdown"
        ));
    }
    Ok(report)
}

#[derive(Serialize, Deserialize)]
struct AeroCoordinatorConfig {
    config: Config,
    directory: PathBuf,
}

fn isolated_aerostore_replay(cfg: &Config, directory: &Path) -> ReplayResult {
    let mut failure = ReplayResult::new("aerostore");
    let run = (|| -> Result<ReplayResult, String> {
        let path = directory.join("aerostore-coordinator.json");
        fs::write(
            &path,
            serde_json::to_vec(&AeroCoordinatorConfig {
                config: cfg.clone(),
                directory: directory.to_path_buf(),
            })
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let mut child = Command::new(std::env::current_exe().map_err(|e| e.to_string())?)
            .arg("--internal-aerostore")
            .arg(&path)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| e.to_string())?;
        let progress = directory.join("aerostore-worker.progress");
        let mut progress_time = None;
        let mut last_progress = Instant::now();
        loop {
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                let result = directory.join("aerostore-result.json");
                if !status.success() {
                    return Err(format!("Aerostore coordinator exited {status}"));
                }
                return serde_json::from_slice(&fs::read(result).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string());
            }
            let current = fs::metadata(&progress).and_then(|m| m.modified()).ok();
            if current != progress_time {
                progress_time = current;
                last_progress = Instant::now();
            }
            if last_progress.elapsed() > Duration::from_secs(60) {
                let _ = child.kill();
                let _ = child.wait();
                return Err("Aerostore coordinator made no phase progress for 60s; disposable processes terminated".into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    })();
    match run {
        Ok(report) => report,
        Err(error) => {
            failure.error = Some(error);
            failure
        }
    }
}

fn postgres_replay(
    cfg: &Config,
    trace: &[Phase],
    directory: &Path,
    url: &str,
    schema: &str,
) -> Result<ReplayResult, String> {
    postgres::initialize(url, schema, &model::initial_records(cfg.families))?;
    let worker_cfg = WorkerConfig {
        engine: "postgres".into(),
        attachment: None,
        pg_url: Some(url.into()),
        schema: schema.into(),
    };
    let report = replay(
        cfg,
        trace,
        &worker_cfg,
        &directory.join("postgres-worker.json"),
        || postgres::snapshot(url, schema),
        || None,
    );
    postgres::cleanup(url, schema)?;
    Ok(report)
}

#[derive(Serialize)]
struct Report {
    format_version: u32,
    config: Config,
    trace_fingerprint: String,
    scope: Vec<&'static str>,
    contracts: Vec<EngineContracts>,
    replays: Vec<ReplayResult>,
    completed: bool,
    operational_errors: Vec<String>,
    postgres_configuration: Option<serde_json::Value>,
    passed: bool,
}
#[derive(Serialize)]
struct EngineContracts {
    engine: String,
    cases: Vec<contracts::ContractResult>,
}

fn parse(args: &[String]) -> Result<Option<Config>, String> {
    let mut cfg = Config::default();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if matches!(arg.as_str(), "--help" | "-h") {
            println!("Extended HyperFeed Crucible (single host)\n\n  --engine both|aerostore|postgres  default both; PostgreSQL uses a disposable Docker container\n  --mode all|replay|contracts      default all; any failed native contract fails all\n  --families N                    default 32 (128 bounded logical slots each)\n  --cycles N                      default 2 (27 transaction phases per cycle)\n  --workers N                     default 4 independent local processes\n  --seed N                        default 20260922; identical fixtures for all engines\n  --shm-mib N                     default 256\n  --pg-url URL                    use an existing local PostgreSQL in a unique temporary schema\n  --output PATH                   default target/extended-crucible.json\n\nReplay checks a declared-write-set, phased synthetic model. Native contracts separately\ncheck absent-key serialization and indexed snapshots without compensating predicate locks.\nA replay-only PASS is scoped to replay. No result asserts full HyperFeed compatibility.");
            return Ok(None);
        }
        if matches!(arg.as_str(), "--bench" | "--noplot") {
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {arg}"))?;
        let number = || {
            value
                .parse::<usize>()
                .map_err(|_| format!("invalid integer for {arg}: {value}"))
        };
        match arg.as_str() {
            "--engine" => cfg.engine = value.clone(),
            "--mode" => cfg.mode = value.clone(),
            "--families" => cfg.families = number()?,
            "--cycles" => cfg.cycles = number()?,
            "--workers" => cfg.workers = number()?,
            "--shm-mib" => cfg.shm_mib = number()?,
            "--seed" => cfg.seed = value.parse().map_err(|_| "invalid seed")?,
            "--output" => cfg.output = value.into(),
            "--pg-url" => cfg.pg_url = Some(value.clone()),
            _ => return Err(format!("unknown option {arg}")),
        }
    }
    if !["both", "aerostore", "postgres"].contains(&cfg.engine.as_str())
        || !["all", "replay", "contracts"].contains(&cfg.mode.as_str())
    {
        return Err("invalid engine or mode; use --help".into());
    }
    if cfg.families == 0
        || cfg.families > 4096
        || cfg.cycles == 0
        || cfg.cycles > 1000
        || cfg.workers == 0
        || cfg.workers > 32
        || cfg.shm_mib < 32
        || cfg.shm_mib > 3584
    {
        return Err(
            "limits: families 1..4096, cycles 1..1000, workers 1..32, shm-mib 32..3584".into(),
        );
    }
    Ok(Some(cfg))
}

fn add_postgres(
    report: &mut Report,
    trace: &[Phase],
    directory: &Path,
    url: &str,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut client = loop {
        match ::postgres::Client::connect(url, ::postgres::NoTls) {
            Ok(client) => break client,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => {
                return Err(format!(
                    "PostgreSQL did not become ready within 30s: {error}"
                ))
            }
        }
    };
    let mut configuration = serde_json::Map::new();
    for setting in [
        "server_version",
        "fsync",
        "wal_writer_delay",
        "shared_buffers",
        "max_connections",
    ] {
        let value: String = client
            .query_one("SELECT current_setting($1)", &[&setting])
            .map_err(|e| e.to_string())?
            .get(0);
        configuration.insert(setting.into(), value.into());
    }
    configuration.insert("session_synchronous_commit".into(), "off".into());
    configuration.insert("isolation".into(), "serializable".into());
    report.postgres_configuration = Some(configuration.into());
    drop(client);
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_micros();
    let schema = format!("ec_{}_{nonce}", std::process::id());
    if report.config.mode != "replay" {
        report.contracts.push(EngineContracts {
            engine: "postgres".into(),
            cases: postgres::contracts(url, &format!("{schema}_contracts"))?,
        });
    }
    if report.config.mode != "contracts" {
        report.replays.push(postgres_replay(
            &report.config,
            trace,
            directory,
            url,
            &schema,
        )?);
    }
    Ok(())
}

fn write_report(report: &Report) -> Result<(), String> {
    if let Some(parent) = report
        .config
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let temporary = report
        .config
        .output
        .with_extension(format!("json.{}.tmp", std::process::id()));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(report).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    fs::rename(temporary, &report.config.output).map_err(|e| e.to_string())
}

pub fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--internal-worker") {
        return internal_worker(Path::new(args.get(1).ok_or("missing worker config")?));
    }
    if args.first().map(String::as_str) == Some("--internal-aerostore") {
        unsafe {
            let parent = libc::getppid();
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 || libc::getppid() != parent
            {
                return Err("coordinator parent disappeared".into());
            }
        }
        let input = args.get(1).ok_or("missing coordinator config")?;
        let cfg: AeroCoordinatorConfig =
            serde_json::from_slice(&fs::read(input).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        let trace = model::generate_trace(cfg.config.families, cfg.config.cycles, cfg.config.seed);
        let result = aerostore_replay(&cfg.config, &trace, &cfg.directory)?;
        fs::write(
            cfg.directory.join("aerostore-result.json"),
            serde_json::to_vec(&result).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        return Ok(());
    }
    let Some(cfg) = parse(&args)? else {
        return Ok(());
    };
    let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
    let trace = model::generate_trace(cfg.families, cfg.cycles, cfg.seed);
    let mut report = Report { format_version: 1, trace_fingerprint: fingerprint(&trace), config: cfg, contracts: Vec::new(), replays: Vec::new(), passed: false,
        completed: false, operational_errors: Vec::new(), postgres_configuration: None,
        scope: vec![
            "Synthetic single-host model; no proprietary HyperFeed code or measured production mix",
            "Identical phased trace, full returned rows and per-phase state/output parity against serial reference",
            "One process per worker, rotating assignments; distinct events ordered per family, duplicate deliveries compete",
            "128 preallocated typed slots/family; native guards before Aerostore snapshot, PostgreSQL FOR UPDATE at transaction start",
            "Aerostore direct shared memory vs PostgreSQL local client/server; not equal transport overhead",
            "WAL enabled, asynchronous acknowledgement in both; clean WAL drain checked, crash durability untested",
            "Native contract probes supply no predicate lock and determine overall compatibility status",
            "No speedup claim: rates include process dispatch/barriers and bounded fixture coordination",
        ] };
    println!(
        "extended_crucible_start families={} cycles={} workers={} phases={} trace={}",
        report.config.families,
        report.config.cycles,
        report.config.workers,
        trace.len(),
        report.trace_fingerprint
    );
    // Overwrite any old PASS before work starts. Interrupted runs remain
    // explicitly incomplete; infrastructure errors also receive a fresh report.
    write_report(&report)?;
    let execution = (|| -> Result<(), String> {
        if report.config.engine != "postgres" {
            if report.config.mode != "replay" {
                report.contracts.push(EngineContracts {
                    engine: "aerostore".into(),
                    cases: contracts::aerostore_contracts()?,
                });
            }
            if report.config.mode != "contracts" {
                report
                    .replays
                    .push(isolated_aerostore_replay(&report.config, directory.path()));
            }
        }
        if report.config.engine != "aerostore" {
            if let Some(url) = report.config.pg_url.clone() {
                add_postgres(&mut report, &trace, directory.path(), &url)?;
            } else {
                use testcontainers::{clients, core::WaitFor, GenericImage};
                let docker = clients::Cli::default();
                let image = GenericImage::new("postgres", "16")
                    .with_env_var("POSTGRES_PASSWORD", "extended_crucible")
                    .with_env_var("POSTGRES_DB", "extended_crucible")
                    .with_exposed_port(5432)
                    .with_wait_for(WaitFor::message_on_stderr(
                        "database system is ready to accept connections",
                    ));
                let arguments = [
                    "-c",
                    "fsync=on",
                    "-c",
                    "synchronous_commit=off",
                    "-c",
                    "wal_writer_delay=10s",
                ]
                .map(str::to_owned)
                .to_vec();
                let container = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    docker.run((image, arguments))
                }))
                .map_err(|_| {
                    "failed to start disposable PostgreSQL; check Docker availability".to_string()
                })?;
                let url = format!("host=127.0.0.1 port={} user=postgres password=extended_crucible dbname=extended_crucible connect_timeout=5", container.get_host_port_ipv4(5432));
                add_postgres(&mut report, &trace, directory.path(), &url)?;
            }
        }
        Ok(())
    })();
    report.completed = execution.is_ok();
    if let Err(error) = execution {
        report.operational_errors.push(error);
    }
    for engine in &report.contracts {
        for case in &engine.cases {
            println!(
                "extended_crucible_contract engine={} case={} status={} details={}",
                engine.engine,
                case.name,
                if case.passed { "PASS" } else { "FAIL" },
                case.details
            );
        }
    }
    for replay in &report.replays {
        println!("extended_crucible_replay engine={} status={} messages={} committed={} retries={} messages_per_second={:.2} p99_us={:.2} error={:?}", replay.engine, if replay.passed { "PASS" } else { "FAIL" }, replay.messages, replay.committed_messages, replay.retries, replay.committed_messages_per_second, replay.latency_p99_us, replay.error);
    }
    report.passed = report.completed
        && report.operational_errors.is_empty()
        && report
            .contracts
            .iter()
            .flat_map(|e| &e.cases)
            .all(|c| c.passed)
        && report.replays.iter().all(|r| r.passed);
    write_report(&report)?;
    println!(
        "extended_crucible_result mode={} status={} report={}",
        report.config.mode,
        if report.passed { "PASS" } else { "FAIL" },
        report.config.output.display()
    );
    if report.passed {
        Ok(())
    } else {
        Err("correctness gate failed; inspect the JSON report (replay success cannot override a failed native contract)".into())
    }
}
