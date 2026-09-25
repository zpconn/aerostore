use super::measurement::{
    calibrated_execution_summary, foreground_concurrency_report, ArrivalPlan, ExecutionSample,
    FlightOrderAudit, ForegroundExecution,
};
use super::supervision::{
    invalidate_previous_report, private_json, report_path_from_arguments,
    unique_evidence_directory, with_cleanup_result, write_json, PrivateCaseFiles, ProcessGroup,
    DEFAULT_OUTPUT,
};
use super::{aerostore, calibrated, model, oracle, postgres, remote, service, workers};
use crate::extended_crucible::{metrics::StoreMetrics, model::Record};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

#[derive(Clone, Serialize, Deserialize)]
struct Config {
    engine: String,
    mode: String,
    workload: String,
    evidence: String,
    arrival_rate: u64,
    projection_interval_seconds: u64,
    housekeeping_interval_seconds: u64,
    #[serde(default)]
    dispatch: calibrated::Dispatch,
    #[serde(default)]
    affinity_ttl_ms: u64,
    #[serde(default)]
    signature_pattern: calibrated::SignaturePattern,
    max_backlog: u64,
    pg_write_mode: postgres::WriteMode,
    rpc_delay_us: u64,
    service_bind: std::net::SocketAddr,
    remote_setup: Option<PathBuf>,
    remote_final: Option<PathBuf>,
    workers: usize,
    families: usize,
    seconds: u64,
    max_messages: usize,
    message_interval_us: u64,
    seed: u64,
    hot_percent: u32,
    shm_mib: usize,
    oracle_budget: usize,
    global_time_predicates: bool,
    pg_url: Option<String>,
    output: PathBuf,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            engine: "both".into(),
            mode: "all".into(),
            workload: "legacy".into(),
            evidence: "full".into(),
            arrival_rate: 0,
            projection_interval_seconds: 300,
            housekeeping_interval_seconds: 600,
            dispatch: calibrated::Dispatch::Identity,
            affinity_ttl_ms: 0,
            signature_pattern: calibrated::SignaturePattern::Both,
            max_backlog: 1000,
            pg_write_mode: postgres::WriteMode::Buffered,
            rpc_delay_us: 0,
            service_bind: "127.0.0.1:0".parse().unwrap(),
            remote_setup: None,
            remote_final: None,
            workers: 4,
            families: 16,
            seconds: 30,
            max_messages: 20_000,
            message_interval_us: 0,
            seed: 20260924,
            hot_percent: 80,
            shm_mib: 256,
            oracle_budget: 2_000_000,
            global_time_predicates: false,
            pg_url: None,
            output: DEFAULT_OUTPUT.into(),
        }
    }
}

fn calibrated_config(config: &Config) -> calibrated::Config {
    calibrated::Config {
        duration_ns: config.seconds * 1_000_000_000,
        foreground_rate: config.arrival_rate,
        foreground_workers: config.workers,
        families: config.families,
        seed: config.seed,
        projection_interval_seconds: config.projection_interval_seconds,
        housekeeping_interval_seconds: config.housekeeping_interval_seconds,
        dispatch: config.dispatch,
        affinity_ttl_ms: config.affinity_ttl_ms,
        signature_pattern: config.signature_pattern,
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct CaseConfig {
    config: Config,
    engine: String,
    scenario: Option<usize>,
    directory: PathBuf,
    schema: String,
}

struct Worker {
    child: Child,
    input: BufWriter<ChildStdin>,
}
impl Worker {
    fn spawn(
        config: &workers::Config,
        path: &Path,
        number: usize,
        events: mpsc::Sender<(usize, Result<workers::Reply, String>)>,
    ) -> Result<Self, String> {
        private_json(path, config)?;
        let mut child = Command::new(std::env::current_exe().map_err(|e| e.to_string())?)
            .arg("--internal-worker")
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| e.to_string())?;
        let input = BufWriter::new(child.stdin.take().ok_or("worker stdin unavailable")?);
        let output = child.stdout.take().ok_or("worker stdout unavailable")?;
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let reply = line.map_err(|e| e.to_string()).and_then(|line| {
                    serde_json::from_str(&line).map_err(|e| format!("worker protocol: {e}"))
                });
                if events.send((number, reply)).is_err() {
                    return;
                }
            }
            let _ = events.send((number, Err("worker output closed".into())));
        });
        Ok(Self { child, input })
    }
    fn send(&mut self, request: &workers::Request) -> Result<(), String> {
        serde_json::to_writer(&mut self.input, request).map_err(|e| e.to_string())?;
        writeln!(self.input).map_err(|e| e.to_string())?;
        self.input.flush().map_err(|e| e.to_string())
    }
    fn stop(&mut self) -> Result<(), String> {
        self.send(&workers::Request::Stop)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("worker exit {status}"))
                };
            }
            if Instant::now() >= deadline {
                return Err("worker shutdown exceeded 10 seconds".into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Completed {
    receipts: Vec<oracle::Receipt>,
    latencies: Vec<u64>,
    service_latencies: Vec<u64>,
    queue_delays: Vec<u64>,
    retries: u64,
    message_retries: Vec<u64>,
    worker_stops: Vec<Value>,
    metrics: StoreMetrics,
    causes: BTreeMap<String, u64>,
    diagnostics: BTreeMap<String, u64>,
    samples: Vec<Value>,
    elapsed: f64,
    admission_started_ns: u64,
    workload_completed_ns: u64,
    workers_stopped_ns: u64,
    drain_confirmed_ns: u64,
    completed_by_worker: Vec<usize>,
    calibrated_samples: Vec<ExecutionSample>,
    flight_order: FlightOrderAudit,
    foreground_executions: Vec<ForegroundExecution>,
    dispatch_audit: Value,
    active_families: usize,
    quiet_families: usize,
    offered_by_worker: Vec<u64>,
}

fn exercise(
    case: &CaseConfig,
    initial: &[Record],
    messages: Option<&[model::Message]>,
    synchronise: bool,
    attachment: Option<aerostore::Attachment>,
    service_endpoint: Option<service::Endpoint>,
    mut sample: impl FnMut() -> Result<Value, String>,
) -> Result<Completed, String> {
    let cfg = &case.config;
    // The small exact scenarios use one process per distinct message so the
    // controlled empty-search cut can never be hidden by worker queueing.
    let calibrated = (messages.is_none() && cfg.workload == "calibrated")
        .then(|| calibrated::Schedule::new(calibrated_config(cfg)))
        .transpose()?;
    let count = messages.map_or_else(
        || {
            calibrated
                .as_ref()
                .map_or(cfg.workers, |plan| plan.worker_count())
        },
        <[model::Message]>::len,
    );
    let (send, receive) = mpsc::channel();
    let mut pool = Vec::new();
    for id in 0..count {
        let worker = workers::Config {
            engine: case.engine.clone(),
            service_endpoint: service_endpoint.clone(),
            workload: cfg.workload.clone(),
            record_history: cfg.evidence == "full" || messages.is_some(),
            pg_write_mode: cfg.pg_write_mode,
            max_backlog: cfg.max_backlog,
            rpc_delay_us: cfg.rpc_delay_us,
            attachment: attachment.clone(),
            pg_url: cfg.pg_url.clone(),
            schema: case.schema.clone(),
            global_time_predicates: cfg.global_time_predicates,
            worker_id: id,
            workers: count,
            families: cfg.families,
            seed: cfg.seed,
            hot_percent: cfg.hot_percent,
            retry_limit: 128,
            message_interval_us: cfg.message_interval_us,
            expected_parent_pid: std::process::id(),
            calibrated_schedule: calibrated.as_ref().map(|plan| plan.worker_schedule(id)),
        };
        pool.push(Worker::spawn(
            &worker,
            &case.directory.join(format!("worker-{id}.json")),
            id,
            send.clone(),
        )?);
    }
    drop(send);
    let mut ready = BTreeSet::new();
    while ready.len() < count {
        let (id, event) = receive
            .recv_timeout(Duration::from_secs(30))
            .map_err(|e| format!("worker ready timeout: {e}"))?;
        match event? {
            workers::Reply::Ready if ready.insert(id) => {}
            other => return Err(format!("unexpected worker startup event: {other:?}")),
        }
    }
    let sample_start = workers::monotonic_ns();
    let initial_sample = sample()?;
    let sample_end = workers::monotonic_ns();
    let started = Instant::now();
    // A shared future epoch prevents serial startup dispatch from suppressing
    // early offered arrivals. All workers share CLOCK_MONOTONIC on this host.
    let admission_start_ns =
        workers::monotonic_ns().saturating_add(if cfg.arrival_rate > 0 && messages.is_none() {
            100_000_000
        } else {
            0
        });
    let deadline_ns = admission_start_ns.saturating_add(cfg.seconds.saturating_mul(1_000_000_000));
    let arrivals = (cfg.arrival_rate > 0 && messages.is_none() && calibrated.is_none()).then_some(
        ArrivalPlan {
            start_ns: admission_start_ns,
            duration_ns: cfg.seconds * 1_000_000_000,
            rate_per_second: cfg.arrival_rate,
            workers: count,
        },
    );
    if let Some(plan) = &arrivals {
        if (0..count).any(|worker| {
            plan.worker_offered(worker) > cfg.max_messages as u64
                || plan.worker_offered(worker) == 0
        }) {
            return Err("offered corpus must give every worker work and fit --max-messages without truncation".into());
        }
    }
    let offered_by_worker: Vec<_> = (0..count)
        .map(|worker| {
            calibrated.as_ref().map_or_else(
                || {
                    arrivals
                        .as_ref()
                        .map_or(0, |plan| plan.worker_offered(worker))
                },
                |plan| plan.worker_offered(worker),
            )
        })
        .collect();
    if calibrated.is_some() {
        if offered_by_worker
            .iter()
            .any(|offered| *offered > cfg.max_messages as u64)
        {
            return Err("calibrated offered corpus exceeds --max-messages for a worker; no jobs may be truncated".into());
        }
        write_json(
            &case.directory.join("offered-schedule.json"),
            &json!({
            "format":"calibrated-fixed-timeline-v1","config":calibrated_config(cfg),
            "admission_started_ns":admission_start_ns,"admission_finished_ns":deadline_ns,
            "offered_by_worker":offered_by_worker,"offered_messages":offered_by_worker.iter().sum::<u64>(),
            "scope":"Compact deterministic schedule includes every admitted foreground message and maintenance tick, independent of completions; failed runs retain uncompleted offered work."}),
        )?;
    }
    for (id, worker) in pool.iter_mut().enumerate() {
        let request = match messages {
            Some(messages) => workers::Request::Run {
                messages: vec![messages[id].clone()],
                synchronise_first_query: synchronise,
            },
            None if calibrated.is_some() => workers::Request::Calibrated {
                start_ns: admission_start_ns,
                config: calibrated_config(cfg),
                max_messages: cfg.max_messages,
            },
            None => workers::Request::Sustain {
                deadline_ns,
                max_messages: cfg.max_messages,
                arrivals: arrivals.clone(),
            },
        };
        worker.send(&request)?;
    }
    let mut result = Completed {
        receipts: Vec::new(),
        latencies: Vec::new(),
        service_latencies: Vec::new(),
        queue_delays: Vec::new(),
        retries: 0,
        message_retries: Vec::new(),
        worker_stops: vec![Value::Null; count],
        metrics: StoreMetrics::default(),
        causes: BTreeMap::new(),
        diagnostics: BTreeMap::new(),
        samples: Vec::new(),
        elapsed: 0.,
        admission_started_ns: admission_start_ns,
        workload_completed_ns: 0,
        workers_stopped_ns: 0,
        drain_confirmed_ns: 0,
        completed_by_worker: vec![0; count],
        calibrated_samples: Vec::new(),
        flight_order: FlightOrderAudit::default(),
        foreground_executions: Vec::new(),
        dispatch_audit: calibrated
            .as_ref()
            .map_or(Value::Null, |plan| plan.dispatch_report()),
        active_families: calibrated.as_ref().map_or(0, |plan| plan.active_families()),
        quiet_families: calibrated.as_ref().map_or(0, |plan| plan.quiet_families()),
        offered_by_worker,
    };
    result.samples.push(json!({"elapsed_seconds":0.0,"completed_messages":0,"storage":initial_sample,"sample_started_ns":sample_start,"sample_finished_ns":sample_end}));
    let mut last_sample = Instant::now();
    let mut last_progress = Instant::now();
    let mut first_queries = BTreeSet::new();
    let mut done = BTreeSet::new();
    // Raw successful receipts are retained, even if a later operation fails.
    let mut history = BufWriter::new(
        fs::File::create(case.directory.join("history.jsonl")).map_err(|e| e.to_string())?,
    );
    while done.len() < count {
        match receive.recv_timeout(Duration::from_millis(100)) {
            Ok((id, event)) => {
                if done.contains(&id) {
                    return Err(format!("worker {id} emitted after Done"));
                }
                match event? {
                    workers::Reply::FirstQuery { rows }
                        if synchronise && first_queries.insert(id) =>
                    {
                        if rows != 0 {
                            return Err(
                                "competing creation did not observe an empty candidate query"
                                    .into(),
                            );
                        }
                        if first_queries.len() == count {
                            for worker in &mut pool {
                                worker.send(&workers::Request::Continue)?;
                            }
                        }
                    }
                    workers::Reply::Observation {
                        receipt,
                        latency_ns,
                        message_started_ns,
                        scheduled_ns,
                        retries,
                    } => {
                        let received_ns = workers::monotonic_ns();
                        if message_started_ns > receipt.started
                            || receipt.finished > received_ns
                            || receipt.finished < message_started_ns
                            || latency_ns != receipt.finished - message_started_ns
                        {
                            return Err("invalid message/receipt timestamps".into());
                        }
                        let sequence =
                            result.completed_by_worker[id] as u64 * count as u64 + id as u64;
                        let calibrated_event = calibrated
                            .as_ref()
                            .map(|plan| {
                                plan.event(id, result.completed_by_worker[id] as u64)
                                    .ok_or("worker returned more calibrated events than offered")
                            })
                            .transpose()?;
                        let expected_scheduled = calibrated_event.as_ref().map_or_else(
                            || {
                                arrivals
                                    .as_ref()
                                    .and_then(|plan| plan.scheduled_ns(sequence))
                            },
                            |event| admission_start_ns.checked_add(event.offset_ns),
                        );
                        if scheduled_ns != expected_scheduled
                            || scheduled_ns.is_some_and(|at| message_started_ns < at)
                        {
                            return Err(
                                "message arrival differs from fixed offered schedule".into()
                            );
                        }
                        let end_to_end_ns =
                            received_ns - scheduled_ns.unwrap_or(message_started_ns);
                        if messages.is_some_and(|m| receipt.message != m[id]) {
                            return Err("worker returned wrong scenario message".into());
                        }
                        if let Some(expected) = messages {
                            if result.completed_by_worker[id] != 0 || expected.len() != count {
                                return Err("duplicate scenario completion".into());
                            }
                        } else if let Some(expected) = &calibrated_event {
                            if receipt.message != expected.message {
                                return Err("calibrated event differs from independently reconstructed offered schedule".into());
                            }
                            match (expected.class, expected.logical_identity, expected.foreground_ordinal) {
                                (calibrated::EventClass::Foreground, Some(identity), Some(ordinal)) => {
                                    if cfg.dispatch == calibrated::Dispatch::Identity {
                                        result.flight_order.observe(identity, ordinal, message_started_ns, receipt.finished)?;
                                    } else {
                                        result.foreground_executions.push(ForegroundExecution {
                                            identity, ordinal, started_ns: message_started_ns, finished_ns: receipt.finished,
                                        });
                                    }
                                },
                                (calibrated::EventClass::Projection | calibrated::EventClass::Housekeeping, None, None) => (),
                                _ => return Err("calibrated event has inconsistent foreground ordering metadata".into()),
                            }
                            let effect = &receipt.body.outcome;
                            result.calibrated_samples.push(ExecutionSample {
                                worker: id,
                                class: expected.class.name(),
                                scheduled_ns: scheduled_ns
                                    .ok_or("calibrated event lacks scheduled time")?,
                                started_ns: message_started_ns,
                                finished_ns: receipt.finished,
                                received_ns,
                                retries,
                                positive_effect: effect.updated_views > 0
                                    || effect.created_views > 0
                                    || effect.expired_records > 0
                                    || effect.claimed_events > 0
                                    || effect.cancelled_events > 0
                                    || effect.rescheduled_events > 0
                                    || effect.expired_families > 0
                                    || !effect.outputs.is_empty(),
                            });
                        } else {
                            let sequence =
                                result.completed_by_worker[id] as u64 * count as u64 + id as u64;
                            let expected = model::sustained_message_for(
                                &cfg.workload,
                                sequence,
                                id,
                                count,
                                cfg.families,
                                cfg.seed,
                                cfg.hot_percent,
                            );
                            if receipt.message != expected {
                                return Err(
                                    "sustained message stream differs from deterministic generator"
                                        .into(),
                                );
                            }
                        }
                        serde_json::to_writer(&mut history, &json!({"worker":id,"service_latency_ns":latency_ns,"end_to_end_latency_ns":end_to_end_ns,"message_started_ns":message_started_ns,"scheduled_ns":scheduled_ns,"received_ns":received_ns,"retries":retries,"receipt":receipt,
                            "workload_class":calibrated_event.as_ref().map(|e|e.class.name()),
                            "logical_identity":calibrated_event.as_ref().and_then(|e|e.logical_identity),
                            "foreground_ordinal":calibrated_event.as_ref().and_then(|e|e.foreground_ordinal)}))
                            .map_err(|e| e.to_string())?;
                        writeln!(history).map_err(|e| e.to_string())?;
                        result.receipts.push(receipt);
                        result.latencies.push(end_to_end_ns);
                        result.service_latencies.push(latency_ns);
                        result
                            .queue_delays
                            .push(scheduled_ns.map_or(0, |at| message_started_ns - at));
                        result.retries += retries;
                        result.message_retries.push(retries);
                        result.completed_by_worker[id] += 1;
                    }
                    workers::Reply::Done {
                        metrics,
                        retry_causes,
                        diagnostics,
                        maximum_backlog,
                        finished_ns,
                        stop_reason,
                    } if done.insert(id) => {
                        if finished_ns > workers::monotonic_ns()
                            || (stop_reason == "deadline" && finished_ns < deadline_ns)
                        {
                            return Err("invalid worker finish clock".into());
                        }
                        if messages.is_some() && stop_reason != "scenario_complete" {
                            return Err("scenario worker stopped early".into());
                        }
                        if messages.is_none()
                            && !["deadline", "message_cap", "arrival_corpus_drained"]
                                .contains(&stop_reason.as_str())
                        {
                            return Err("invalid sustained stop reason".into());
                        }
                        if let Some(plan) = &arrivals {
                            if stop_reason != "arrival_corpus_drained"
                                || finished_ns < deadline_ns
                                || result.completed_by_worker[id] as u64 != plan.worker_offered(id)
                            {
                                return Err("worker did not drain its entire offered corpus".into());
                            }
                        }
                        if calibrated.is_some()
                            && (stop_reason != "arrival_corpus_drained"
                                || finished_ns < deadline_ns
                                || result.completed_by_worker[id] as u64
                                    != result.offered_by_worker[id])
                        {
                            return Err("calibrated worker did not drain every independently admitted event".into());
                        }
                        result.worker_stops[id] = json!({"finished_ns":finished_ns,"stop_reason":stop_reason,"maximum_backlog":maximum_backlog});
                        for (name, value) in diagnostics {
                            *result.diagnostics.entry(name).or_default() += value;
                        }
                        result.metrics.add(&metrics);
                        for (cause, count) in retry_causes {
                            *result.causes.entry(cause).or_default() += count;
                        }
                    }
                    workers::Reply::Error { message } => {
                        history.flush().map_err(|e| e.to_string())?;
                        write_json(
                            &case.directory.join("failure-progress.json"),
                            &json!({
                            "passed":false,"execution_completed":false,"worker":id,"error":message,
                            "completed_by_worker":result.completed_by_worker,"offered_by_worker":result.offered_by_worker,
                            "completed_messages":result.receipts.len(),"completed_message_retries":result.retries,
                            "note":"Offered schedule remains authoritative; failed message retry count is retained in worker error. Uncompleted offered events are not dropped or called successful."}),
                        )?;
                        return Err(format!("worker {id}: {message}"));
                    }
                    other => return Err(format!("unexpected worker event: {other:?}")),
                }
                last_progress = Instant::now();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("all worker streams closed before completion".into())
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if last_sample.elapsed() >= Duration::from_secs(1) {
            history.flush().map_err(|e| e.to_string())?;
            let sample_start = workers::monotonic_ns();
            let storage = sample()?;
            let sample_end = workers::monotonic_ns();
            result
                .samples
                .push(json!({"elapsed_seconds":started.elapsed().as_secs_f64(),
                "sample_started_ns":sample_start,"sample_finished_ns":sample_end,
                "completed_messages":result.receipts.len(),"storage":storage}));
            write_json(
                &case.directory.join("progress.json"),
                &json!({"passed":false,"execution_completed":false,"completed_messages":result.receipts.len(),"completed_by_worker":result.completed_by_worker,"offered_by_worker":result.offered_by_worker,"retries":result.retries,"retention_samples":result.samples,
                    "due_uncompleted_by_worker":calibrated.as_ref().map(|plan|(0..count).map(|worker|plan.backlog(worker,result.completed_by_worker[worker] as u64,workers::monotonic_ns().saturating_sub(admission_start_ns))).collect::<Vec<_>>())}),
            )?;
            last_sample = Instant::now();
        }
        if last_progress.elapsed() > Duration::from_secs(60) {
            return Err("no worker progress for 60 seconds".into());
        }
    }
    result.workload_completed_ns = workers::monotonic_ns();
    result.elapsed = (result.workload_completed_ns - admission_start_ns) as f64 / 1e9;
    history.flush().map_err(|e| e.to_string())?;
    for worker in &mut pool {
        worker.stop()?;
    }
    result.workers_stopped_ns = workers::monotonic_ns();
    for id in 0..count {
        let _ = fs::remove_file(case.directory.join(format!("worker-{id}.json")));
    }
    if result.receipts.is_empty()
        || (calibrated.is_none() && result.completed_by_worker.contains(&0))
    {
        return Err("a worker completed zero messages".into());
    }
    if result.metrics.commits != result.receipts.len() as u64 {
        return Err("receipt count differs from successful native commits".into());
    }
    if synchronise && first_queries.len() != count {
        return Err("required simultaneous empty-search cut was not reached".into());
    }
    let _ = initial;
    Ok(result)
}

fn summarize(
    case: &CaseConfig,
    initial: &[Record],
    final_rows: &[Record],
    mut completed: Completed,
    audit: Value,
    drained: Value,
) -> Result<Value, String> {
    if !(completed.admission_started_ns < completed.workload_completed_ns
        && completed.workload_completed_ns <= completed.workers_stopped_ns
        && completed.workers_stopped_ns <= completed.drain_confirmed_ns)
    {
        return Err("invalid continuous admission-to-drain timestamps".into());
    }
    let elapsed_including_drain =
        (completed.drain_confirmed_ns - completed.admission_started_ns) as f64 / 1e9;
    let invariants = model::validate_snapshot(final_rows);
    let invariant_passed = invariants.is_ok();
    let invariants = match invariants {
        Ok(report) => json!({"passed":true,"checks":report}),
        Err(error) => json!({"passed":false,"error":error}),
    };
    let mut per_kind = BTreeMap::<String, (Vec<u64>, u64, [usize; 12])>::new();
    for (i, receipt) in completed.receipts.iter().enumerate() {
        let name = receipt.message.kind.name();
        let (latencies, retries, outcome) = per_kind.entry(name.into()).or_default();
        latencies.push(completed.latencies[i]);
        *retries += completed.message_retries[i];
        outcome[0] += receipt.body.outcome.updated_views;
        outcome[1] += receipt.body.outcome.created_views;
        outcome[2] += receipt.body.outcome.ignored_stale;
        outcome[3] += receipt.body.outcome.expired_records;
        outcome[4] += receipt.body.outcome.outputs.len();
        outcome[5] += usize::from(receipt.body.outcome.missing_family);
        outcome[6] += usize::from(receipt.body.outcome.allocation_deferred);
        outcome[7] += receipt.body.outcome.claimed_events;
        outcome[8] += receipt.body.outcome.cancelled_events;
        outcome[9] += receipt.body.outcome.rescheduled_events;
        outcome[10] += receipt.body.outcome.expired_families;
        outcome[11] += usize::from(receipt.body.outcome.duplicate);
    }
    let per_kind: BTreeMap<_,_> = per_kind.into_iter().map(|(name,(mut latencies,retries,outcome))| {
        latencies.sort_unstable();
        (name,json!({"completed":latencies.len(),"retries":retries,"p99_us_including_retries":latencies[(latencies.len()*99).div_ceil(100)-1] as f64 / 1000.0,"outcomes":{"updated_views":outcome[0],"created_views":outcome[1],"ignored_stale":outcome[2],"expired_records":outcome[3],"outputs":outcome[4],"missing_family":outcome[5],"allocation_deferred":outcome[6],"claimed_events":outcome[7],"cancelled_events":outcome[8],"rescheduled_events":outcome[9],"expired_families":outcome[10],"duplicate_messages":outcome[11]}}))
    }).collect();
    completed.latencies.sort_unstable();
    let quantile = |percent: usize| {
        completed.latencies[(completed.latencies.len() * percent).div_ceil(100) - 1] as f64 / 1000.
    };
    write_json(&case.directory.join("initial.json"), &initial)?;
    write_json(&case.directory.join("final.json"), &final_rows)?;
    let checking = Instant::now();
    let history_checked = case.config.evidence == "full" || case.scenario.is_some();
    let witness = if history_checked {
        serde_json::to_value(oracle::check(
            initial,
            &completed.receipts,
            final_rows,
            case.config.oracle_budget,
        ))
        .map_err(|e| e.to_string())?
    } else {
        json!({"status":"NotCheckedMetricsOnly","explored":0,"detail":"Operation histories intentionally disabled; final invariants are not a serial history proof."})
    };
    write_json(&case.directory.join("serial-witness.json"), &witness)?;
    let passed = (!history_checked || witness["status"] == "Valid") && invariant_passed;
    let mut service_latencies = completed.service_latencies.clone();
    service_latencies.sort_unstable();
    completed.queue_delays.sort_unstable();
    let p99 = |values: &[u64]| values[(values.len() * 99).div_ceil(100) - 1] as f64 / 1000.0;
    let count = completed.receipts.len();
    let mut kinds = BTreeMap::<String, usize>::new();
    for receipt in &completed.receipts {
        let name = receipt.message.kind.name();
        *kinds.entry(name.into()).or_default() += 1;
    }
    let mut report = json!({"engine":case.engine,"scenario":case.scenario.map_or("sustained-mixed".to_owned(), |i| model::scenarios(case.config.seed)[i].name.clone()),
        "passed":passed,"execution_completed":true,"oracle_status":witness["status"],"oracle_explored":witness["explored"],
        "history_checked":history_checked,"correctness_history_verified":history_checked && witness["status"] == "Valid",
        "oracle_detail":witness["detail"],"oracle_seconds":checking.elapsed().as_secs_f64(),
        "completed_messages":count,"completed_by_worker":completed.completed_by_worker,"message_kinds":kinds,
        "total_process_workers":completed.completed_by_worker.len(),
        "initial_fleet":fleet_population(initial),"final_fleet":fleet_population(final_rows),
        "per_kind":per_kind,"invariants":invariants,"worker_stops":completed.worker_stops,
        "elapsed_seconds":completed.elapsed,"completed_messages_per_second":count as f64 / completed.elapsed,
        "admission_started_ns":completed.admission_started_ns,"workload_completed_ns":completed.workload_completed_ns,
        "workers_stopped_ns":completed.workers_stopped_ns,"drain_confirmed_ns":completed.drain_confirmed_ns,
        "elapsed_seconds_including_drain":elapsed_including_drain,
        "completed_messages_per_second_including_drain":count as f64 / elapsed_including_drain,
        "timing_scope":"continuous_client_monotonic",
        "message_latency_p50_us_including_retries":quantile(50),"message_latency_p99_us_including_retries":quantile(99),
        "message_latency_max_us_including_retries":quantile(100),"retries":completed.retries,"retry_causes":completed.causes,
        "store_metrics":completed.metrics,"operation_diagnostics":completed.diagnostics,
        "service_latency_p99_us_including_retries":p99(&service_latencies),"arrival_queue_delay_p99_us":p99(&completed.queue_delays),
        "arrival_mode":if case.config.arrival_rate > 0 && case.scenario.is_none() {"independent_fixed_corpus"} else {"closed_loop"},
        "offered_messages":if case.config.arrival_rate > 0 && case.scenario.is_none() {json!(case.config.arrival_rate * case.config.seconds)} else {Value::Null},
        "admission_seconds":case.config.seconds,"offered_rate_per_second":case.config.arrival_rate,"retention_samples":completed.samples,"after_drain":drained,"native_audit":audit,
        "evidence_directory":case.directory,"timing_contract":"message latency measures scheduled arrival (open loop) or first attempt (closed loop) through coordinator receipt, including queue, transaction RPC, retries/backoff and result IPC; service latency separately excludes arrival queue and receipt IPC; scenarios include deliberate barrier; drained throughput uses a continuous client monotonic interval from admission through worker shutdown and drain confirmation, including intervening cleanup and remote confirmation delivery; local oracle/final audit outside timing",
        "retry_cause_limit":"native stages do not distinguish exact predicate bucket collisions, row validation or underlying newer-stamp causes; PostgreSQL reports SQLSTATE",
        "requested_duration_reached":case.scenario.is_none() && completed.worker_stops.iter().all(|s| s["stop_reason"] == "deadline" || s["stop_reason"] == "arrival_corpus_drained"),
        "worker_message_cap_reached":completed.worker_stops.iter().any(|s| s["stop_reason"] == "message_cap"),
        "performance_comparison_eligible":false,
        "performance_scope":"diagnostic instrumented workload; repeated matched trials, noise analysis and availability acceptance are required before architecture promotion",
        "worker_failure_availability_tested":false});
    if case.scenario.is_none() && case.config.workload == "calibrated" {
        let measured = calibrated_execution_summary(
            &completed.calibrated_samples,
            &completed.offered_by_worker,
            case.config.workers,
            completed.drain_confirmed_ns - completed.admission_started_ns,
        )?;
        report
            .as_object_mut()
            .unwrap()
            .extend(measured.as_object().unwrap().clone());
        report["per_flight_order"] = if case.config.dispatch == calibrated::Dispatch::Identity {
            completed.flight_order.report()
        } else {
            foreground_concurrency_report(&completed.foreground_executions)?
        };
        report["dispatch_audit"] = completed.dispatch_audit;
        report["dispatch_audit"]["checked"] = json!(true);
        report["dispatch_audit"]["passed"] = json!(true);
        report["dispatch_audit"]["scope"] = json!("Coordinator matched every worker receipt to its deterministic offered signature-dispatch schedule; worker preparation precedes admission. The assignment fingerprint is a reproducibility check, not a cryptographic commitment.");
        report["arrival_mode"] = json!("calibrated_fixed_timeline");
        report["offered_messages"] = json!(completed.offered_by_worker.iter().sum::<u64>());
        report["offered_rate_scope"] = json!("foreground_only");
        report["calibrated_schedule"] = json!({
            "foreground_workers":case.config.workers,"maintenance_workers":2,
            "active_families":completed.active_families,"quiet_families":completed.quiet_families,
            "projection_interval_seconds":case.config.projection_interval_seconds,
            "housekeeping_interval_seconds":case.config.housekeeping_interval_seconds,
            "first_timer_tick":"after_one_interval","timer_admission":"strictly_before_end",
            "per_flight_ordering":if case.config.dispatch == calibrated::Dispatch::Identity {"stable_foreground_worker_fifo"} else {"signature_affinity_worker_fifo"},"clock":"wall_clock",
            "dispatch":case.config.dispatch,"affinity_ttl_ms":case.config.affinity_ttl_ms,"signature_pattern":case.config.signature_pattern,
            "cadence":if case.config.projection_interval_seconds<300 || case.config.housekeeping_interval_seconds<300 {"accelerated"} else if (300..=600).contains(&case.config.projection_interval_seconds) && (300..=600).contains(&case.config.housekeeping_interval_seconds) {"representative_interval_config"} else {"custom_outside_calibration"},
            "projection_batch_limit":4,"housekeeping_batch_limit":32,
            "maintenance_scope":"bounded_batch_not_full_sweep",
            "population_turnover_tested":false,"global_maintenance_sweep_complete":false,
            "scope":"Partial calibration of foreground dispatch and wall-clock maintenance cadence; fixed populated cohorts, no flight lifecycle/turnover, bounded maintenance batches, no proprietary HyperFeed compatibility claim.",
            "ordering_assumption":if case.config.dispatch == calibrated::Dispatch::Identity {"Permanent identity routing is a FIFO comparison control."} else {"Only each worker's queue is FIFO; aliases or affinity expiry can overlap or reorder the same flight. TTL uses scheduled arrival time, with sliding refresh on hits; aliases and TTL are explicit synthetic parameters."}});
        report["calibrated_schedule"]["timer_event_time"] = json!("Scheduled tick time; delayed jobs retain their admitted timestamp and every tick is drained, without coalescing.");
        report["calibrated_schedule"]["backlog_measurement"] = json!("Maximum sampled due-but-unfinished jobs at worker scheduling boundaries; not a continuous queue maximum.");
        report["population_turnover_tested"] = json!(false);
        report["global_maintenance_sweep_complete"] = json!(false);
    }
    Ok(report)
}

fn fleet_population(rows: &[Record]) -> Value {
    use crate::extended_crucible::model::{FLIGHT, POSITION, SCHEDULED};
    let active: Vec<_> = rows.iter().filter(|row| row.active).collect();
    let families: BTreeSet<_> = active
        .iter()
        .filter(|row| row.kind == FLIGHT)
        .map(|row| row.family)
        .collect();
    json!({"live_families":families.len(),"flight_views":active.iter().filter(|row|row.kind==FLIGHT).count(),
        "scheduled_events":active.iter().filter(|row|row.kind==SCHEDULED).count(),
        "retained_positions":active.iter().filter(|row|row.kind==POSITION).count(),"active_rows":active.len(),
        "scope":"initial/final snapshot population, not a measured runtime minimum"})
}

fn coordinator(case: &CaseConfig) -> Result<Value, String> {
    let scenario = case
        .scenario
        .map(|i| model::scenarios(case.config.seed).remove(i));
    let initial = if scenario.is_none() && case.config.workload == "calibrated" {
        calibrated::initial_records(case.config.families, case.config.seed)?
    } else {
        scenario.as_ref().map_or_else(
            || {
                model::sustained_initial_for(
                    &case.config.workload,
                    case.config.families,
                    case.config.seed,
                )
            },
            |s| s.initial.clone(),
        )
    };
    let messages = scenario.as_ref().map(|s| s.messages.as_slice());
    let synchronise = scenario.as_ref().is_some_and(|s| s.synchronise_first_query);
    if case.engine == "service-remote" {
        let setup_path = case
            .config
            .remote_setup
            .as_ref()
            .ok_or("missing remote setup")?;
        let final_path = case
            .config
            .remote_final
            .as_ref()
            .ok_or("missing remote final")?;
        let setup: remote::FrameSetup =
            serde_json::from_slice(&fs::read(setup_path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        if setup.version != 1
            || setup.run_id.is_empty()
            || setup.workload != case.config.workload
            || setup.families != case.config.families
            || setup.seed != case.config.seed
            || setup.projection_interval_seconds != case.config.projection_interval_seconds
            || setup.housekeeping_interval_seconds != case.config.housekeeping_interval_seconds
            || setup.dispatch != case.config.dispatch
            || setup.affinity_ttl_ms != case.config.affinity_ttl_ms
            || setup.signature_pattern != case.config.signature_pattern
            || setup.global_time_predicates != case.config.global_time_predicates
            || setup.initial_rows != initial
            || setup.max_seconds <= case.config.seconds
            || !matches!(setup.endpoint, service::Endpoint::Tcp(_))
        {
            return Err("remote setup does not match this bounded workload".into());
        }
        // Worker timestamps and arrival scheduling belong entirely to the client
        // host. Server samples have their own epoch and are reported separately.
        let mut completed = exercise(
            case,
            &initial,
            None,
            false,
            None,
            Some(setup.endpoint.clone()),
            || Ok(Value::Null),
        )?;
        write_json(
            &setup_path.with_extension("client-complete.json"),
            &json!({"run_id":setup.run_id,"completed":true}),
        )?;
        let deadline = Instant::now() + Duration::from_secs(120);
        let final_frame: remote::FrameFinal = loop {
            match fs::read(final_path) {
                Ok(bytes) => break serde_json::from_slice(&bytes).map_err(|e| e.to_string())?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                Err(e) => return Err(format!("remote final frame unavailable: {e}")),
            }
        };
        if final_frame.version != 1
            || final_frame.run_id != setup.run_id
            || !final_frame.passed
            || final_frame.registrations_after_stop != 0
            || !final_frame.completion_drain_seconds.is_finite()
            || final_frame.completion_drain_seconds < 0.0
        {
            return Err(format!(
                "remote owner failed or final frame mismatched: {:?}",
                final_frame.error
            ));
        }
        completed.drain_confirmed_ns = workers::monotonic_ns();
        let mut report = summarize(
            case,
            &initial,
            &final_frame.final_rows,
            completed,
            final_frame.audit,
            final_frame.after_drain,
        )?;
        report["service_stats"] =
            serde_json::to_value(final_frame.service_stats).map_err(|e| e.to_string())?;
        report["transport"] = json!("tcp_external_owner");
        report["physical_remote_workers_measured"] = json!(false);
        report["topology_note"] = json!("A separate owner process is established; distinct physical hosts require orchestration evidence, never inferred from TCP.");
        report["server_retention_samples"] = json!(final_frame.retention_samples);
        report["completion_drain_seconds"] = json!(final_frame.completion_drain_seconds);
        report["wal_bytes_after_drain"] = json!(final_frame.wal_bytes_after_drain);
        report["remote_run_id"] = json!(setup.run_id);
        return Ok(report);
    }
    if case.engine == "postgres" {
        let url = case
            .config
            .pg_url
            .as_deref()
            .ok_or("missing PostgreSQL URL")?;
        postgres::initialize(url, &case.schema, &initial)?;
        let plans = postgres::query_plan_audit(url, &case.schema)?;
        let execution = (|| {
            let mut completed =
                exercise(case, &initial, messages, synchronise, None, None, || {
                    postgres::retention(url, &case.schema)
                })?;
            let drain = postgres::drain(url, &case.schema)?;
            completed.drain_confirmed_ns = workers::monotonic_ns();
            let final_rows = postgres::snapshot(url, &case.schema)?;
            let mut report = summarize(
                case,
                &initial,
                &final_rows,
                completed,
                Value::Null,
                postgres::retention(url, &case.schema)?,
            )?;
            report["query_plan_audit"] = plans;
            report["wal_drain"] = drain;
            report["transport"] = json!("postgres_connection_string_recorded_privately");
            Ok(report)
        })();
        let cleanup = postgres::cleanup(url, &case.schema);
        return match (execution, cleanup) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
        };
    }
    let path = case.directory.join("arena.mmap");
    let shared = aerostore::Shared::create(&path, case.config.shm_mib << 20, &initial)?;
    // Fork helpers before reader/vacuum threads exist in this coordinator.
    let writer =
        aerostore_core::spawn_wal_writer_daemon(shared.ring.clone(), &case.directory.join("wal"))
            .map_err(|e| e.to_string())?;
    let collectors = shared
        .indexes
        .iter()
        .map(|i| {
            i.spawn_gc_daemon(Duration::from_millis(25))
                .map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Error unwinding must not join a collector blocked by an abandoned native
    // lock. The isolated coordinator's process group is killed by its parent.
    let vacuum = std::mem::ManuallyDrop::new(
        aerostore_core::spawn_vacuum_daemon_with_config(
            Arc::clone(&shared.table),
            aerostore_core::VacuumDaemonConfig::default().with_interval(Duration::from_millis(25)),
        )
        .map_err(|e| e.to_string())?,
    );
    let socket_directory = if case.engine == "service-unix" {
        Some(
            tempfile::Builder::new()
                .prefix("aero-ipc-")
                .tempdir()
                .map_err(|e| e.to_string())?,
        )
    } else {
        None
    };
    let mut server = if case.engine.starts_with("service-") {
        let endpoint = if let Some(directory) = &socket_directory {
            service::BindEndpoint::Unix(directory.path().join("engine.sock"))
        } else {
            service::BindEndpoint::Tcp("127.0.0.1:0".parse().unwrap())
        };
        let attachment = shared.attachment(&path);
        let global = case.config.global_time_predicates;
        let mut limits = service::Limits::default();
        limits.idle_timeout = Duration::from_secs(case.config.seconds + 300);
        Some(service::Server::start(endpoint, limits, move |session| {
            let mapping = aerostore::Shared::attach(&attachment)?;
            let mut adapter = aerostore::Adapter::new(&mapping, global);
            session.serve(&mut adapter, |adapter| service::BackendMetrics {
                metrics: adapter.metrics.clone(),
                retry_causes: adapter.retry_causes.clone(),
                diagnostics: adapter.diagnostics.clone(),
            })
        })?)
    } else {
        None
    };
    let mut completed = exercise(
        case,
        &initial,
        messages,
        synchronise,
        if server.is_none() {
            Some(shared.attachment(&path))
        } else {
            None
        },
        server.as_ref().map(|s| s.endpoint().clone()),
        || aerostore::retention(&shared),
    )?;
    let drain_started = Instant::now();
    let service_stats = match &mut server {
        Some(server) => serde_json::to_value(server.stop(Duration::from_secs(10))?)
            .map_err(|e| e.to_string())?,
        None => Value::Null,
    };
    vacuum.stop().map_err(|e| e.to_string())?;
    for collector in &collectors {
        collector.stop().map_err(|e| e.to_string())?;
    }
    shared.ring.close().map_err(|e| e.to_string())?;
    writer.join().map_err(|e| e.to_string())?;
    aerostore_core::run_vacuum_pass(&shared.table).map_err(|e| e.to_string())?;
    completed.drain_confirmed_ns = workers::monotonic_ns();
    let completion_drain_seconds = drain_started.elapsed().as_secs_f64();
    let final_rows = shared.snapshot()?;
    let audit = shared.audit()?;
    if !shared.arena.create_snapshot().is_empty() {
        return Err("registrations remain after worker exit".into());
    }
    let mut report = summarize(
        case,
        &initial,
        &final_rows,
        completed,
        audit,
        aerostore::retention(&shared)?,
    )?;
    report["service_stats"] = service_stats;
    report["transport"] = json!(match case.engine.as_str() {
        "service-unix" => "unix_socket",
        "service-tcp" => "tcp_loopback",
        _ => "direct_shared_mapping",
    });
    report["physical_remote_workers_measured"] = json!(false);
    report["completion_drain_seconds"] = json!(completion_drain_seconds);
    report["wal_bytes_after_drain"] = json!(fs::metadata(case.directory.join("wal"))
        .map(|m| m.len())
        .unwrap_or(0));
    // Full logical evidence is retained; large disposable storage isn't an archive.
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(case.directory.join("wal"));
    Ok(report)
}

fn cleanup_postgres_case(case: &CaseConfig, config_path: &Path) -> Result<(), String> {
    if case.engine != "postgres" {
        return Ok(());
    }
    // Even if the coordinator died before its cleanup path, its workers have
    // now been killed. Bound cleanup separately, including connection hangs.
    // The adapter checks the schema's ownership marker before dropping it.
    let mut command = Command::new(std::env::current_exe().map_err(|e| e.to_string())?);
    command
        .arg("--internal-pg-cleanup")
        .arg(config_path)
        .stdin(Stdio::null());
    let mut cleanup = ProcessGroup::spawn(command)?;
    let status = cleanup.wait(Duration::from_secs(75))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("PostgreSQL cleanup process exited {status}"))
    }
}

fn isolated_case(case: &CaseConfig) -> Result<Value, String> {
    fs::create_dir(&case.directory).map_err(|e| e.to_string())?;
    let _private_files = PrivateCaseFiles(case.directory.clone());
    let config_path = case.directory.join("private-config.json");
    private_json(&config_path, case)?;
    let execution = (|| {
        let mut command = Command::new(std::env::current_exe().map_err(|e| e.to_string())?);
        command
            .arg("--internal-coordinator")
            .arg(&config_path)
            .stdin(Stdio::null());
        let mut child = ProcessGroup::spawn(command)?;
        let status = child.wait(Duration::from_secs(case.config.seconds + 300))?;
        if !status.success() {
            return Err(format!("coordinator exited {status}"));
        }
        serde_json::from_slice(
            &fs::read(case.directory.join("result.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())
    })();
    // The group guard has already terminated/reaped the coordinator here;
    // external database cleanup cannot race a surviving fixture worker.
    let cleanup = cleanup_postgres_case(case, &config_path);
    with_cleanup_result(execution, cleanup, &case.schema)
}

fn parse() -> Result<Option<Config>, String> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("HyperFeed contention Crucible\n--engine both|aerostore|postgres|service-unix|service-tcp|service-remote --mode all|scenarios|sustained|serve\n--workers 4 --families 16 --seconds 30 --max-messages 20000 (per worker) --message-interval-us 0\n--workload legacy|lifecycle|fleet|calibrated --evidence full|metrics (metrics requires sustained)\n--arrival-rate 0 (0=closed loop; positive=fixed arrivals per second) --max-backlog 1000\n--projection-interval-seconds 300 --housekeeping-interval-seconds 600 (calibrated only; wall seconds; workers means foreground plus two maintenance processes)\n--dispatch identity|signature-affinity --affinity-ttl-ms N (positive explicit TTL required for affinity) --signature-pattern both|mixed (calibrated only)\n--pg-write-mode buffered|immediate --rpc-delay-us 0 (service sensitivity only)\n--service-bind 127.0.0.1:0 (serve mode) --remote-setup FILE --remote-final FILE (service-remote)\n--seed 20260924 --hot-percent 80 --shm-mib 256 --oracle-budget 2000000\n--query-plan family|global-time --pg-url URL --output target/contention-crucible.json\n\nNo declared write sets or application prelocks. Complete successful histories must\nhave a serial witness; exhausted oracle budgets are INCONCLUSIVE and fail the run.\nBoth engines use asynchronous WAL acknowledgement; crash durability is not equated.\n--pg-url is required for both/postgres; start and manage the comparison server explicitly.");
            return Ok(None);
        }
        if arg == "--bench" || arg == "--noplot" {
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {arg}"))?;
        match arg.as_str() {
            "--engine" => config.engine = value,
            "--mode" => config.mode = value,
            "--workload" => config.workload = value,
            "--evidence" => config.evidence = value,
            "--dispatch" => {
                config.dispatch = match value.as_str() {
                    "identity" => calibrated::Dispatch::Identity,
                    "signature-affinity" => calibrated::Dispatch::SignatureAffinity,
                    _ => return Err("invalid dispatch policy".into()),
                }
            }
            "--affinity-ttl-ms" => {
                config.affinity_ttl_ms = value.parse().map_err(|_| "invalid affinity TTL")?
            }
            "--signature-pattern" => {
                config.signature_pattern = match value.as_str() {
                    "both" => calibrated::SignaturePattern::Both,
                    "mixed" => calibrated::SignaturePattern::Mixed,
                    _ => return Err("invalid signature pattern".into()),
                }
            }
            "--arrival-rate" => {
                config.arrival_rate = value.parse().map_err(|_| "invalid arrival rate")?
            }
            "--projection-interval-seconds" => {
                config.projection_interval_seconds =
                    value.parse().map_err(|_| "invalid projection interval")?
            }
            "--housekeeping-interval-seconds" => {
                config.housekeeping_interval_seconds =
                    value.parse().map_err(|_| "invalid housekeeping interval")?
            }
            "--max-backlog" => config.max_backlog = value.parse().map_err(|_| "invalid backlog")?,
            "--rpc-delay-us" => {
                config.rpc_delay_us = value.parse().map_err(|_| "invalid RPC delay")?
            }
            "--service-bind" => {
                config.service_bind = value.parse().map_err(|_| "invalid service bind address")?
            }
            "--remote-setup" => config.remote_setup = Some(value.into()),
            "--remote-final" => config.remote_final = Some(value.into()),
            "--pg-write-mode" => {
                config.pg_write_mode = match value.as_str() {
                    "immediate" => postgres::WriteMode::Immediate,
                    "buffered" => postgres::WriteMode::Buffered,
                    _ => return Err("invalid PostgreSQL write mode".into()),
                }
            }
            "--workers" => config.workers = value.parse().map_err(|_| "invalid workers")?,
            "--families" => config.families = value.parse().map_err(|_| "invalid families")?,
            "--seconds" => config.seconds = value.parse().map_err(|_| "invalid seconds")?,
            "--max-messages" => {
                config.max_messages = value.parse().map_err(|_| "invalid message cap")?
            }
            "--message-interval-us" => {
                config.message_interval_us =
                    value.parse().map_err(|_| "invalid message interval")?
            }
            "--seed" => config.seed = value.parse().map_err(|_| "invalid seed")?,
            "--hot-percent" => {
                config.hot_percent = value.parse().map_err(|_| "invalid hot-percent")?
            }
            "--shm-mib" => config.shm_mib = value.parse().map_err(|_| "invalid shm-mib")?,
            "--oracle-budget" => {
                config.oracle_budget = value.parse().map_err(|_| "invalid oracle budget")?
            }
            "--query-plan" => {
                config.global_time_predicates = match value.as_str() {
                    "family" => false,
                    "global-time" => true,
                    _ => return Err("invalid query plan".into()),
                }
            }
            "--pg-url" => config.pg_url = Some(value),
            "--output" => config.output = value.into(),
            _ => return Err(format!("unknown option {arg}")),
        }
    }
    if ![
        "both",
        "aerostore",
        "postgres",
        "service-unix",
        "service-tcp",
        "service-remote",
    ]
    .contains(&config.engine.as_str())
        || !["all", "scenarios", "sustained", "serve"].contains(&config.mode.as_str())
        || (config.mode == "serve" && config.engine != "service-tcp")
        || (config.engine == "service-remote"
            && (config.mode != "sustained"
                || config.remote_setup.is_none()
                || config.remote_final.is_none()
                || config.remote_setup == config.remote_final))
        || !(1..=32).contains(&config.workers)
        || !(1..=1024).contains(&config.families)
        || !["legacy", "lifecycle", "fleet", "calibrated"].contains(&config.workload.as_str())
        || (config.workload == "fleet" && (config.families < 16 || config.hot_percent != 0))
        || (config.workload == "calibrated"
            && config.mode != "serve"
            && (config.arrival_rate == 0 || config.hot_percent != 0))
        || !(1..=3600).contains(&config.projection_interval_seconds)
        || !(1..=3600).contains(&config.housekeeping_interval_seconds)
        || (config.workload != "calibrated"
            && (config.projection_interval_seconds != 300
                || config.housekeeping_interval_seconds != 600
                || config.dispatch != calibrated::Dispatch::Identity
                || config.affinity_ttl_ms != 0
                || config.signature_pattern != calibrated::SignaturePattern::Both))
        || (config.dispatch == calibrated::Dispatch::SignatureAffinity
            && !(1..=3_600_000).contains(&config.affinity_ttl_ms))
        || (config.dispatch == calibrated::Dispatch::Identity && config.affinity_ttl_ms != 0)
        || !["full", "metrics"].contains(&config.evidence.as_str())
        || (config.evidence == "metrics" && config.mode != "sustained")
        || (config.arrival_rate > 0 && config.message_interval_us > 0)
        || config.arrival_rate > 1_000_000
        || !(1..=100_000).contains(&config.max_backlog)
        || config.rpc_delay_us > 1_000_000
        || (config.rpc_delay_us > 0 && !config.engine.starts_with("service-"))
        || !(1..=3600).contains(&config.seconds)
        || !(1..=100_000).contains(&config.max_messages)
        || config.message_interval_us > 1_000_000
        || config.hot_percent > 100
        || !(32..=3584).contains(&config.shm_mib)
        || config.oracle_budget == 0
    {
        return Err("invalid configuration; use --help (bounded workers/families/duration/history capacity)".into());
    }
    if config.workload == "calibrated" && config.mode != "serve" {
        calibrated::validate_config(&calibrated_config(&config))?;
    }
    Ok(Some(config))
}

fn run_engines(config: &Config, evidence: &Path, report: &mut Value) -> Result<(), String> {
    let engines: Vec<_> = if config.engine == "both" {
        vec!["aerostore", "postgres"]
    } else {
        vec![config.engine.as_str()]
    };
    for engine in engines {
        let mut cases: Vec<Option<usize>> = if config.mode == "sustained" {
            vec![]
        } else {
            (0..model::scenarios(config.seed).len()).map(Some).collect()
        };
        if config.mode != "scenarios" {
            cases.push(None);
        }
        for (number, scenario) in cases.into_iter().enumerate() {
            let case = CaseConfig {
                config: config.clone(),
                engine: engine.into(),
                scenario,
                directory: evidence.join(format!("{engine}-{number}")),
                schema: format!(
                    "aero_contention_{}_{:x}_{}",
                    std::process::id(),
                    workers::monotonic_ns(),
                    number
                ),
            };
            let result = isolated_case(&case).unwrap_or_else(|error| json!({"engine":engine,"scenario":scenario,"passed":false,"execution_completed":false,"error":error,"evidence_directory":case.directory}));
            if engine == "postgres" && result["postgres_configuration"].is_object() {
                report["postgres_configuration"] = result["postgres_configuration"].clone();
            }
            println!("contention_crucible engine={engine} scenario={} passed={} completed={} retries={} p99_us={}",
                result["scenario"],result["passed"],result["completed_messages"],result["retries"],result["message_latency_p99_us_including_retries"]);
            report["runs"].as_array_mut().unwrap().push(result);
            write_json(&config.output, report)?;
        }
    }
    Ok(())
}

pub fn run() -> Result<(), String> {
    let arguments: Vec<_> = std::env::args().collect();
    if arguments.get(1).is_some_and(|a| a == "--internal-worker") {
        return workers::worker_main(Path::new(arguments.get(2).ok_or("missing worker config")?));
    }
    if arguments
        .get(1)
        .is_some_and(|a| a == "--internal-pg-cleanup")
    {
        let path = Path::new(arguments.get(2).ok_or("missing cleanup config")?);
        let case: CaseConfig = serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        return postgres::cleanup(
            case.config
                .pg_url
                .as_deref()
                .ok_or("missing PostgreSQL connection")?,
            &case.schema,
        );
    }
    if arguments
        .get(1)
        .is_some_and(|a| a == "--internal-coordinator")
    {
        let path = Path::new(arguments.get(2).ok_or("missing coordinator config")?);
        let case: CaseConfig = serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        // Server metadata collection belongs inside the same bounded process
        // as execution; a broken network must not hang the top-level runner.
        let configuration = if case.engine == "postgres" {
            postgres::configuration(
                case.config
                    .pg_url
                    .as_deref()
                    .ok_or("missing PostgreSQL connection")?,
                false,
            )
        } else {
            Ok(Value::Null)
        };
        let result = configuration.and_then(|configuration| {
            coordinator(&case).map(|mut report| {
                if case.engine == "postgres" { report["postgres_configuration"] = configuration; }
                report
            })
        }).unwrap_or_else(|error| json!({"engine":case.engine,"scenario":case.scenario,"passed":false,"execution_completed":false,"error":error,"evidence_directory":case.directory}));
        write_json(&case.directory.join("result.json"), &result)?;
        // Skip destructors for any abandoned native critical section on errors.
        std::process::exit(0);
    }
    // Invalidate stale success before parsing or any other fallible setup. Help
    // is informational and deliberately leaves a previous run untouched.
    if arguments.iter().any(|arg| arg == "--help") {
        parse()?;
        return Ok(());
    }
    let output = invalidate_previous_report(&report_path_from_arguments(&arguments))?;
    let mut config = match parse() {
        Ok(Some(config)) => config,
        Ok(None) => return Ok(()),
        Err(error) => {
            write_json(
                &output,
                &json!({"schema":1,"completed":false,"passed":false,
                "stage":"configuration","runs":[],"operational_error":error}),
            )?;
            return Err(error);
        }
    };
    config.output = output;
    if config.mode == "serve" {
        return remote::serve(
            &config.output,
            config.service_bind,
            &config.workload,
            config.families,
            config.seed,
            config.shm_mib,
            config.global_time_predicates,
            config.seconds,
            config.projection_interval_seconds,
            config.housekeeping_interval_seconds,
            config.dispatch,
            config.affinity_ttl_ms,
            config.signature_pattern,
        );
    }
    if config.engine == "service-remote" {
        for stale in [
            config
                .remote_setup
                .as_ref()
                .unwrap()
                .with_extension("client-complete.json"),
            config.remote_final.as_ref().unwrap().clone(),
        ] {
            match fs::remove_file(stale) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.to_string()),
            }
        }
    }
    let mut public_config = serde_json::to_value(&config).map_err(|e| e.to_string())?;
    public_config.as_object_mut().unwrap().remove("pg_url");
    let mut report = json!({"schema":2,"completed":false,"passed":false,"config":public_config,"runs":[],
        "git_revision":null,"evidence_directory":null,
        "transaction_contract":"single-table native optimistic transactions versus PostgreSQL SERIALIZABLE; dynamic reads/writes, no family prelocks; full evidence mode checks every successful attempt against a serial witness; metrics mode explicitly omits history checking",
        "durability_contract":"both use WAL with asynchronous acknowledgement; fsync on PostgreSQL; native writer drained on normal completion; equal crash loss windows and crash recovery NOT claimed",
        "scope":"synthetic harder HyperFeed storage workload, not proprietary handler compatibility or architecture superiority",
        "worker_failure_requirement":"surviving workers must continue; separate worker_failure_contract test reports current violations",
        "whole_engine_verified":false,"architecture_promotion_eligible":false});
    write_json(&config.output, &report)?;
    let outcome = (|| {
        if ["both", "postgres"].contains(&config.engine.as_str())
            && config.pg_url.as_deref().is_none_or(str::is_empty)
        {
            return Err("--pg-url is required for both/postgres; manage the comparison PostgreSQL server explicitly".into());
        }
        let evidence = unique_evidence_directory(&config.output)?;
        report["evidence_directory"] = json!(evidence);
        let git = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .map_err(|e| e.to_string())?;
        if !git.status.success() {
            return Err("cannot record baseline git revision".into());
        }
        report["git_revision"] = json!(String::from_utf8_lossy(&git.stdout).trim());
        write_json(&config.output, &report)?;
        run_engines(&config, &evidence, &mut report)
    })();
    report["completed"] = json!(outcome.is_ok());
    if let Err(error) = &outcome {
        report["operational_error"] = json!(error);
    }
    report["passed"] = json!(
        outcome.is_ok()
            && !report["runs"].as_array().unwrap().is_empty()
            && report["runs"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["passed"] == true)
    );
    write_json(&config.output, &report)?;
    if report["passed"] == true {
        Ok(())
    } else {
        Err(format!(
            "contention correctness/progress check failed: {}",
            config.output.display()
        ))
    }
}
