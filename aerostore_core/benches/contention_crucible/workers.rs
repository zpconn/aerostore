//! Independent process workers. A sustained request generates messages locally;
//! the coordinator does not dispatch or admit individual transactions.
use super::measurement::ArrivalPlan;
use super::storage::{Query, Store};
use super::supervision::pacing_wake_ns;
use super::{aerostore, calibrated, maintenance, model, oracle, postgres, service};
use crate::extended_crucible::metrics::StoreMetrics;
use crate::extended_crucible::model::{DbError, Record};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::Duration;

/// Written to a private coordinator-owned file; never serialize into reports.
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    /// Captured by the spawning coordinator before this process is started.
    pub expected_parent_pid: u32,
    pub engine: String,
    pub service_endpoint: Option<service::Endpoint>,
    pub workload: String,
    pub record_history: bool,
    pub pg_write_mode: postgres::WriteMode,
    pub max_backlog: u64,
    pub rpc_delay_us: u64,
    pub attachment: Option<aerostore::Attachment>,
    pub pg_url: Option<String>,
    pub schema: String,
    pub global_time_predicates: bool,
    pub worker_id: usize,
    pub workers: usize,
    pub families: usize,
    pub seed: u64,
    pub hot_percent: u32,
    pub retry_limit: u64,
    /// Minimum spacing between logical-message starts in sustained requests.
    /// Zero is unpaced; pacing waits are outside measured service latency.
    pub message_interval_us: u64,
    /// Prepared by the coordinator before workers report Ready; no replay of
    /// the complete affinity dispatcher occurs inside the measured interval.
    #[serde(default)]
    pub calibrated_schedule: Option<calibrated::WorkerSchedule>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Run {
        messages: Vec<model::Message>,
        /// Only the first query of the first attempt of the first message.
        synchronise_first_query: bool,
    },
    Sustain {
        deadline_ns: u64,
        max_messages: usize,
        arrivals: Option<ArrivalPlan>,
    },
    Calibrated {
        start_ns: u64,
        config: calibrated::Config,
        max_messages: usize,
    },
    Continue,
    Stop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Reply {
    Ready,
    FirstQuery {
        rows: usize,
    },
    Observation {
        receipt: oracle::Receipt,
        latency_ns: u64,
        message_started_ns: u64,
        scheduled_ns: Option<u64>,
        retries: u64,
    },
    /// Cumulative counters for this worker process.
    Done {
        metrics: StoreMetrics,
        retry_causes: BTreeMap<String, u64>,
        diagnostics: BTreeMap<String, u64>,
        maximum_backlog: u64,
        finished_ns: u64,
        /// scenario_complete, deadline, message_cap, or stop_requested.
        stop_reason: String,
    },
    Error {
        message: String,
    },
}

pub fn monotonic_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // CLOCK_MONOTONIC has one epoch shared by all local worker processes.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) };
    assert_eq!(status, 0, "CLOCK_MONOTONIC unavailable");
    (time.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(time.tv_nsec as u64)
}

fn reply(output: &mut impl Write, value: &Reply) -> Result<(), String> {
    serde_json::to_writer(&mut *output, value).map_err(|error| error.to_string())?;
    output.write_all(b"\n").map_err(|error| error.to_string())?;
    output.flush().map_err(|error| error.to_string())
}

type Inbox = Receiver<Result<Request, String>>;

fn inbox() -> Inbox {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let input = std::io::stdin();
        for line in input.lock().lines() {
            let request = line
                .map_err(|error| error.to_string())
                .and_then(|line| serde_json::from_str(&line).map_err(|error| error.to_string()));
            let failed = request.is_err();
            if sender.send(request).is_err() || failed {
                break;
            }
        }
    });
    receiver
}

fn receive(input: &Inbox) -> Result<Request, String> {
    input
        .recv()
        .map_err(|_| "coordinator closed worker input".to_string())?
}

/// The wall-clock schedule keeps advancing while a worker is busy. Waiting
/// never changes a job's due time or coalesces missed maintenance ticks.
fn wait_until(input: &Inbox, deadline_ns: u64) -> Result<bool, String> {
    loop {
        match input.try_recv() {
            Ok(Ok(Request::Stop)) => return Ok(false),
            Ok(Ok(_)) => return Err("unexpected command during calibrated schedule".into()),
            Ok(Err(error)) => return Err(error),
            Err(TryRecvError::Disconnected) => {
                return Err("coordinator closed calibrated input".into())
            }
            Err(TryRecvError::Empty) => (),
        }
        let now = monotonic_ns();
        if now >= deadline_ns {
            return Ok(true);
        }
        match input.recv_timeout(Duration::from_nanos(deadline_ns - now)) {
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Ok(Ok(Request::Stop)) => return Ok(false),
            Ok(Ok(_)) => return Err("unexpected command during calibrated schedule wait".into()),
            Ok(Err(error)) => return Err(error),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("coordinator closed input during calibrated schedule".into())
            }
        }
    }
}

trait MeasuredStore: Store {
    fn metrics(&self) -> StoreMetrics;
    fn retry_causes(&self) -> BTreeMap<String, u64>;
    fn diagnostics(&self) -> BTreeMap<String, u64> {
        BTreeMap::new()
    }
}
impl MeasuredStore for aerostore::Adapter<'_> {
    fn diagnostics(&self) -> BTreeMap<String, u64> {
        self.diagnostics.clone()
    }
    fn metrics(&self) -> StoreMetrics {
        self.metrics.clone()
    }
    fn retry_causes(&self) -> BTreeMap<String, u64> {
        self.retry_causes.clone()
    }
}
impl MeasuredStore for postgres::Adapter {
    fn diagnostics(&self) -> BTreeMap<String, u64> {
        serde_json::to_value(&self.sql_metrics)
            .expect("SQL counters serialize")
            .as_object()
            .expect("SQL counters object")
            .iter()
            .filter_map(|(k, v)| v.as_u64().map(|v| (format!("postgres:{k}"), v)))
            .collect()
    }
    fn metrics(&self) -> StoreMetrics {
        self.metrics.clone()
    }
    fn retry_causes(&self) -> BTreeMap<String, u64> {
        self.retry_causes.clone()
    }
}

impl MeasuredStore for service::Client {
    fn metrics(&self) -> StoreMetrics {
        self.metrics.clone()
    }
    fn retry_causes(&self) -> BTreeMap<String, u64> {
        self.retry_causes.clone()
    }
    fn diagnostics(&self) -> BTreeMap<String, u64> {
        self.diagnostics.clone()
    }
}

/// A controlled extra delay per interactive service operation. It is a
/// sensitivity experiment, not a measurement of a physical network.
struct Delayed<S> {
    inner: S,
    delay: Duration,
}
impl<S> Delayed<S> {
    fn wait(&self) {
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
    }
}
impl<S: Store> Store for Delayed<S> {
    fn begin(&mut self, ids: &[usize]) -> Result<(), DbError> {
        self.wait();
        self.inner.begin(ids)
    }
    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        self.wait();
        self.inner.read(id)
    }
    fn query(&mut self, q: &Query) -> Result<Vec<Record>, DbError> {
        self.wait();
        self.inner.query(q)
    }
    fn write(&mut self, row: Record) -> Result<(), DbError> {
        self.wait();
        self.inner.write(row)
    }
    fn savepoint(&mut self) -> Result<usize, DbError> {
        self.wait();
        self.inner.savepoint()
    }
    fn rollback_to(&mut self, id: usize) -> Result<(), DbError> {
        self.wait();
        self.inner.rollback_to(id)
    }
    fn commit(&mut self) -> Result<(), DbError> {
        self.wait();
        self.inner.commit()
    }
    fn abort(&mut self) -> Result<(), DbError> {
        self.wait();
        self.inner.abort()
    }
}
impl<S: MeasuredStore> MeasuredStore for Delayed<S> {
    fn metrics(&self) -> StoreMetrics {
        self.inner.metrics()
    }
    fn retry_causes(&self) -> BTreeMap<String, u64> {
        self.inner.retry_causes()
    }
    fn diagnostics(&self) -> BTreeMap<String, u64> {
        self.inner.diagnostics()
    }
}

/// The optional first-query barrier is solely for bounded correctness cases.
/// It runs after the actual backend query returns and before any handler write.
struct FirstQuery<'a, S, W> {
    store: &'a mut S,
    input: &'a Inbox,
    output: &'a mut W,
    armed: bool,
}
impl<S: Store, W: Write> Store for FirstQuery<'_, S, W> {
    fn begin(&mut self, declared: &[usize]) -> Result<(), DbError> {
        self.store.begin(declared)
    }
    fn read(&mut self, id: usize) -> Result<Record, DbError> {
        self.store.read(id)
    }
    fn query(&mut self, query: &Query) -> Result<Vec<Record>, DbError> {
        let result = self.store.query(query);
        if !self.armed {
            return result;
        }
        self.armed = false;
        // Retrying before all workers reach this cutpoint would silently weaken
        // the forced-overlap case. Fail that case instead; retries after a
        // successful barrier release proceed without another barrier.
        let rows = result.map_err(|error| {
            DbError::Fatal(format!(
                "synchronized first query failed before barrier: {error}"
            ))
        })?;
        reply(self.output, &Reply::FirstQuery { rows: rows.len() }).map_err(DbError::Fatal)?;
        match receive(self.input).map_err(DbError::Fatal)? {
            Request::Continue => Ok(rows),
            _ => Err(DbError::Fatal(
                "expected Continue at first-query barrier".into(),
            )),
        }
    }
    fn write(&mut self, row: Record) -> Result<(), DbError> {
        self.store.write(row)
    }
    fn savepoint(&mut self) -> Result<usize, DbError> {
        self.store.savepoint()
    }
    fn rollback_to(&mut self, savepoint: usize) -> Result<(), DbError> {
        self.store.rollback_to(savepoint)
    }
    fn commit(&mut self) -> Result<(), DbError> {
        self.store.commit()
    }
    fn abort(&mut self) -> Result<(), DbError> {
        self.store.abort()
    }
}

fn execute(
    store: &mut impl MeasuredStore,
    input: &Inbox,
    output: &mut impl Write,
    message: model::Message,
    barrier: bool,
    retry_limit: u64,
    record_history: bool,
    scheduled_ns: Option<u64>,
    sweep_batch: bool,
) -> Result<(u64, bool), String> {
    let first_started = monotonic_ns();
    let mut retries = 0;
    loop {
        let started = monotonic_ns();
        let mut observed = FirstQuery {
            store,
            input,
            output,
            armed: barrier && retries == 0,
        };
        let result = model::execute_attempt_with_recording(&mut observed, &message, record_history);
        let finished = monotonic_ns();
        match result {
            Ok(body) => {
                if finished <= started {
                    return Err("non-increasing successful attempt clock".into());
                }
                let terminal = sweep_batch && maintenance::processed(&message, &body.outcome)? == 0;
                return reply(
                    output,
                    &Reply::Observation {
                        receipt: oracle::Receipt {
                            message,
                            started,
                            finished,
                            body,
                        },
                        latency_ns: finished - first_started,
                        message_started_ns: first_started,
                        scheduled_ns,
                        retries,
                    },
                )
                .map(|()| (first_started, terminal));
            }
            Err(DbError::Conflict) if retries < retry_limit => {
                store
                    .abort()
                    .map_err(|error| format!("retry cleanup failed: {error}"))?;
                retries += 1;
                std::thread::sleep(Duration::from_micros(
                    retries.saturating_mul(50).min(10_000),
                ));
            }
            Err(error) => {
                let cleanup = store.abort();
                return Err(format!(
                    "message {} after {retries} retries: {error}; cleanup={cleanup:?}",
                    message.id
                ));
            }
        }
    }
}

fn worker_loop(config: &Config, store: &mut impl MeasuredStore) -> Result<(), String> {
    let input = inbox();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    reply(&mut output, &Reply::Ready)?;
    let mut local_sequence = 0_u64;
    let mut previous_sustained_start = None;
    let mut maximum_backlog = 0;
    loop {
        let mut stop = false;
        let mut stop_reason;
        match receive(&input)? {
            Request::Run {
                messages,
                synchronise_first_query,
            } => {
                stop_reason = "scenario_complete";
                for (index, message) in messages.into_iter().enumerate() {
                    execute(
                        store,
                        &input,
                        &mut output,
                        message,
                        synchronise_first_query && index == 0,
                        config.retry_limit,
                        true,
                        None,
                        false,
                    )?;
                }
            }
            Request::Sustain {
                deadline_ns,
                max_messages,
                arrivals,
            } => {
                stop_reason = "message_cap";
                for _ in 0..max_messages {
                    match input.try_recv() {
                        Ok(Ok(Request::Stop)) => {
                            stop = true;
                            stop_reason = "stop_requested";
                            break;
                        }
                        Ok(Ok(_)) => return Err("unexpected command during sustained run".into()),
                        Ok(Err(error)) => return Err(error),
                        Err(TryRecvError::Disconnected) => {
                            return Err("coordinator closed worker input".into())
                        }
                        Err(TryRecvError::Empty) => (),
                    }
                    let sequence = local_sequence
                        .checked_mul(config.workers as u64)
                        .and_then(|value| value.checked_add(config.worker_id as u64))
                        .ok_or("worker sequence overflow")?;
                    let scheduled_ns = match &arrivals {
                        Some(plan) => match plan.scheduled_ns(sequence) {
                            Some(at) => Some(at),
                            None => {
                                stop_reason = "arrival_corpus_drained";
                                break;
                            }
                        },
                        None => None,
                    };
                    if arrivals.is_none() && monotonic_ns() >= deadline_ns {
                        stop_reason = "deadline";
                        break;
                    }
                    let now = monotonic_ns();
                    let wake = scheduled_ns.unwrap_or_else(|| {
                        pacing_wake_ns(
                            previous_sustained_start,
                            config.message_interval_us,
                            now,
                            deadline_ns,
                        )
                    });
                    if wake > now {
                        match input.recv_timeout(Duration::from_nanos(wake - now)) {
                            Ok(Ok(Request::Stop)) => {
                                stop = true;
                                stop_reason = "stop_requested";
                                break;
                            }
                            Ok(Ok(_)) => {
                                return Err("unexpected command during message pacing".into())
                            }
                            Ok(Err(error)) => return Err(error),
                            Err(mpsc::RecvTimeoutError::Disconnected) => {
                                return Err("coordinator closed worker input".into())
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => (),
                        }
                    }
                    if let Some(plan) = &arrivals {
                        let backlog =
                            plan.backlog(config.worker_id, local_sequence, monotonic_ns());
                        maximum_backlog = maximum_backlog.max(backlog);
                        if backlog > config.max_backlog {
                            return Err(format!("offered arrival backlog {backlog} exceeds worker bound {} after {local_sequence} completions; arrivals were not throttled", config.max_backlog));
                        }
                    } else if monotonic_ns() >= deadline_ns {
                        stop_reason = "deadline";
                        break;
                    }
                    let message = model::sustained_message_for(
                        &config.workload,
                        sequence,
                        config.worker_id,
                        config.workers,
                        config.families,
                        config.seed,
                        config.hot_percent,
                    );
                    let message_started = execute(
                        store,
                        &input,
                        &mut output,
                        message,
                        false,
                        config.retry_limit,
                        config.record_history,
                        scheduled_ns,
                        false,
                    )?;
                    previous_sustained_start = Some(message_started.0);
                    local_sequence = local_sequence
                        .checked_add(1)
                        .ok_or("worker sequence overflow")?;
                }
                if let Some(plan) = &arrivals {
                    if local_sequence == plan.worker_offered(config.worker_id) {
                        stop_reason = "arrival_corpus_drained";
                        // Finish the complete admission interval even if its
                        // last scheduled message completed early. Keep idle
                        // retention and elapsed capacity measurements honest.
                        let now = monotonic_ns();
                        if now < deadline_ns {
                            match input.recv_timeout(Duration::from_nanos(deadline_ns - now)) {
                                Err(mpsc::RecvTimeoutError::Timeout) => (),
                                Ok(Ok(Request::Stop)) => {
                                    stop = true;
                                    stop_reason = "stop_requested";
                                }
                                Ok(Ok(_)) => {
                                    return Err(
                                        "unexpected command while waiting for admission deadline"
                                            .into(),
                                    )
                                }
                                Ok(Err(error)) => return Err(error),
                                Err(mpsc::RecvTimeoutError::Disconnected) => {
                                    return Err(
                                        "coordinator closed input before admission deadline".into(),
                                    )
                                }
                            }
                        }
                    } else if stop_reason == "message_cap" {
                        return Err("message cap truncated the offered arrival corpus".into());
                    }
                }
            }
            Request::Calibrated {
                start_ns,
                config: schedule_config,
                max_messages,
            } => {
                let deadline_ns = start_ns
                    .checked_add(schedule_config.duration_ns)
                    .ok_or("calibrated admission deadline overflow")?;
                if schedule_config.families != config.families
                    || schedule_config.seed != config.seed
                    || config.workload != "calibrated"
                {
                    return Err("calibrated worker configuration mismatch".into());
                }
                let plan = config
                    .calibrated_schedule
                    .as_ref()
                    .ok_or("missing prepared calibrated schedule")?;
                if plan.config != schedule_config
                    || plan.worker != config.worker_id
                    || config.workers != plan.config.foreground_workers + 2
                {
                    return Err("calibrated worker routing dimensions differ".into());
                }
                let offered = plan.offered();
                if offered > max_messages as u64 {
                    return Err("calibrated message cap would truncate admitted jobs".into());
                }
                stop_reason = "arrival_corpus_drained";
                for ordinal in 0..offered {
                    let event = plan
                        .event(ordinal)
                        .ok_or("calibrated offered event missing")?;
                    let scheduled_ns = start_ns
                        .checked_add(event.offset_ns)
                        .ok_or("calibrated event deadline overflow")?;
                    if !wait_until(&input, scheduled_ns)? {
                        stop = true;
                        stop_reason = "stop_requested";
                        break;
                    }
                    let backlog = plan.backlog(ordinal, monotonic_ns().saturating_sub(start_ns));
                    maximum_backlog = maximum_backlog.max(backlog);
                    if backlog > config.max_backlog {
                        return Err(format!("calibrated offered backlog {backlog} exceeds worker bound {} after {ordinal} completions; all independently due jobs remain admitted",config.max_backlog));
                    }
                    let sweep = plan.config.maintenance_mode == maintenance::Mode::Sweep
                        && event.class != calibrated::EventClass::Foreground;
                    if sweep {
                        let mut terminal = false;
                        for batch in 0..plan.config.max_maintenance_batches {
                            if !wait_until(&input, monotonic_ns())? {
                                return Err(format!("maintenance job {} interrupted before terminal query after {batch} committed batches", event.message.id));
                            }
                            let backlog =
                                plan.backlog(ordinal, monotonic_ns().saturating_sub(start_ns));
                            maximum_backlog = maximum_backlog.max(backlog);
                            if backlog > config.max_backlog {
                                return Err(format!("maintenance job {} offered backlog {backlog} exceeds worker bound {} after {batch} committed batches", event.message.id, config.max_backlog));
                            }
                            let (_, empty) = execute(
                                store,
                                &input,
                                &mut output,
                                maintenance::batch_message(&event.message, batch)?,
                                false,
                                config.retry_limit,
                                config.record_history,
                                Some(scheduled_ns),
                                true,
                            )?;
                            if empty {
                                terminal = true;
                                break;
                            }
                        }
                        if !terminal {
                            return Err(format!("maintenance job {} exhausted {} committed batches without a terminal empty query; prior batch effects remain committed", event.message.id, plan.config.max_maintenance_batches));
                        }
                    } else {
                        execute(
                            store,
                            &input,
                            &mut output,
                            event.message,
                            false,
                            config.retry_limit,
                            config.record_history,
                            Some(scheduled_ns),
                            false,
                        )?;
                    }
                    maximum_backlog = maximum_backlog
                        .max(plan.backlog(ordinal + 1, monotonic_ns().saturating_sub(start_ns)));
                }
                if !stop && !wait_until(&input, deadline_ns)? {
                    stop = true;
                    stop_reason = "stop_requested";
                }
            }
            Request::Stop => {
                store.abort().map_err(|error| error.to_string())?;
                return Ok(());
            }
            Request::Continue => return Err("Continue outside first-query barrier".into()),
        }
        reply(
            &mut output,
            &Reply::Done {
                metrics: store.metrics(),
                retry_causes: store.retry_causes(),
                diagnostics: store.diagnostics(),
                maximum_backlog,
                finished_ns: monotonic_ns(),
                stop_reason: stop_reason.into(),
            },
        )?;
        if stop {
            store.abort().map_err(|error| error.to_string())?;
            return Ok(());
        }
    }
}

fn worker_inner(config_path: &Path) -> Result<(), String> {
    let config: Config =
        serde_json::from_slice(&std::fs::read(config_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    unsafe {
        // A coordinator-supplied PID detects an exit before the worker's first
        // instruction, including adoption by a subreaper whose PID is not 1.
        // The check after registration closes the PR_SET_PDEATHSIG race.
        if config.expected_parent_pid == 0
            || libc::getppid() as u32 != config.expected_parent_pid
            || libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0
            || libc::getppid() as u32 != config.expected_parent_pid
        {
            return Err("failed to establish worker parent lifetime".into());
        }
    }
    if config.workers == 0
        || config.worker_id >= config.workers
        || config.families == 0
        || config.hot_percent > 100
    {
        return Err("invalid worker count, identity count, or hot-identity percentage".into());
    }
    if let Some(plan) = &config.calibrated_schedule {
        plan.validate()?;
        if config.workload != "calibrated"
            || plan.worker != config.worker_id
            || plan.config.foreground_workers + 2 != config.workers
            || plan.config.families != config.families
            || plan.config.seed != config.seed
        {
            return Err("invalid prepared calibrated worker schedule".into());
        }
    }
    match config.engine.as_str() {
        "aerostore" => {
            let shared = aerostore::Shared::attach(
                config
                    .attachment
                    .as_ref()
                    .ok_or("missing arena attachment")?,
            )?;
            let mut adapter = aerostore::Adapter::new(&shared, config.global_time_predicates);
            worker_loop(&config, &mut adapter)
        }
        "service-unix" | "service-tcp" | "service-remote" => {
            let client = service::Client::connect(
                config
                    .service_endpoint
                    .as_ref()
                    .ok_or("missing service endpoint")?,
                Duration::from_secs(60),
            )?;
            let mut delayed = Delayed {
                inner: client,
                delay: Duration::from_micros(config.rpc_delay_us),
            };
            worker_loop(&config, &mut delayed)
        }
        "postgres" => worker_loop(
            &config,
            &mut postgres::Adapter::connect_with_mode(
                config
                    .pg_url
                    .as_deref()
                    .ok_or("missing PostgreSQL connection")?,
                &config.schema,
                config.pg_write_mode,
            )?,
        ),
        _ => Err("invalid worker engine".into()),
    }
}

pub fn worker_main(config_path: &Path) -> Result<(), String> {
    let result = worker_inner(config_path);
    if let Err(message) = &result {
        // Configuration/attachment failures and runtime failures share one wire
        // error shape; the process still exits unsuccessfully via the runner.
        let _ = reply(
            &mut std::io::stdout().lock(),
            &Reply::Error {
                message: message.clone(),
            },
        );
    }
    result
}
