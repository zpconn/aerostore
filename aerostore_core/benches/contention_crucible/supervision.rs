//! Standalone benchmark supervision and pacing helpers. This module deliberately
//! has no database/model imports so its tests do not pull engine fixtures into
//! unrelated integration targets.
use serde::Serialize;
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::os::unix::{fs::OpenOptionsExt, process::CommandExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

pub(super) const DEFAULT_OUTPUT: &str = "target/contention-crucible.json";

pub(super) fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let temporary = path.with_extension("tmp");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    fs::rename(temporary, path).map_err(|e| e.to_string())
}
pub(super) fn private_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| e.to_string())?;
    serde_json::to_writer(&mut file, value).map_err(|e| e.to_string())
}

/// Own the complete disposable process group, including error returns from
/// try_wait. Child alone does not terminate descendants when dropped.
pub(super) struct ProcessGroup {
    child: Child,
    group_cleaned: bool,
}
impl ProcessGroup {
    pub(super) fn spawn(mut command: Command) -> Result<Self, String> {
        let expected_parent = unsafe { libc::getpid() };
        // Only async-signal-safe operations belong in this pre-exec closure.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != expected_parent {
                    return Err(std::io::Error::from_raw_os_error(libc::ECHILD));
                }
                Ok(())
            });
        }
        command
            .spawn()
            .map(|child| Self {
                child,
                group_cleaned: false,
            })
            .map_err(|e| e.to_string())
    }

    pub(super) fn wait(&mut self, timeout: Duration) -> Result<std::process::ExitStatus, String> {
        let started = Instant::now();
        loop {
            if self.group_cleaned {
                return self.child.wait().map_err(|e| e.to_string());
            }
            // Observe exit without releasing the leader PID. Clean descendants
            // while it still owns that process-group identity, then reap it.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.child.id(),
                    &mut info,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            if result < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error.to_string());
            }
            if unsafe { info.si_pid() } != 0 {
                unsafe {
                    libc::kill(-(self.child.id() as i32), libc::SIGKILL);
                }
                self.group_cleaned = true;
                return self.child.wait().map_err(|e| e.to_string());
            }
            if started.elapsed() >= timeout {
                return Err(format!(
                    "disposable process group exceeded {} seconds",
                    timeout.as_secs_f64()
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        // setsid made this child the leader of a fresh group. Never signal the
        // caller's process group or reuse the mapping after this abrupt cleanup.
        if !self.group_cleaned {
            unsafe {
                libc::kill(-(self.child.id() as i32), libc::SIGKILL);
            }
        }
        let _ = self.child.wait();
    }
}

pub(super) struct PrivateCaseFiles(pub(super) PathBuf);
impl Drop for PrivateCaseFiles {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0.join("private-config.json"));
        if let Ok(entries) = fs::read_dir(&self.0) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.file_name().is_some_and(|name| {
                    let name = name.to_string_lossy();
                    name.starts_with("worker-") && name.ends_with(".json")
                }) {
                    let _ = fs::remove_file(path);
                }
            }
        }
    }
}

pub(super) fn with_cleanup_result(
    execution: Result<Value, String>,
    cleanup: Result<(), String>,
    schema: &str,
) -> Result<Value, String> {
    match (execution, cleanup) {
        (Ok(mut report), Ok(())) => {
            report["scratch_cleanup_completed"] = json!(true);
            Ok(report)
        }
        (Ok(mut report), Err(error)) => {
            report["passed"] = json!(false);
            report["scratch_cleanup_completed"] = json!(false);
            report["cleanup_error"] = json!(format!("schema {schema} may remain: {error}"));
            Ok(report)
        }
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => Err(format!(
            "{error}; scratch cleanup failed, schema {schema} may remain: {cleanup}"
        )),
    }
}

pub(super) fn report_path_from_arguments(arguments: &[String]) -> PathBuf {
    arguments
        .windows(2)
        .rev()
        .find(|pair| pair[0] == "--output")
        .map_or_else(
            || PathBuf::from(DEFAULT_OUTPUT),
            |pair| PathBuf::from(&pair[1]),
        )
}

pub(super) fn invalidate_previous_report(output: &Path) -> Result<PathBuf, String> {
    let output = std::path::absolute(output).map_err(|e| e.to_string())?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    write_json(
        &output,
        &json!({"schema":1,"completed":false,"passed":false,
        "stage":"configuration","runs":[]}),
    )?;
    Ok(output)
}

pub(super) fn unique_evidence_directory(output: &Path) -> Result<PathBuf, String> {
    tempfile::Builder::new()
        .prefix("contention-crucible-")
        .tempdir_in(output.parent().ok_or("output has no parent directory")?)
        .map(|directory| directory.keep())
        .map_err(|e| e.to_string())
}

pub(super) fn pacing_wake_ns(
    previous_start: Option<u64>,
    interval_us: u64,
    now: u64,
    deadline: u64,
) -> u64 {
    let eligible = previous_start.map_or(now, |start| {
        start.saturating_add(interval_us.saturating_mul(1000))
    });
    eligible.max(now).min(deadline)
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    use std::process::Stdio;

    #[test]
    fn stale_success_is_invalidated_before_configuration_or_setup() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("run.json");
        write_json(&output, &json!({"completed":true,"passed":true})).unwrap();
        let selected = report_path_from_arguments(&[
            "runner".into(),
            "--output".into(),
            output.to_string_lossy().into_owned(),
            "--invalid-configuration".into(),
            "value".into(),
        ]);
        invalidate_previous_report(&selected).unwrap();
        let report: Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
        assert_eq!(report["passed"], false);
        assert_eq!(report["completed"], false);
        assert_eq!(report["stage"], "configuration");
    }

    #[test]
    fn repeated_output_paths_get_distinct_evidence_directories() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("run.json");
        let first = unique_evidence_directory(&output).unwrap();
        let second = unique_evidence_directory(&output).unwrap();
        assert_ne!(first, second);
        assert!(first.is_dir() && second.is_dir());
    }

    #[test]
    fn private_configuration_is_removed_even_after_early_return() {
        let directory = tempfile::tempdir().unwrap();
        let private = directory.path().join("private-config.json");
        let worker = directory.path().join("worker-0.json");
        let history = directory.path().join("history.jsonl");
        fs::write(&history, b"retain diagnostic history").unwrap();
        let failed = (|| -> Result<(), String> {
            let _cleanup = PrivateCaseFiles(directory.path().to_path_buf());
            private_json(&private, &json!({"pg_url":"private test credential"})).unwrap();
            private_json(&worker, &json!({"pg_url":"private test credential"})).unwrap();
            Err("fixture failure".into())
        })();
        assert!(failed.is_err());
        assert!(!private.exists() && !worker.exists());
        assert!(history.exists());
    }

    #[test]
    fn cleanup_failure_cannot_preserve_a_success_verdict() {
        let report = with_cleanup_result(
            Ok(json!({"passed":true,"completed_messages":5})),
            Err("timeout".into()),
            "owned_fixture",
        )
        .unwrap();
        assert_eq!(report["passed"], false);
        assert_eq!(report["scratch_cleanup_completed"], false);
        assert_eq!(report["completed_messages"], 5);
        assert!(report["cleanup_error"]
            .as_str()
            .unwrap()
            .contains("owned_fixture"));
        assert!(with_cleanup_result(
            Err("worker died".into()),
            Err("cleanup timed out".into()),
            "owned_fixture"
        )
        .unwrap_err()
        .contains("worker died"));
    }

    #[test]
    fn isolated_timeout_terminates_the_entire_disposable_group() {
        let directory = tempfile::tempdir().unwrap();
        let descendant = directory.path().join("descendant.pid");
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "sleep 30 & echo $! > \"$1\"; wait", "fixture"])
            .arg(&descendant)
            .stdin(Stdio::null());
        {
            let mut process = ProcessGroup::spawn(command).unwrap();
            let started = Instant::now();
            while !descendant.exists() {
                assert!(started.elapsed() < Duration::from_secs(5));
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(process.wait(Duration::from_millis(30)).is_err());
            // Drop on the error path performs the group kill, not just a kill
            // of the shell process that owns the sleeping descendant.
        }
        let pid: u32 = fs::read_to_string(descendant)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let started = Instant::now();
        loop {
            match fs::read_to_string(format!("/proc/{pid}/stat")) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Ok(stat) if stat.rsplit_once(") ").unwrap().1.starts_with('Z') => break,
                _ => assert!(
                    started.elapsed() < Duration::from_secs(2),
                    "descendant remained running"
                ),
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn successful_leader_exit_cleans_descendants_before_reaping() {
        let directory = tempfile::tempdir().unwrap();
        let descendant = directory.path().join("descendant.pid");
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "sleep 30 & echo $! > \"$1\"; exit 0", "fixture"])
            .arg(&descendant)
            .stdin(Stdio::null());
        let mut process = ProcessGroup::spawn(command).unwrap();
        assert!(process.wait(Duration::from_secs(5)).unwrap().success());
        assert!(process.group_cleaned);
        // Repeated wait/drop may use the cached status but never signal a
        // numeric process group again after releasing its leader's PID.
        assert!(process.wait(Duration::from_secs(1)).unwrap().success());
        let pid: u32 = fs::read_to_string(descendant)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match fs::read_to_string(format!("/proc/{pid}/stat")) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Ok(stat) if stat.rsplit_once(") ").unwrap().1.starts_with('Z') => break,
                _ => assert!(
                    Instant::now() < deadline,
                    "descendant survived successful owner exit"
                ),
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::pacing_wake_ns;

    #[test]
    fn pacing_accounts_for_execution_time_and_never_extends_deadline() {
        assert_eq!(pacing_wake_ns(None, 1000, 500, 10_000), 500);
        assert_eq!(pacing_wake_ns(Some(1000), 0, 5000, 10_000), 5000);
        // A 10us interval after a start at 1us leaves 6us to wait at t=5us.
        assert_eq!(pacing_wake_ns(Some(1000), 10, 5000, 50_000), 11_000);
        // Slow execution already consumed the interval: no additive sleep.
        assert_eq!(pacing_wake_ns(Some(1000), 10, 20_000, 50_000), 20_000);
        // No paced message may be scheduled beyond the common deadline.
        assert_eq!(pacing_wake_ns(Some(1000), 10, 5000, 9000), 9000);
        assert_eq!(
            pacing_wake_ns(Some(u64::MAX - 10), u64::MAX, 5000, 9000),
            9000
        );
    }
}
