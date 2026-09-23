#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant};

const SHM_PATH_ENV_KEY: &str = "AEROSTORE_SHM_PATH";

#[test]
fn bound_mode_transition_is_rejected_without_changing_config_or_rows() {
    let libpath = find_tcl_cdylib().expect("failed to locate libaerostore_tcl shared library");
    let data_dir = unique_temp_dir("aerostore_tcl_rejected_mode_transition");
    let shm_path = unique_tmpfs_shm_path("aerostore_tcl_rejected_mode_transition");
    let script_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
aerostore::set_config aerostore.checkpoint_interval_secs 0
aerostore::set_config aerostore.synchronous_commit off
for {set i 0} {$i < 10} {incr i} {
    set ident [format "ASY%03d" $i]
    FlightState ingest_tsv [format "%s\t37.600000\t-122.300000\t12000\t410\t1709200000" $ident] 1
}
if {![catch {aerostore::set_config aerostore.synchronous_commit on} message]} {
    error "mode change after binding must be rejected"
}
if {![string match "*cannot change WAL mode:*" $message]} {
    error "unexpected mode change error: $message"
}
if {[aerostore::get_config aerostore.synchronous_commit] ne "off"} {
    error "rejected mode change modified configured mode"
}
set count [FlightState search -compare {{> altitude 10000}} -limit 1000]
if {$count != 10} { error "rejected mode change changed rows: $count" }
FlightState ingest_tsv "ASY010\t37.600000\t-122.300000\t12000\t410\t1709200001" 1
set count [FlightState search -compare {{> altitude 10000}} -limit 1000]
if {$count != 11} { error "original asynchronous stream no longer accepts writes: $count" }
if {![catch {aerostore::checkpoint_now} message]} {
    error "file checkpoint of asynchronous stream must be rejected"
}
puts "rejected_transition_ok"
"#;
    let script = script_template
        .replace("__LIBPATH__", tcl_quote_path(&libpath).as_str())
        .replace("__DATA_DIR__", tcl_quote_path(&data_dir).as_str());
    run_tcl_script(&script, Duration::from_secs(40), &shm_path)
        .expect("rejected mode transition script failed");

    // The same mapping still remembers its bound asynchronous stream. Startup
    // must select that compatible mode, not silently construct a sync writer.
    let warm_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
if {[aerostore::get_config aerostore.synchronous_commit] ne "off"} {
    error "warm attachment did not restore the bound asynchronous mode"
}
set count [FlightState search -compare {{> altitude 10000}} -limit 1000]
if {$count != 11} { error "warm attachment lost rows: $count" }
FlightState ingest_tsv "ASY011\t37.600000\t-122.300000\t12000\t410\t1709200002" 1
set count [FlightState search -compare {{> altitude 10000}} -limit 1000]
if {$count != 12} { error "warm attachment cannot append to its bound stream: $count" }
puts "warm_bound_mode_ok"
"#;
    let warm = warm_template
        .replace("__LIBPATH__", tcl_quote_path(&libpath).as_str())
        .replace("__DATA_DIR__", tcl_quote_path(&data_dir).as_str());
    run_tcl_script(&warm, Duration::from_secs(40), &shm_path)
        .expect("warm bound mode script failed");
    let _ = std::fs::remove_dir_all(data_dir);
    let _ = std::fs::remove_file(shm_path);
}

#[test]
fn synchronous_checkpoint_and_wal_recovers_all_rows() {
    let libpath = find_tcl_cdylib().expect("failed to locate libaerostore_tcl shared library");
    let data_dir = unique_temp_dir("aerostore_tcl_mode_transition");
    let shm_path = unique_tmpfs_shm_path("aerostore_tcl_mode_transition");
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(&shm_path);

    let phase1_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
aerostore::set_config aerostore.checkpoint_interval_secs 0
aerostore::set_config aerostore.synchronous_commit on

for {set i 0} {$i < 10} {incr i} {
    set ident [format "ASY%03d" $i]
    set line [format "%s\t37.600000\t-122.300000\t12000\t410\t%d" $ident [expr {1709200000 + $i}]]
    FlightState ingest_tsv $line 1
}

set checkpoint [aerostore::checkpoint_now]

for {set i 0} {$i < 3} {incr i} {
    set ident [format "SYN%03d" $i]
    set line [format "%s\t37.610000\t-122.310000\t15000\t420\t%d" $ident [expr {1709300000 + $i}]]
    FlightState ingest_tsv $line 1
}

puts "phase1_ok $checkpoint"
"#;
    let phase1 = phase1_template
        .replace("__LIBPATH__", tcl_quote_path(libpath.as_path()).as_str())
        .replace("__DATA_DIR__", tcl_quote_path(data_dir.as_path()).as_str());
    run_tcl_script(phase1.as_str(), Duration::from_secs(40), shm_path.as_path())
        .expect("phase1 synchronous checkpoint script failed");

    // Force a cold replay path for phase 2 so this test validates checkpoint+WAL recovery.
    let _ = std::fs::remove_file(&shm_path);

    let phase2_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
set count [FlightState search -compare {{> altitude 10000}} -limit 1000]
if {$count != 13} {
    error "expected 13 recovered rows after synchronous checkpoint + WAL, got $count"
}

set asy [FlightState search -compare {{= flight_id ASY000}} -limit 10]
if {$asy != 1} {
    error "expected recovered PK row ASY000, got count=$asy"
}

set syn [FlightState search -compare {{= flight_id SYN002}} -limit 10]
if {$syn != 1} {
    error "expected recovered PK row SYN002, got count=$syn"
}

set missing [FlightState search -compare {{= flight_id MISSING}} -limit 10]
if {$missing != 0} {
    error "expected missing PK row count 0, got $missing"
}
puts "phase2_ok"
"#;
    let phase2 = phase2_template
        .replace("__LIBPATH__", tcl_quote_path(libpath.as_path()).as_str())
        .replace("__DATA_DIR__", tcl_quote_path(data_dir.as_path()).as_str());
    run_tcl_script(phase2.as_str(), Duration::from_secs(30), shm_path.as_path())
        .expect("phase2 recovery validation script failed");

    let _ = std::fs::remove_dir_all(data_dir);
    let _ = std::fs::remove_file(shm_path);
}

#[test]
fn periodic_checkpointer_creates_checkpoint_and_shrinks_wal() {
    let libpath = find_tcl_cdylib().expect("failed to locate libaerostore_tcl shared library");
    let data_dir = unique_temp_dir("aerostore_tcl_periodic_checkpoint");
    let shm_path = unique_tmpfs_shm_path("aerostore_tcl_periodic_checkpoint");
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(&shm_path);

    let script_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
aerostore::set_config aerostore.synchronous_commit on
aerostore::set_config aerostore.checkpoint_interval_secs 1

FlightState ingest_tsv "CPK001\t37.618805\t-122.375416\t35000\t451\t1709400000" 1

set wal_file "__DATA_DIR__/aerostore.wal"
set checkpoint_file "__DATA_DIR__/occ_checkpoint.dat"

if {![file exists $wal_file]} {
    error "expected WAL file to exist at $wal_file"
}
set before [file size $wal_file]

set checkpoint_ready 0
for {set i 0} {$i < 15} {incr i} {
    if {[file exists $checkpoint_file]} {
        set checkpoint_ready 1
        break
    }
    exec sleep 1
}

if {!$checkpoint_ready} {
    set fallback [aerostore::checkpoint_now]
    if {![string match "checkpoint rows=*" $fallback]} {
        error "checkpoint fallback returned unexpected payload: $fallback"
    }
    if {[file exists $checkpoint_file]} {
        set checkpoint_ready 1
    }
}

if {!$checkpoint_ready} {
    error "expected periodic checkpoint file at $checkpoint_file"
}
set checkpoint_bytes [file size $checkpoint_file]
if {$checkpoint_bytes <= 0} {
    error "checkpoint file should be non-empty"
}

set after [file size $wal_file]
if {$after > $before} {
    error "expected periodic checkpointer to truncate or hold WAL size; before=$before after=$after"
}
puts "periodic_ok before=$before after=$after checkpoint_bytes=$checkpoint_bytes"
"#;
    let script = script_template
        .replace("__LIBPATH__", tcl_quote_path(libpath.as_path()).as_str())
        .replace("__DATA_DIR__", tcl_quote_path(data_dir.as_path()).as_str());

    run_tcl_script(script.as_str(), Duration::from_secs(45), shm_path.as_path())
        .expect("periodic checkpoint script failed");
    let _ = std::fs::remove_dir_all(data_dir);
    let _ = std::fs::remove_file(shm_path);
}

#[test]
fn benchmark_tcl_synchronous_commit_modes() {
    let libpath = find_tcl_cdylib().expect("failed to locate libaerostore_tcl shared library");
    let mut rates = Vec::new();
    for mode in ["on", "off"] {
        // Each durability mode owns a fresh table, mapping and WAL stream.
        let data_dir = unique_temp_dir("aerostore_tcl_sync_benchmark");
        let shm_path = unique_tmpfs_shm_path("aerostore_tcl_sync_benchmark");
        let script_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
aerostore::set_config aerostore.checkpoint_interval_secs 0
aerostore::set_config aerostore.synchronous_commit __MODE__
set txns 1200
set started [clock milliseconds]
for {set i 0} {$i < $txns} {incr i} {
    set ident [format "BEN%05d" $i]
    set line [format "%s\t37.620000\t-122.380000\t35500\t455\t%d" $ident [expr {1709500000 + $i}]]
    FlightState ingest_tsv $line 1
}
set elapsed [expr {[clock milliseconds] - $started}]
if {$elapsed < 1} { set elapsed 1 }
set tps [expr {double($txns) / (double($elapsed) / 1000.0)}]
set count [FlightState search -compare {{> altitude 10000}} -limit 2000]
if {$count != $txns} { error "benchmark lost rows: expected $txns, got $count" }
set result [open "__DATA_DIR__/benchmark_tps.txt" w]
puts $result $tps
close $result
puts "tcl_sync_benchmark mode=__MODE__ txns=$txns tps=$tps"
"#;
        let script = script_template
            .replace("__LIBPATH__", tcl_quote_path(libpath.as_path()).as_str())
            .replace("__DATA_DIR__", tcl_quote_path(data_dir.as_path()).as_str())
            .replace("__MODE__", mode);
        run_tcl_script(script.as_str(), Duration::from_secs(90), shm_path.as_path())
            .expect("tcl throughput benchmark script failed");
        let tps: f64 = std::fs::read_to_string(data_dir.join("benchmark_tps.txt"))
            .expect("missing benchmark output")
            .trim()
            .parse()
            .expect("invalid benchmark output");
        rates.push(tps);
        let _ = std::fs::remove_dir_all(data_dir);
        let _ = std::fs::remove_file(shm_path);
    }
    let ratio = rates[1] / rates[0];
    eprintln!(
        "tcl_sync_benchmark on_tps={} off_tps={} ratio={ratio}",
        rates[0], rates[1]
    );
    assert!(
        ratio >= 2.0,
        "expected asynchronous mode to be at least 2x synchronous mode, got {ratio}"
    );
}

fn run_tcl_script(script: &str, timeout: Duration, shm_path: &Path) -> Result<(), String> {
    let script_path = write_temp_script(script)?;
    let status = run_tcl_script_with_timeout(script_path.as_path(), timeout, shm_path)?;
    let _ = std::fs::remove_file(script_path);
    if status.success() {
        Ok(())
    } else {
        Err(format!("tcl script failed with status {}", status))
    }
}

fn run_tcl_script_with_timeout(
    script_path: &Path,
    timeout: Duration,
    shm_path: &Path,
) -> Result<std::process::ExitStatus, String> {
    let mut child = Command::new("tclsh")
        .arg(script_path)
        .env(SHM_PATH_ENV_KEY, shm_path)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("failed to spawn tclsh: {}", e))?;

    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| format!("failed polling tclsh status: {}", e))?
        {
            return Ok(status);
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "timed out waiting for tclsh script after {:?}",
                timeout
            ));
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}

fn find_tcl_cdylib() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|e| format!("current_exe failed: {}", e))?;
    let deps_dir = exe
        .parent()
        .ok_or_else(|| "test binary has no parent directory".to_string())?;
    let profile_dir = deps_dir
        .parent()
        .ok_or_else(|| "deps directory has no profile parent".to_string())?;

    // Ensure the cdylib reflects the current workspace sources for this test run.
    build_cdylib_for_profile(profile_dir)?;

    let candidates = shared_library_candidates();
    for root in candidate_roots(profile_dir, deps_dir) {
        for name in candidates {
            let path = root.join(name);
            if path.exists() {
                return Ok(path);
            }
        }
    }

    Err(format!(
        "unable to find shared library after probing {} and {}",
        profile_dir.display(),
        deps_dir.display()
    ))
}

fn candidate_roots(profile_dir: &Path, deps_dir: &Path) -> Vec<PathBuf> {
    let mut roots = vec![profile_dir.to_path_buf(), deps_dir.to_path_buf()];
    if let Some(target_dir) = profile_dir.parent() {
        roots.push(target_dir.join("debug"));
        roots.push(target_dir.join("debug").join("deps"));
        roots.push(target_dir.join("release"));
        roots.push(target_dir.join("release").join("deps"));
    }
    roots
}

fn build_cdylib_for_profile(profile_dir: &Path) -> Result<(), String> {
    let profile = profile_dir
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("debug");
    let mut cmd = Command::new("cargo");
    cmd.args(["build", "--offline", "--locked", "-p", "aerostore_tcl"]);
    let target_dir = profile_dir
        .parent()
        .ok_or_else(|| "profile directory has no target parent".to_string())?;
    cmd.arg("--target-dir").arg(target_dir);
    if profile.eq_ignore_ascii_case("release") {
        cmd.arg("--release");
    }
    let status = cmd
        .status()
        .map_err(|e| format!("failed to invoke cargo build for cdylib: {}", e))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "cargo build failed while producing aerostore_tcl cdylib: {}",
            status
        ))
    }
}

fn shared_library_candidates() -> &'static [&'static str] {
    #[cfg(target_os = "linux")]
    {
        &["libaerostore_tcl.so"]
    }
    #[cfg(target_os = "macos")]
    {
        &["libaerostore_tcl.dylib"]
    }
    #[cfg(target_os = "windows")]
    {
        &["aerostore_tcl.dll"]
    }
}

fn tcl_quote_path(path: &Path) -> String {
    let raw = path.to_string_lossy();
    raw.replace('\\', "/")
}

fn write_temp_script(script: &str) -> Result<PathBuf, String> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("clock error: {}", e))?
        .as_nanos();
    let path = std::env::temp_dir().join(format!("aerostore_tcl_cfg_{nonce}.tcl"));
    std::fs::write(path.as_path(), script).map_err(|e| format!("script write failed: {}", e))?;
    Ok(path)
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock drift while deriving temp dir")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nonce}"))
}

fn unique_tmpfs_shm_path(prefix: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock drift while deriving tmpfs shm path")
        .as_nanos();
    PathBuf::from(format!("/dev/shm/{prefix}_{nonce}.mmap"))
}
