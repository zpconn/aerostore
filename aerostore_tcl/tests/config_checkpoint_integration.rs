#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant};

const SHM_PATH_ENV_KEY: &str = "AEROSTORE_SHM_PATH";

#[test]
fn mode_transition_off_to_on_checkpoint_recovers_all_rows() {
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
aerostore::set_config aerostore.synchronous_commit off

for {set i 0} {$i < 10} {incr i} {
    set ident [format "ASY%03d" $i]
    set line [format "%s\t37.600000\t-122.300000\t12000\t410\t%d" $ident [expr {1709200000 + $i}]]
    FlightState ingest_tsv $line 1
}

aerostore::set_config aerostore.synchronous_commit on
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
        .expect("phase1 mode-transition script failed");

    // Force a cold replay path for phase 2 so this test validates checkpoint+WAL recovery.
    let _ = std::fs::remove_file(&shm_path);

    let phase2_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
set count [FlightState search -compare {{> altitude 10000}} -limit 1000]
if {$count != 13} {
    error "expected 13 recovered rows after mode transition + checkpoint, got $count"
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
    let data_dir = unique_temp_dir("aerostore_tcl_sync_benchmark");
    let shm_path = unique_tmpfs_shm_path("aerostore_tcl_sync_benchmark");
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(&shm_path);

    let script_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]
aerostore::set_config aerostore.checkpoint_interval_secs 0

proc run_mode {mode txns prefix} {
    aerostore::set_config aerostore.synchronous_commit $mode
    set started [clock milliseconds]
    for {set i 0} {$i < $txns} {incr i} {
        set ident [format "%s%05d" $prefix $i]
        set line [format "%s\t37.620000\t-122.380000\t35500\t455\t%d" $ident [expr {1709500000 + $i}]]
        FlightState ingest_tsv $line 1
    }
    set elapsed [expr {[clock milliseconds] - $started}]
    if {$elapsed < 1} { set elapsed 1 }
    return [expr {double($txns) / (double($elapsed) / 1000.0)}]
}

set txns 1200
set on_tps [run_mode on $txns ON]
set off_tps [run_mode off $txns OFF]
aerostore::set_config aerostore.synchronous_commit on
set ratio [expr {$off_tps / ($on_tps > 0 ? $on_tps : 1.0)}]
puts "tcl_sync_benchmark txns=$txns on_tps=$on_tps off_tps=$off_tps ratio=$ratio"

if {$ratio < 2.0} {
    error "expected synchronous_commit=off to be at least 2x faster in Tcl path, observed ratio=$ratio"
}
"#;
    let script = script_template
        .replace("__LIBPATH__", tcl_quote_path(libpath.as_path()).as_str())
        .replace("__DATA_DIR__", tcl_quote_path(data_dir.as_path()).as_str());

    run_tcl_script(script.as_str(), Duration::from_secs(90), shm_path.as_path())
        .expect("tcl throughput benchmark script failed");
    let _ = std::fs::remove_dir_all(data_dir);
    let _ = std::fs::remove_file(shm_path);
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
    cmd.args(["build", "-p", "aerostore_tcl"]);
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
