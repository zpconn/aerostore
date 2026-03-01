#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant};

const SHM_PATH_ENV_KEY: &str = "AEROSTORE_SHM_PATH";

#[test]
fn tcl_search_uses_stapi_path_for_list_and_raw_compare_inputs() {
    let libpath = find_tcl_cdylib().expect("failed to locate libaerostore_tcl shared library");
    let data_dir = unique_temp_dir("aerostore_tcl_it");
    let shm_path = unique_tmpfs_shm_path("aerostore_tcl_bridge");
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(&shm_path);
    let script_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init __DATA_DIR__]

set batch [join [list \
    "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" \
    "UAL555\t41.974200\t-87.907300\t41000\t472\t1709000001" \
    "DAL789\t40.641300\t-73.778100\t9000\t310\t1709000002" \
    "AAL456\t33.942500\t-118.408100\t12000\t390\t1709000003" \
] "\n"]
FlightState ingest_tsv $batch 64

set compare_list {{> altitude 10000} {match ident UAL*}}
set compare_raw "{> altitude 10000} {match ident UAL*}"

set list_count [FlightState search -compare $compare_list -sort altitude -limit 50]
set raw_count [FlightState search -compare $compare_raw -sort altitude -limit 50]
if {$list_count != $raw_count} {
    error "list/raw compare mismatch: list=$list_count raw=$raw_count"
}
if {$list_count != 2} {
    error "unexpected match count: $list_count"
}

set compare_gte_list {{>= altitude 12000} {match ident UAL*}}
set compare_gte_raw "{>= altitude 12000} {match ident UAL*}"
set gte_list_count [FlightState search -compare $compare_gte_list -sort altitude -limit 50]
set gte_raw_count [FlightState search -compare $compare_gte_raw -sort altitude -limit 50]
if {$gte_list_count != $gte_raw_count} {
    error ">= list/raw compare mismatch: list=$gte_list_count raw=$gte_raw_count"
}
if {$gte_list_count != 2} {
    error "unexpected >= match count: $gte_list_count"
}

set compare_lte_list {{<= altitude 12000}}
set compare_lte_raw "{<= altitude 12000}"
set lte_list_count [FlightState search -compare $compare_lte_list -sort altitude -limit 50]
set lte_raw_count [FlightState search -compare $compare_lte_raw -sort altitude -limit 50]
if {$lte_list_count != $lte_raw_count} {
    error "<= list/raw compare mismatch: list=$lte_list_count raw=$lte_raw_count"
}
if {$lte_list_count != 2} {
    error "unexpected <= match count: $lte_list_count"
}

set compare_ne_list {{!= ident UAL123}}
set compare_ne_raw "{!= ident UAL123}"
set ne_list_count [FlightState search -compare $compare_ne_list -limit 50]
set ne_raw_count [FlightState search -compare $compare_ne_raw -limit 50]
if {$ne_list_count != $ne_raw_count} {
    error "!= list/raw compare mismatch: list=$ne_list_count raw=$ne_raw_count"
}
if {$ne_list_count != 3} {
    error "unexpected != match count: $ne_list_count"
}

set compare_notmatch_list {{notmatch ident UAL*}}
set compare_notmatch_raw "{notmatch ident UAL*}"
set notmatch_list_count [FlightState search -compare $compare_notmatch_list -limit 50]
set notmatch_raw_count [FlightState search -compare $compare_notmatch_raw -limit 50]
if {$notmatch_list_count != $notmatch_raw_count} {
    error "notmatch list/raw compare mismatch: list=$notmatch_list_count raw=$notmatch_raw_count"
}
if {$notmatch_list_count != 2} {
    error "unexpected notmatch count: $notmatch_list_count"
}

set compare_null_list {{null ident}}
set compare_null_raw "{null ident}"
set null_list_count [FlightState search -compare $compare_null_list -limit 50]
set null_raw_count [FlightState search -compare $compare_null_raw -limit 50]
if {$null_list_count != $null_raw_count} {
    error "null list/raw compare mismatch: list=$null_list_count raw=$null_raw_count"
}
if {$null_list_count != 0} {
    error "unexpected null match count: $null_list_count"
}

set compare_notnull_list {{notnull ident}}
set compare_notnull_raw "{notnull ident}"
set notnull_list_count [FlightState search -compare $compare_notnull_list -limit 50]
set notnull_raw_count [FlightState search -compare $compare_notnull_raw -limit 50]
if {$notnull_list_count != $notnull_raw_count} {
    error "notnull list/raw compare mismatch: list=$notnull_list_count raw=$notnull_raw_count"
}
if {$notnull_list_count != 4} {
    error "unexpected notnull match count: $notnull_list_count"
}

set pk_list {{= flight_id UAL123}}
set pk_raw "{= flight_id UAL123}"
set pk_list_count [FlightState search -compare $pk_list -limit 10]
set pk_raw_count [FlightState search -compare $pk_raw -limit 10]
if {$pk_list_count != $pk_raw_count} {
    error "pk list/raw compare mismatch: list=$pk_list_count raw=$pk_raw_count"
}
if {$pk_list_count != 1} {
    error "unexpected pk match count: $pk_list_count"
}

set mixed_list {{= flight_id UAL123} {> altitude 10000} {match ident UAL*}}
set mixed_raw "{= flight_id UAL123} {> altitude 10000} {match ident UAL*}"
set mixed_list_count [FlightState search -compare $mixed_list -sort altitude -limit 10]
set mixed_raw_count [FlightState search -compare $mixed_raw -sort altitude -limit 10]
if {$mixed_list_count != $mixed_raw_count} {
    error "mixed list/raw compare mismatch: list=$mixed_list_count raw=$mixed_raw_count"
}
if {$mixed_list_count != 1} {
    error "unexpected mixed predicate match count: $mixed_list_count"
}

set malformed_raw "{> altitude 10000"
if {[catch {FlightState search -compare $malformed_raw -limit 5} err1] == 0} {
    error "malformed raw STAPI compare unexpectedly succeeded"
}
if {![string match "TCL_ERROR:*" $err1]} {
    error "malformed raw STAPI error missing TCL_ERROR prefix: $err1"
}

set malformed_null_arity "{null ident extra}"
if {[catch {FlightState search -compare $malformed_null_arity -limit 5} err_null] == 0} {
    error "malformed null arity unexpectedly succeeded"
}
if {![string match "TCL_ERROR:*" $err_null]} {
    error "malformed null arity missing TCL_ERROR prefix: $err_null"
}

set malformed_notnull_arity "{notnull ident extra}"
if {[catch {FlightState search -compare $malformed_notnull_arity -limit 5} err_notnull] == 0} {
    error "malformed notnull arity unexpectedly succeeded"
}
if {![string match "TCL_ERROR:*" $err_notnull]} {
    error "malformed notnull arity missing TCL_ERROR prefix: $err_notnull"
}

set malformed_notmatch_arity "{notmatch ident}"
if {[catch {FlightState search -compare $malformed_notmatch_arity -limit 5} err_notmatch] == 0} {
    error "malformed notmatch arity unexpectedly succeeded"
}
if {![string match "TCL_ERROR:*" $err_notmatch]} {
    error "malformed notmatch arity missing TCL_ERROR prefix: $err_notmatch"
}

if {[catch {FlightState search -bogus value -limit 5} err2] == 0} {
    error "unknown search option unexpectedly succeeded"
}
if {![string match "TCL_ERROR:*" $err2]} {
    error "unknown option error missing TCL_ERROR prefix: $err2"
}

set sync_mode [aerostore::get_config aerostore.synchronous_commit]
if {$sync_mode ne "on"} {
    error "expected default synchronous_commit mode on, got: $sync_mode"
}

aerostore::set_config aerostore.synchronous_commit off
set sync_mode [aerostore::get_config aerostore.synchronous_commit]
if {$sync_mode ne "off"} {
    error "expected synchronous_commit mode off after toggle, got: $sync_mode"
}

if {[catch {aerostore::checkpoint_now} checkpoint_err] == 0} {
    error "checkpoint unexpectedly succeeded while synchronous_commit=off"
}
if {![string match "TCL_ERROR:*" $checkpoint_err]} {
    error "checkpoint error missing TCL_ERROR prefix: $checkpoint_err"
}

aerostore::set_config aerostore.synchronous_commit on
set sync_mode [aerostore::get_config aerostore.synchronous_commit]
if {$sync_mode ne "on"} {
    error "expected synchronous_commit mode on after restore, got: $sync_mode"
}

aerostore::set_config aerostore.checkpoint_interval_secs 1
set checkpoint_interval [aerostore::get_config aerostore.checkpoint_interval_secs]
if {$checkpoint_interval != 1} {
    error "expected checkpoint interval 1, got: $checkpoint_interval"
}

set checkpoint_ok [aerostore::checkpoint_now]
if {![string match "checkpoint rows=*" $checkpoint_ok]} {
    error "unexpected checkpoint success payload: $checkpoint_ok"
}

puts "bridge_ok"
"#;
    let script = script_template
        .replace("__LIBPATH__", tcl_quote_path(libpath.as_path()).as_str())
        .replace("__DATA_DIR__", tcl_quote_path(data_dir.as_path()).as_str());

    let script_path = write_temp_script(script.as_str()).expect("failed to write temp Tcl script");
    let status = run_tcl_script_with_timeout(
        script_path.as_path(),
        Duration::from_secs(20),
        shm_path.as_path(),
    )
    .expect("failed to execute tcl bridge script");
    let _ = std::fs::remove_file(script_path);
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(shm_path);

    assert!(
        status.success(),
        "tcl bridge script exited with non-zero status: {}",
        status
    );
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
    // Backslash is special in Tcl strings, so normalize to forward slashes.
    raw.replace('\\', "/")
}

fn write_temp_script(script: &str) -> Result<PathBuf, String> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("clock error: {}", e))?
        .as_nanos();
    let path = std::env::temp_dir().join(format!("aerostore_tcl_bridge_{nonce}.tcl"));
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
