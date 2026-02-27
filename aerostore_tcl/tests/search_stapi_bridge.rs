#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant};

#[test]
fn tcl_search_uses_stapi_path_for_list_and_raw_compare_inputs() {
    let libpath = find_tcl_cdylib().expect("failed to locate libaerostore_tcl shared library");
    let script_template = r#"
load __LIBPATH__ Aerostore
package require aerostore
set _ [aerostore::init ./tmp/aerostore_tcl_it]

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

set malformed_raw "{> altitude 10000"
if {[catch {FlightState search -compare $malformed_raw -limit 5} err1] == 0} {
    error "malformed raw STAPI compare unexpectedly succeeded"
}
if {![string match "TCL_ERROR:*" $err1]} {
    error "malformed raw STAPI error missing TCL_ERROR prefix: $err1"
}

if {[catch {FlightState search -bogus value -limit 5} err2] == 0} {
    error "unknown search option unexpectedly succeeded"
}
if {![string match "TCL_ERROR:*" $err2]} {
    error "unknown option error missing TCL_ERROR prefix: $err2"
}

puts "bridge_ok"
"#;
    let script = script_template.replace("__LIBPATH__", tcl_quote_path(libpath.as_path()).as_str());

    let script_path = write_temp_script(script.as_str()).expect("failed to write temp Tcl script");
    let status = run_tcl_script_with_timeout(script_path.as_path(), Duration::from_secs(20))
        .expect("failed to execute tcl bridge script");
    let _ = std::fs::remove_file(script_path);

    assert!(
        status.success(),
        "tcl bridge script exited with non-zero status: {}",
        status
    );
}

fn run_tcl_script_with_timeout(
    script_path: &Path,
    timeout: Duration,
) -> Result<std::process::ExitStatus, String> {
    let mut child = Command::new("tclsh")
        .arg(script_path)
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

    let candidates = shared_library_candidates();
    for root in candidate_roots(profile_dir, deps_dir) {
        for name in candidates {
            let path = root.join(name);
            if path.exists() {
                return Ok(path);
            }
        }
    }

    build_cdylib_for_profile(profile_dir)?;

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
