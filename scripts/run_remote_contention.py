#!/usr/bin/env python3
"""Run one bounded service/client fixture locally or on a trusted SSH server host.

The default is a TCP loopback lifecycle smoke, not multi-machine evidence.
--ssh-host explicitly enables remote commands; this script never provisions a
machine or installs software. Supply the same built benchmark binary on both
hosts. The service TCP endpoint has NO authentication or TLS: use only an isolated
trusted test network. SSH protects setup/final transfer, not benchmark RPC.
"""
from __future__ import annotations
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time
import uuid


DISPATCH_DEFAULTS = {"dispatch": "identity", "affinity_ttl_ms": 0, "signature_pattern": "both"}


def validate_dispatch_setup(setup: dict, expected: dict) -> None:
    """Missing fields identify the historic identity/both control only."""
    if any(type(setup.get(field, default)) is not type(expected.get(field, default))
           or setup.get(field, default) != expected.get(field, default)
           for field, default in DISPATCH_DEFAULTS.items()):
        raise RuntimeError("server setup dispatch/signature configuration differs from the client")


# A remote owner can die while cooperative GC shutdown is blocked. This
# independent supervisor survives SSH hangup and bounds the entire owned group,
# rather than depending on the owner's watchdog thread surviving that death.
# WNOWAIT retains the leader PID until after killpg, preventing PID/group reuse.
REMOTE_SUPERVISOR = r'''
import ctypes, json, os, signal, subprocess, sys, time
if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0) != 0:
    raise RuntimeError("cannot establish owned-descendant subreaper")
signal.signal(signal.SIGHUP, signal.SIG_IGN)
def terminate(signum, frame):
    raise RuntimeError("supervisor received signal %d" % signum)
signal.signal(signal.SIGTERM, terminate)
timeout=float(sys.argv[1]); evidence=sys.argv[2]; command=sys.argv[3:]
process=None; status=125; reason="startup_failed"; killed=False; reaped=0
try:
    process=subprocess.Popen(command,start_new_session=True)
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        ended=os.waitid(os.P_PID,process.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)
        if ended is not None:
            reason="owner_exited"; break
        time.sleep(0.05)
    else:
        reason="supervisor_deadline"
except BaseException as error:
    reason=str(error)
finally:
    if process is not None:
        try:
            os.killpg(process.pid,signal.SIGKILL); killed=True
        except ProcessLookupError:
            pass
        status=process.wait(timeout=10)
        reap_deadline=time.monotonic()+10
        while True:
            try:
                child,_=os.waitpid(-1,os.WNOHANG)
            except ChildProcessError:
                break
            if child:
                reaped+=1; continue
            if time.monotonic()>=reap_deadline:
                reason+=";descendant_reap_deadline"; status=125; break
            time.sleep(0.01)
    try:
        temporary=evidence+".tmp"
        with open(temporary,"w") as stream:
            json.dump({"reason":reason,"owner_pid":None if process is None else process.pid,
                       "owner_status":status,"owned_group_kill_sent":killed,
                       "adopted_descendants_reaped":reaped},stream)
        os.replace(temporary,evidence)
    except OSError:
        pass
sys.exit(status if 0<=status<=255 else 1)
'''


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def ssh_command(host: str, command: list[str]) -> list[str]:
    # Remote shell receives one deliberately quoted argument vector. No user
    # text is interpolated into shell program syntax.
    return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2",
            "--", host, shlex.join(command)]


def checked(command: list[str], timeout: float = 15) -> bytes:
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"command exited {result.returncode}: {shlex.join(command)}\n"
                           + result.stderr.decode(errors="replace"))
    return result.stdout


def read_setup(path: str, host: str | None) -> object | None:
    if host:
        result = subprocess.run(ssh_command(host, ["cat", "--", path]),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=15, check=False)
        if result.returncode:
            return None
        data = result.stdout
    else:
        try:
            data = Path(path).read_bytes()
        except FileNotFoundError:
            return None
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return None


def ready_setup(frame: object) -> dict | None:
    # The CLI invalidates a previous report before parsing. That configuration
    # placeholder is deliberately not a ready server frame, even though it is
    # already valid JSON at the requested output path.
    if (isinstance(frame, dict) and frame.get("version") == 1
            and isinstance(frame.get("run_id"), str) and frame["run_id"]
            and isinstance(frame.get("initial_rows"), list)
            and isinstance(frame.get("endpoint"), dict)):
        return frame
    return None


def validate_marker(frame: object, run_id: str) -> None:
    if (not isinstance(frame, dict) or frame.get("run_id") != run_id
            or frame.get("completed") is not True):
        raise RuntimeError("client completion marker is not for this run")


def validate_final(frame: object, run_id: str) -> dict:
    if (not isinstance(frame, dict) or frame.get("version") != 1
            or frame.get("run_id") != run_id
            or not isinstance(frame.get("passed"), bool)):
        raise RuntimeError("server final state missing, malformed or stale")
    return frame


def terminate_group(process: subprocess.Popen | None) -> None:
    if process is not None and process.poll() is None:
        try:
            # Local owners run under the independent supervisor, which handles
            # TERM by killing/reaping its owned group before it exits. For SSH,
            # disconnect lets the remote supervisor/owner observe stdin EOF.
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)


def wait_until(predicate, deadline: float, processes=()):
    while time.monotonic() < deadline:
        value = predicate()
        if value is not None:
            return value
        for name, process in processes:
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited {process.returncode} before handshake completed")
        time.sleep(0.1)
    raise TimeoutError("bounded benchmark handshake deadline expired")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True, help="new evidence directory; must not already exist")
    parser.add_argument("--ssh-host", help="explicitly enable remote execution on this trusted SSH target")
    parser.add_argument("--remote-binary", help="absolute path to the same binary already present on SSH server")
    parser.add_argument("--remote-directory", help="new owned remote evidence directory (default unique /tmp path)")
    parser.add_argument("--advertise-address", help="numeric server IP reachable from the client; required with --ssh-host")
    parser.add_argument("--bind", default=None, help="server listen SocketAddr; default127.0.0.1:0 locally,0.0.0.0:0 over SSH")
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=None, help="whole-run deadline; default seconds+240, maximum3600")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--families", type=int, default=16)
    parser.add_argument("--seed", type=int, default=20260925)
    parser.add_argument("--workload", choices=["legacy", "lifecycle", "fleet", "calibrated"], default="lifecycle")
    parser.add_argument("--projection-interval-seconds", type=int, default=300,
                        help="calibrated profile: independent wall-clock projection interval")
    parser.add_argument("--housekeeping-interval-seconds", type=int, default=600,
                        help="calibrated profile: independent wall-clock housekeeping interval")
    parser.add_argument("--dispatch", choices=["identity", "signature-affinity"], default="identity")
    parser.add_argument("--affinity-ttl-ms", type=int, default=0,
                        help="explicit sliding TTL in 1..3600000 ms required for signature-affinity")
    parser.add_argument("--signature-pattern", choices=["both", "mixed"], default="both")
    parser.add_argument("--evidence", choices=["full", "metrics"], default="metrics")
    parser.add_argument("--arrival-rate", type=int, default=100)
    parser.add_argument("--max-backlog", type=int, default=1000)
    parser.add_argument("--max-messages", type=int, default=20000)
    parser.add_argument("--shm-mib", type=int, default=256)
    parser.add_argument("--hot-percent", type=int, default=80)
    parser.add_argument("--query-plan", choices=["family", "global-time"], default="family")
    args = parser.parse_args()
    if not 0 <= args.affinity_ttl_ms <= 3600000 or (args.dispatch == "signature-affinity") != (args.affinity_ttl_ms > 0):
        parser.error("signature-affinity requires an explicit TTL in 1..3600000 ms; identity requires TTL 0")
    if args.workload != "calibrated" and any(getattr(args, field) != value for field, value in DISPATCH_DEFAULTS.items()):
        parser.error("dispatch/signature overrides apply only to --workload calibrated")
    timeout = args.timeout if args.timeout is not None else args.seconds + 240
    if args.workload == "fleet" and (args.families < 16 or args.hot_percent != 0):
        parser.error("fleet requires at least 16 families and --hot-percent 0")
    if args.workload == "calibrated" and (not 4 <= args.families <= 1024 or args.hot_percent != 0):
        parser.error("calibrated requires 4..1024 families and --hot-percent 0")
    if (not 1 <= args.projection_interval_seconds <= 3600
            or not 1 <= args.housekeeping_interval_seconds <= 3600):
        parser.error("maintenance intervals must be between 1 and 3600 seconds")
    if args.workload == "calibrated" and args.arrival_rate <= 0:
        parser.error("calibrated requires a positive fixed foreground --arrival-rate")
    if args.workload != "calibrated" and (args.projection_interval_seconds != 300
                                         or args.housekeeping_interval_seconds != 600):
        parser.error("maintenance interval overrides apply only to --workload calibrated")
    if not 1 <= args.seconds < timeout <= 3600:
        parser.error("require 1 <= seconds < timeout <=3600")
    if args.ssh_host and (not args.remote_binary or not args.advertise_address):
        parser.error("--ssh-host requires --remote-binary and --advertise-address")
    if args.remote_binary and not Path(args.remote_binary).is_absolute():
        parser.error("--remote-binary must be absolute")
    if args.advertise_address:
        ipaddress.ip_address(args.advertise_address)
    binary = args.binary.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    server_setup = str(output / "server-setup.json")
    if args.ssh_host:
        remote_directory = args.remote_directory or f"/tmp/aerostore-remote-{uuid.uuid4().hex}"
        if not Path(remote_directory).is_absolute():
            parser.error("remote directory must be absolute")
        server_setup = str(Path(remote_directory) / "server-setup.json")
    else:
        remote_directory = None
    client_setup = output / "client-setup.json"
    client_final = output / "client-final.json"
    complete = client_setup.with_suffix(".client-complete.json")
    server_final = str(Path(server_setup).with_suffix(".final.json"))
    bind = args.bind or ("0.0.0.0:0" if args.ssh_host else "127.0.0.1:0")
    common = ["--workload", args.workload, "--families", str(args.families),
              "--seed", str(args.seed), "--shm-mib", str(args.shm_mib),
              "--query-plan", args.query_plan, "--hot-percent", str(args.hot_percent),
              "--projection-interval-seconds", str(args.projection_interval_seconds),
              "--housekeeping-interval-seconds", str(args.housekeeping_interval_seconds),
              "--dispatch", args.dispatch, "--affinity-ttl-ms", str(args.affinity_ttl_ms),
              "--signature-pattern", args.signature_pattern]
    server_args = [args.remote_binary if args.ssh_host else str(binary),
                   "--mode", "serve", "--engine", "service-tcp", "--seconds", str(timeout),
                   "--service-bind", bind, "--output", server_setup, *common]
    supervised_server = ["python3" if args.ssh_host else sys.executable, "-c", REMOTE_SUPERVISOR,
        str(timeout+95), str(Path(server_setup).with_suffix(".supervisor.json")), *server_args]
    server_command = ssh_command(args.ssh_host, ["exec", *supervised_server]) if args.ssh_host else supervised_server
    client_command = [str(binary), "--engine", "service-remote", "--mode", "sustained",
                      "--remote-setup", str(client_setup), "--remote-final", str(client_final),
                      "--seconds", str(args.seconds), "--workers", str(args.workers),
                      "--evidence", args.evidence, "--arrival-rate", str(args.arrival_rate),
                      "--max-backlog", str(args.max_backlog), "--max-messages", str(args.max_messages),
                      "--output", str(output / "client-report.json"), *common]
    manifest = {"passed": False, "completed": False, "topology": "ssh_external_host" if args.ssh_host else "tcp_loopback",
                "physical_hosts_independently_verified": False,
                "architecture_promotion_eligible": False,
                "binary_sha256": sha256(binary), "server_command": server_command,
                "client_command": client_command, "server_setup": server_setup,
                "trusted_test_network_required": True, "benchmark_rpc_authenticated": False,
                "remote_cleanup_supervision": "independent bounded Linux Python subreaper",
                "timeout_seconds": timeout, "server_resources_cleanly_drained": False}
    atomic_json(output / "orchestration.json", manifest)
    server = client = None
    started = time.monotonic()
    deadline = started + timeout
    try:
        if args.ssh_host:
            # These remote actions occur only when the caller explicitly supplies
            # --ssh-host. No upload/install/deployment is hidden in this helper.
            checked(ssh_command(args.ssh_host, ["mkdir", "-m", "700", "--", remote_directory]))
            remote_hash = checked(ssh_command(args.ssh_host, ["sha256sum", "--", args.remote_binary])).decode().split()[0]
            if remote_hash != manifest["binary_sha256"]:
                raise RuntimeError("local/server binary SHA256 mismatch")
            manifest["remote_binary_sha256"] = remote_hash
        with (output / "server.log").open("wb") as server_log, (output / "client.log").open("wb") as client_log:
            server = subprocess.Popen(server_command, stdin=subprocess.PIPE, stdout=server_log,
                                      stderr=subprocess.STDOUT, start_new_session=True)
            setup = wait_until(lambda: ready_setup(read_setup(server_setup, args.ssh_host)),
                               min(deadline, time.monotonic()+60), [("server", server)])
            validate_dispatch_setup(setup, vars(args))
            manifest["run_id"] = setup["run_id"]
            if args.advertise_address:
                endpoint = setup.get("endpoint", {}).get("Tcp")
                if not isinstance(endpoint, str):
                    raise RuntimeError("server did not advertise a TCP endpoint")
                port = int(endpoint.rsplit(":", 1)[1])
                address = ipaddress.ip_address(args.advertise_address)
                setup["endpoint"] = {"Tcp": f"[{address}]:{port}" if address.version == 6 else f"{address}:{port}"}
            atomic_json(client_setup, setup)
            atomic_json(output / "orchestration.json", manifest)
            supervised_client = [sys.executable, "-c", REMOTE_SUPERVISOR, str(timeout+15),
                                 str(output / "client.supervisor.json"), *client_command]
            client = subprocess.Popen(supervised_client, stdin=subprocess.DEVNULL, stdout=client_log,
                                      stderr=subprocess.STDOUT, start_new_session=True)
            marker = wait_until(lambda: read_setup(str(complete), None), deadline,
                                [("server", server), ("client", client)])
            validate_marker(marker, setup["run_id"])
            server.stdin.write(b"finish\n")
            server.stdin.flush()
            server.stdin.close()
            status = server.wait(timeout=max(1, deadline-time.monotonic()))
            final = validate_final(read_setup(server_final, args.ssh_host), setup["run_id"])
            atomic_json(client_final, final)
            manifest["server_resources_cleanly_drained"] = final.get("passed") is True and final.get("registrations_after_stop") == 0
            if status != 0:
                raise RuntimeError(f"server exited {status}; final error: {final.get('error')}")
            status = client.wait(timeout=max(1, deadline-time.monotonic()))
            report = read_setup(str(output / "client-report.json"), None)
            if status != 0 or not isinstance(report, dict) or report.get("passed") is not True:
                raise RuntimeError(f"client correctness/progress report failed (exit{status})")
            manifest["completed"] = True
            manifest["passed"] = True
    except (OSError, RuntimeError, TimeoutError, subprocess.SubprocessError, ValueError) as error:
        manifest["error"] = str(error)
    finally:
        terminate_group(client)
        terminate_group(server)
        manifest["wall_seconds"] = time.monotonic()-started
        atomic_json(output / "orchestration.json", manifest)
    print(json.dumps({"passed":manifest["passed"],"evidence":str(output),"error":manifest.get("error")}))
    return 0 if manifest["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
