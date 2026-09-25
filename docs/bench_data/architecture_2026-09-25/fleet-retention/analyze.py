#!/usr/bin/env python3
"""Stream original histories into small, reproducible retention summaries.

Creation: python3 analyze.py --history-root target/.../retention-final
Recheck:  python3 analyze.py --verify

No database access. The compressed projection retains timing/outcome inputs,
not the complete transaction observations needed to rerun the serial oracle.
"""
import argparse
import collections
import csv
import gzip
import hashlib
import heapq
import json
import pathlib
import shutil
import tarfile

HERE = pathlib.Path(__file__).resolve().parent
KINDS = {
    "Plan": "plan", "Position": "position", "Arrival": "arrival",
    "ExpireFamily": "expire_family", "GlobalCancel": "global_cancel",
    "GlobalHousekeeping": "global_housekeeping",
    "GlobalProject": "global_projection", "GlobalReschedule": "global_reschedule",
}
OUTCOMES = [
    "updated_views", "created_views", "ignored_stale", "expired_records",
    "outputs", "missing_family", "allocation_deferred", "claimed_events",
    "cancelled_events", "rescheduled_events", "expired_families", "duplicate_messages",
]
TIMING = [
    "id", "worker", "scheduled_ns", "message_started_ns", "received_ns",
    "end_to_end_latency_ns", "service_latency_ns", "retries", "write_operations",
    "successful_attempt_started_ns", "successful_attempt_finished_ns",
]
FIELDS = ["kind"] + TIMING + OUTCOMES
WINDOW_NS = 10_000_000_000


def read_json(path):
    return json.loads(path.read_text())


def write_json(path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def file_info(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def project(history, destination):
    digest = hashlib.sha256()
    count = 0
    with history.open("rb") as source, gzip.open(destination, "wt", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        for line in source:
            digest.update(line)
            raw = json.loads(line)
            receipt = raw["receipt"]
            outcome = receipt["body"]["outcome"]
            tag = receipt["message"]["kind"]
            tag = tag if isinstance(tag, str) else next(iter(tag))
            row = {key: raw[key] for key in TIMING if key not in (
                "id", "write_operations", "successful_attempt_started_ns", "successful_attempt_finished_ns")}
            row.update(id=receipt["message"]["id"], kind=KINDS[tag],
                       successful_attempt_started_ns=receipt["started"],
                       successful_attempt_finished_ns=receipt["finished"],
                       write_operations=sum("Write" in op for op in receipt["body"]["operations"]))
            for key in OUTCOMES:
                value = outcome["duplicate" if key == "duplicate_messages" else key]
                row[key] = len(value) if key == "outputs" else int(value)
            writer.writerow(row)
            count += 1
    return {"path": str(history.resolve()), "bytes": history.stat().st_size,
            "sha256": digest.hexdigest(), "messages": count}


def load_projection(path):
    with gzip.open(path, "rt", newline="") as source:
        return [{key: value if key == "kind" else int(value) for key, value in row.items()}
                for row in csv.DictReader(source)]


def percentile(values, percent):
    ordered = sorted(values)
    return ordered[(len(ordered) * percent + 99) // 100 - 1] / 1000


def summarize_rows(rows):
    latencies = [row["end_to_end_latency_ns"] for row in rows]
    return {
        "messages": len(rows), "retries": sum(row["retries"] for row in rows),
        "messages_retried": sum(row["retries"] > 0 for row in rows),
        "e2e_p50_us": percentile(latencies, 50),
        "e2e_p99_us": percentile(latencies, 99), "e2e_max_us": percentile(latencies, 100),
        "service_p99_us": percentile([row["service_latency_ns"] for row in rows], 99),
        "arrival_queue_p99_us": percentile(
            [row["message_started_ns"] - row["scheduled_ns"] for row in rows], 99),
        "transcripts_without_write_operations": sum(row["write_operations"] == 0 for row in rows),
        "transcript_write_operations": sum(row["write_operations"] for row in rows),
        "outcomes": {key: sum(row[key] for row in rows) for key in OUTCOMES},
        "message_kinds": dict(sorted(collections.Counter(row["kind"] for row in rows).items())),
    }


def flatten_storage(storage):
    values = {key: value for key, value in storage.items() if type(value) in (int, float)}
    if "indexes" in storage:
        for key in ("alloc_failures", "gc_recycle_errors", "reclaimed_postings", "retired_nodes", "retired_postings"):
            values["index_" + key + "_sum"] = sum(index[key] for index in storage["indexes"])
    return values


def summarize_samples(samples):
    if not samples:
        return {"sample_count": 0}
    values = [flatten_storage(sample["storage"]) for sample in samples]
    return {
        "sample_count": len(samples),
        "first_sample_offset_ns": samples[0]["sample_offset_ns"],
        "last_sample_offset_ns": samples[-1]["sample_offset_ns"],
        "maximum_sample_duration_ns": max(sample["sample_finished_ns"] - sample["sample_started_ns"] for sample in samples),
        "gauges_and_counters": {
            key: {"first": values[0][key], "last": values[-1][key],
                  "min": min(value[key] for value in values), "max": max(value[key] for value in values)}
            for key in sorted(values[0])
        },
    }


def overlap(intervals):
    """Count actual overlapping half-open intervals, with ends before equal starts."""
    active = []
    pairs = 0
    maximum = 0
    participants = set()
    for start, end, message in sorted(intervals):
        assert start < end
        while active and active[0][0] <= start:
            heapq.heappop(active)
        pairs += len(active)
        if active:
            participants.add(message)
            participants.update(other for _, other in active)
        heapq.heappush(active, (end, message))
        maximum = max(maximum, len(active))
    return {"maximum_simultaneous_intervals": maximum, "overlapping_pairs": pairs,
            "messages_overlapping_another": len(participants)}


def population(path):
    with gzip.open(path, "rt") as source:
        rows = [row for row in json.load(source) if row["active"]]
    return {"active_rows": len(rows),
            "live_families": len({row["family"] for row in rows if row["kind"] == 1}),
            "flight_views": sum(row["kind"] == 1 for row in rows),
            "retained_positions": sum(row["kind"] == 2 for row in rows),
            "scheduled_events": sum(row["kind"] == 3 for row in rows),
            "scope": "initial/final snapshot population, not a measured runtime minimum"}


def analyze(engine_directory):
    report = read_json(engine_directory / "report.json")
    run = report["runs"][0]
    trial = read_json(engine_directory / "trial.json")
    rows = load_projection(engine_directory / "message-timing-outcomes.csv.gz")
    rows.sort(key=lambda row: row["scheduled_ns"])
    epoch = rows[0]["scheduled_ns"]
    assert len(rows) == len({row["id"] for row in rows}) == run["completed_messages"] == 12000
    assert all(row["scheduled_ns"] == epoch + index * 10_000_000 for index, row in enumerate(rows))
    assert all(row["received_ns"] - row["scheduled_ns"] == row["end_to_end_latency_ns"] for row in rows)
    assert all(row["scheduled_ns"] <= row["message_started_ns"] <= row["received_ns"] for row in rows)
    assert all(row["service_latency_ns"] <= row["received_ns"] - row["message_started_ns"] for row in rows)
    assert all(row["message_started_ns"] <= row["successful_attempt_started_ns"] <
               row["successful_attempt_finished_ns"] <= row["received_ns"] for row in rows)
    workers = collections.Counter(row["worker"] for row in rows)
    assert [workers[i] for i in range(report["config"]["workers"])] == run["completed_by_worker"]
    aggregate = summarize_rows(rows)
    for ours, theirs in (
        ("e2e_p50_us", "message_latency_p50_us_including_retries"),
        ("e2e_p99_us", "message_latency_p99_us_including_retries"),
        ("e2e_max_us", "message_latency_max_us_including_retries"),
        ("service_p99_us", "service_latency_p99_us_including_retries"),
        ("arrival_queue_p99_us", "arrival_queue_delay_p99_us"),
        ("retries", "retries"), ("message_kinds", "message_kinds"),
    ):
        assert aggregate[ours] == run[theirs], (run["engine"], ours, aggregate[ours], run[theirs])
    assert aggregate["outcomes"] == trial["assessment"]["outcomes"]
    per_kind = {}
    for kind, original in run["per_kind"].items():
        subset = summarize_rows([row for row in rows if row["kind"] == kind])
        per_kind[kind] = subset
        assert subset["messages"] == original["completed"]
        assert subset["retries"] == original["retries"]
        assert subset["e2e_p99_us"] == original["p99_us_including_retries"]
        assert subset["outcomes"] == original["outcomes"]
    samples = []
    for raw in run["retention_samples"]:
        sample = dict(raw)
        sample["sample_offset_ns"] = (sample["sample_started_ns"] + sample["sample_finished_ns"]) // 2 - epoch
        samples.append(sample)
    windows = []
    for index in range(12):
        start, end = index * WINDOW_NS, (index + 1) * WINDOW_NS
        cohort = [row for row in rows if start <= row["scheduled_ns"] - epoch < end]
        assert len(cohort) == 1000
        windows.append({
            "start_seconds": index * 10, "end_seconds_exclusive": (index + 1) * 10,
            **summarize_rows(cohort),
            "storage_samples": summarize_samples([sample for sample in samples if start <= sample["sample_offset_ns"] < end]),
        })
    initial_population = population(engine_directory / "initial.json.gz")
    final_population = population(engine_directory / "final.json.gz")
    assert initial_population == run["initial_fleet"]
    assert final_population == run["final_fleet"]
    return {
        "engine": run["engine"], "workload_classification": "synthetic staggered fleet; not calibrated production HyperFeed",
        "source_workload_label": report["config"]["workload"],
        "admission_epoch_monotonic_ns": epoch, "aggregate": aggregate,
        "initial_population": initial_population, "final_population": final_population,
        "background_work": {key: value for key, value in per_kind.items() if key.startswith("global_")},
        "overlap": {
            "successful_transaction_attempts": overlap([
                (row["successful_attempt_started_ns"], row["successful_attempt_finished_ns"], row["id"])
                for row in rows]),
            "message_service_including_retries": overlap([
                (row["message_started_ns"], row["message_started_ns"] + row["service_latency_ns"], row["id"])
                for row in rows]),
            "scheduled_arrival_through_coordinator_receipt": overlap([
                (row["scheduled_ns"], row["received_ns"], row["id"]) for row in rows]),
            "scope": "half-open monotonic intervals on this host; successful-attempt intervals exclude failed attempts, service intervals include retries/backoff",
        },
        "all_messages_before_next_scheduled_arrival": all(
            earlier["received_ns"] < later["scheduled_ns"] for earlier, later in zip(rows, rows[1:])),
        "checks": {"unique_message_ids": True, "exact_100_hz_schedule": True,
                   "per_kind_counts_outcomes_retries_and_p99_match_report": True,
                   "overall_latency_retry_and_outcome_totals_match_report": True,
                   "timestamp_identities_match": True, "per_worker_counts_match": True},
        "oracle_status_from_original_run": run["oracle_status"],
        "all_sample_summary": summarize_samples(samples),
        "baseline_samples": [sample for sample in samples if sample["sample_offset_ns"] < 0],
        "samples_after_admission": [sample for sample in samples if sample["sample_offset_ns"] >= 120 * 1_000_000_000],
        "after_drain": run["after_drain"], "windows": windows,
    }


def create(source):
    sources = {}
    for directory in sorted(source.iterdir()):
        if not directory.is_dir() or not (directory / "report.json").exists():
            continue
        run = read_json(directory / "report.json")["runs"][0]
        destination = HERE / run["engine"]
        destination.mkdir(exist_ok=True)
        evidence = pathlib.Path(run["evidence_directory"])
        sources[run["engine"]] = {"original_history": project(evidence / "history.jsonl", destination / "message-timing-outcomes.csv.gz")}
        for name in ("report.json", "trial.json", "run.log"):
            shutil.copyfile(directory / name, destination / name)
        for name in ("initial.json", "final.json"):
            with gzip.open(destination / (name + ".gz"), "wb") as target:
                target.write((evidence / name).read_bytes())
        shutil.copyfile(evidence / "serial-witness.json", destination / "serial-witness.json")
        sources[run["engine"]]["original_report"] = file_info(directory / "report.json")
        sources[run["engine"]]["original_oracle_inputs"] = {
            name: file_info(evidence / name)
            for name in ("initial.json", "final.json", "serial-witness.json")
        }
    shutil.copyfile(source / "campaign.json", HERE / "campaign.json")
    shutil.copyfile(HERE.parent / "fleet-build-provenance.json", HERE / "build-provenance.json")
    provenance = read_json(HERE / "build-provenance.json")
    bundle = HERE.parent / "fleet-source.tar.gz"
    with tarfile.open(bundle) as source_tar:
        source_files = provenance["source_after"]["files"]
        assert len(source_files) == len(source_tar.getmembers())
        for name, digest in source_files.items():
            assert hashlib.sha256(source_tar.extractfile(name).read()).hexdigest() == digest, name
    sources["build"] = {"provenance": "build-provenance.json", "binary_sha256": provenance["binary_sha256"],
                        "source_bundle": file_info(bundle), "source_files_verified": len(source_files),
                        "source_inventory_sha256": provenance["source_after"]["sha256"]}
    write_json(HERE / "inputs.json", sources)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-root", type=pathlib.Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.history_root:
        if args.verify:
            parser.error("--history-root and --verify are mutually exclusive")
        create(args.history_root)
    provenance = read_json(HERE / "build-provenance.json")
    for engine in ("aerostore", "service-unix", "postgres"):
        trial = read_json(HERE / engine / "trial.json")
        assert trial["source_stable"]
        assert trial["binary_before_sha256"] == trial["binary_after_sha256"] == provenance["binary_sha256"]
        assert trial["source_before_sha256"] == trial["source_after_sha256"] == provenance["source_after"]["sha256"]
    engines = [analyze(HERE / engine) for engine in ("aerostore", "service-unix", "postgres")]
    result = {"version": 1, "window_definition": "scheduled-arrival cohorts [0,10), [10,20), ... relative to minimum scheduled_ns; delayed completions remain in their original cohort",
              "quantile_definition": "nearest rank: sorted[ceil(N * percentile / 100) - 1]; nanoseconds divided by 1000",
              "storage_window_definition": "observed sample midpoint relative to admission epoch; no interpolation or RSS inference; counters are cumulative",
              "evidence_limit": "compact projections reproduce these aggregates; full serial-oracle replay requires original complete histories identified in inputs.json",
              "runs": engines}
    if args.verify:
        for name, expected in read_json(HERE / "manifest.json")["files"].items():
            actual = file_info(HERE / name)
            assert actual["sha256"] == expected["sha256"] and actual["bytes"] == expected["bytes"], name
        assert result == read_json(HERE / "windows.json"), "window summary differs"
        print("PASS: 36,000 projected messages reconcile to three original reports; all 36 ten-second windows reproduce exactly")
    else:
        write_json(HERE / "windows.json", result)
        print("Created windows.json; 36,000 projected messages reconcile to original reports")


if __name__ == "__main__":
    main()
