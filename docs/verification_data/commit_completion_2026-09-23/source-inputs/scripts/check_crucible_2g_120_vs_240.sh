#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

for required in cargo python3; do
  if ! command -v "$required" >/dev/null 2>&1; then
    echo "error: $required is required" >&2
    exit 1
  fi
done

LOG_DIR="${AEROSTORE_CRUCIBLE_LOG_DIR:-/tmp/aerostore_crucible_2g_compare}"
mkdir -p "$LOG_DIR"
LOG_120="$LOG_DIR/crucible_profile_2g_120s.log"
LOG_240="$LOG_DIR/crucible_profile_2g_240s.log"

run_crucible() {
  local duration="$1"
  local log_file="$2"
  echo "==> Running sustained crucible for ${duration}s (Aerostore-only=${AEROSTORE_CRUCIBLE_AEROSTORE_ONLY:-0})"
  AEROSTORE_CRUCIBLE_DURATION_SECS="$duration" \
  AEROSTORE_CRUCIBLE_PROFILE_FILTER=profile_2g \
  AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH="${AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH:-${log_file%.log}.csv}" \
  cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot \
    2>&1 | tee "$log_file"
}

run_crucible 120 "$LOG_120"
run_crucible 240 "$LOG_240"

# Parse named fields instead of markdown column positions: extending telemetry
# must not silently change which counter a gate checks.
python3 - "$LOG_120" "$LOG_240" "${AEROSTORE_CRUCIBLE_AEROSTORE_ONLY:-0}" <<'PY'
import re
import sys
from pathlib import Path

paths = [Path(path) for path in sys.argv[1:3]]
only_aerostore = sys.argv[3] == "1"

def parse(path):
    text = path.read_text()
    def value(name):
        matches = re.findall(rf'\b{re.escape(name)}=([0-9.]+)', text)
        if not matches:
            raise SystemExit(f"FAIL: missing {name} in {path}")
        return float(matches[-1])
    rows = [line.split('|') for line in text.splitlines() if re.match(r'^\| aerostore \| [0-9]+(?:\.[0-9]+)? \|', line)]
    if len(rows) != 1:
        raise SystemExit(f"FAIL: expected one Aerostore result in {path}, got {len(rows)}")
    row = rows[0]
    if not re.search(r'hyperfeed_crucible_correctness: .*exact_match=true', text):
        raise SystemExit(f"FAIL: missing exact table/index correctness gate in {path}")
    if not re.search(r'hyperfeed_crucible_gc_drain: .*retired_backlog=0 gc_recycle_errors=0 retired_postings=0', text):
        raise SystemExit(f"FAIL: missing clean index GC drain in {path}")
    if not re.search(r'hyperfeed_crucible_allocation_audit: .*status=pass(?:\s|$)', text):
        raise SystemExit(f"FAIL: missing exact index allocation audit in {path}")
    return dict(tps=float(row[2]), remove_fail=int(row[11]), insert_fail=int(row[12]),
                ratio=None if only_aerostore else value('tps_ratio_aerostore_vs_postgres'),
                elapsed=value('aerostore_elapsed_secs'),
                max_insert=value('max_insert_attempts'), pressure=value('pressure_state'),
                tail_fresh=value('tail_fresh_bytes'), growth_budget=value('fresh_growth_budget_bytes'),
                retained_tps=value('retained_tps'))

short, long = map(parse, paths)
failures = []
if long['tps'] < short['tps'] * 0.9:
    failures.append('240s TPS is below 90% of 120s TPS')
if not only_aerostore and long['ratio'] < short['ratio'] * 0.9:
    failures.append('240s Aerostore/Postgres ratio is below 90% of 120s ratio')
for label, run, duration in [('120s', short, 120), ('240s', long, 240)]:
    if run['remove_fail'] or run['insert_fail']:
        failures.append(f'{label} index mutation failures are non-zero')
    if run['max_insert'] > 128:
        failures.append(f'{label} max_insert_attempts exceeds 128')
    if run['pressure'] == 2:
        failures.append(f'{label} end pressure_state is HOT')
    if run['elapsed'] > duration + 1:
        failures.append(f'{label} drain exceeds one second')
    if run['tail_fresh'] > run['growth_budget']:
        failures.append(f'{label} fresh allocation growth exceeds one seeded working set')
    if run['retained_tps'] < 0.5:
        failures.append(f'{label} interval throughput collapsed')
    print(f"{label}: TPS={run['tps']:.2f}, elapsed={run['elapsed']:.3f}s, "
          f"tail fresh={run['tail_fresh']:.0f} bytes, retained interval TPS={run['retained_tps']:.3f}")
print(f'Logs: {paths[0]}, {paths[1]}')
if failures:
    raise SystemExit('\n'.join(f'FAIL: {message}' for message in failures))
print('PASS: sustained correctness, memory, throughput, and drain gates satisfied.' +
      (' PostgreSQL comparison was not requested.' if only_aerostore else ' PostgreSQL comparison gates satisfied.'))
PY
