#!/usr/bin/env python3
import json
from pathlib import Path
base = Path(__file__).resolve().parent
x = json.loads((base / 'controlled-review.json').read_text())
assert x['passed'] and x['reviewed_cells'] == 90
assert sum(c['numeric_comparison_eligible'] for c in x['cells']) == 60
lines = [
'The next engine experiment should target broad time-predicate dependency and latch behavior, starting with the due index. The optional housekeeping eligibility filter is correct within the tested fixture, but these measurements do not establish it as a performance fix. Every failed native 2,048-message/s cell exhausted projection retries; diagnostic-enabled failures identify the due predicate, which the expiry filter does not change.',
'',
'The independent review accepted all 90 evidence collections without a mismatch. It recomputed qualification assessments, checked exact full companions, compared raw reports and trial files with campaign copies, and validated trace/failure schemas. There are 65 complete execution-valid cells and 60 useful, history/companion-verified, continuously timed cells eligible for numeric comparison. The other 25 execution failures and five complete but unsuitable cells remain visible. This review does not independently replay serial histories; the benchmark oracle provides that check for full-history trials.',
'',
'Artifacts: [review JSON](controlled-review.json.gz), [review script](review_controlled_campaign.py), [campaign execution](controlled-campaign/execution.json.gz). The JSON retains every seed, all full/metrics pairs, detailed failure snapshots, bounded tails, reasons, and hashes. The campaign lasted 556.2 seconds under the recorded isolation assertion; this is not enforced CPU isolation.',
'',
'All 60 cells at 128 and 512 messages/s completed useful foreground work, with exact full companions for metrics runs. Each completed six projection jobs and three housekeeping jobs, including the required committed empty terminal queries: 25 maintenance transactions and 224 processed rows. The single positive projection job and accelerated one-/two-second timers limit representativeness even when the bounded execution succeeds.',
'',
'Foreground p99 including retries, milliseconds; median [minimum, maximum] across three metrics runs:',
'',
'| Offered rate | Engine / policy | Diagnostics off | Diagnostics on |',
'| ---: | --- | ---: | ---: |',
]
for rate in (128, 512):
 for engine, policy in [('aerostore','all-active'), ('aerostore','housekeeping'), ('postgres','all-active')]:
  values=[]
  for diag in ('off','on'):
   g=next((g for g in x['groups'] if g['engine']==engine and g['rate']==rate and g['expiry_index_policy']==policy and g['retry_diagnostics']==diag and g['evidence']=='metrics'),None)
   s=g['qualified_metric_summary']['foreground_p99_ms'] if g else None
   values.append(f"{s['median']:.3f} [{s['min']:.3f}, {s['max']:.3f}]" if s else 'not run')
  lines.append(f"| {rate} | {engine} / {policy if engine!='postgres' else 'existing partial index'} | {' | '.join(values)} |")
lines += [
'',
'Paired changes below compare the same seed and evidence mode, with positive values meaning higher p99. These are medians of per-seed percentage changes, not ratios of separately selected medians. No failed or stale-work cell enters these calculations.',
'',
'| Comparison | Rate | Held fixed | Seed 20260924 | Seed 20260925 | Seed 20260926 | Median [min, max] |',
'| --- | ---: | --- | ---: | ---: | ---: | ---: |',
]
for name, label, field in [('runtime_diagnostic_pairs','on / off','expiry_index_policy'), ('expiry_policy_pairs','housekeeping / all-active','retry_diagnostics')]:
 for g in x[name]['groups']:
  if g['rate'] not in (128,512) or g['evidence']!='metrics':continue
  p=[p for p in x[name]['pairs'] if p['rate']==g['rate'] and p['evidence']=='metrics' and p[field]==g[field]]
  p.sort(key=lambda p:p['seed'])
  values=[f"{p['changes']['foreground_p99_ms']['percent_change']:+.2f}%" for p in p]
  s=g['changes']['foreground_p99_ms']['percent_change']
  lines.append(f"| {label} | {g['rate']} | {field}={g[field]} | {' | '.join(values)} | {s['median']:+.2f}% [{s['min']:+.2f}%, {s['max']:+.2f}%] |")
lines += [
'',
'The filter has a modest encouraging result at rate 512 with diagnostics off: every paired metrics p99 is lower, with median change −2.50% and range −12.91% to −0.76%; full-history pairs also improve, median −4.63%. Its metrics result with diagnostics on changes sign and spans −8.87% to +38.30%. At rate 128 the direction is inconsistent. Retain this control for further experiments; do not promote it as the solution to the high-rate failure.',
'',
'The tracing overhead has not been bounded reliably by this short matrix. Rate-512 housekeeping on/off p99 rises in all three seeds in both evidence modes (metrics median +6.62%, full median +7.01%), suggesting a real cost worth respecting. The all-active comparisons vary in direction. At rate 128, all-active full-history p99 rises by a median +154.26% while the corresponding metrics pairs have median −2.25%; those modes must not be pooled or interpreted as proving zero-cost tracing. Keep diagnostics off for performance qualification, and use separate diagnostic trials to investigate rejection causes. These measurements compare runtime on/off within one feature-enabled binary, not feature-enabled versus feature-compiled-out builds.',
'',
'At the same offered rate, drained throughput is admission-limited. The lower native p99 in these cells is not a 10× capacity result. For example, the rate-512 metrics median is 2.192 ms for baseline native versus 3.057 ms for PostgreSQL; neither engine was demonstrated to be at its maximum useful sustainable rate here. The PostgreSQL control uses its existing prepared/buffered SERIALIZABLE path and partial expiry index. Equal crash-loss windows and recovery guarantees are not established.',
'',
'At rate 2,048 there are no eligible numeric performance comparisons:',
'',
'| Engine / variant | Full: valid executions / 3 | Metrics: valid executions / 3 | Useful eligible cells |',
'| --- | ---: | ---: | ---: |',
]
for engine,policy,diag in [('aerostore','all-active','off'),('aerostore','all-active','on'),('aerostore','housekeeping','off'),('aerostore','housekeeping','on'),('postgres','all-active','off')]:
 gs=[next(g for g in x['groups'] if g['engine']==engine and g['rate']==2048 and g['expiry_index_policy']==policy and g['retry_diagnostics']==diag and g['evidence']==e) for e in ('full','metrics')]
 lines.append(f"| {engine} / {policy} / {diag} | {gs[0]['execution_valid']} | {gs[1]['execution_valid']} | 0 |")
lines += [
'',
'All 19 native execution failures identify worker 4, the projection worker, exhausting 128 retries. The six PostgreSQL failures instead exceed the declared 1,000-job per-worker backlog bound. None is a benchmark timeout. The five completed native executions contain 8, 12, 13, 38, or 78 stale view updates, so they fail the useful-work condition. Their correctness outcome is compatible with the deliberately permitted alias-driven reordering; it is not evidence of corrupt history. Three of these are full histories checked Valid; the two metrics executions do not verify their own histories. One of those metrics cells also lacks a successful full companion. No partial failure throughput or favorable completed subset is used as a replacement/capacity claim.',
'',
'Failure attribution is strongest for the ten diagnostic-enabled native failures. Every retained terminal attempt (attempt index 128, the 129th failed attempt) ends in `commit:predicate_validation_stamp:due`. The table keeps cumulative failed-worker observations separate from sampled tails; the latter are a subset of the former, not additional failures. Completed/running peers are not included in these failed-worker counters.',
'',
'| Evidence mode | Failed diagnostic cells | Failed-worker cumulative observations | Due commit stamp | Due lookup stamp | Other | Last-32 retained samples | Due commit / lookup in tails |',
'| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |',
'| Full | 4 | 893 | 803 | 88 | 2 | 128 | 119 / 9 |',
'| Metrics | 6 | 959 | 887 | 68 | 4 | 192 | 176 / 16 |',
'',
'The ten terminal attempts spent approximately 2.16–2.30 ms in a successful due lookup before commit rejected its predicate stamp. These timings do not measure latch hold duration. Lower-rate successful runs expose a related but different symptom: in rate-512 diagnostic metrics runs, due/event-time bucket-busy commit rejections account for 1,362 of 1,693 observations (80.45%) with the baseline index and 1,518 of 1,853 (81.92%) with the filtered index. Those counts are cumulative over three independent complete runs per policy, kept separate from the failed-worker table.',
'',
'The source provides a plausible mechanism, not proof that every observed rejection is avoidable. `shm_index.rs` maps range predicates to all 4,096 publication buckets. `OccTable::index_lookup` captures those bucket stamps, releases lookup guards before candidate MVCC materialization, and commit later reacquires dependencies for validation. A global due search can consequently conflict with scheduled-event changes outside its cutoff; broad acquisition also creates opportunities for bucket-budget failures. The branch counters identify rejection decisions but cannot distinguish necessary phantoms, unrelated key changes, or hash collisions for each individual event. The eligibility experiment changes event-time membership, leaving this due-index mechanism untouched.',
'',
'Proceed with a narrowly scoped due-range dependency experiment behind an explicit comparison option. First add deterministic cases for a matching insert, a matching removal/key movement, a nonmatching scheduled-event update outside the cutoff, an empty query followed by insertion, a repeat query at the original snapshot, and independent disjoint range readers/writers. The nonmatching update should be allowed by the new design; relevant changes must still abort or provide complete historical results. Prototype range-aware dependency stamps/latches with retained removal information before considering historical index versions. Current-posting indexes must never silently omit a historical candidate after a key is removed or moved.',
'',
'Keep transaction atomicity, complete queries including empty predicates, snapshot visibility and safe reclamation as hard regression/proof requirements. Do not simply loosen validation, increase retry limits, suppress failures, or remove the terminal empty maintenance query to improve scores. Repeat the exact controlled matrix against the preserved baseline, then separate alias reordering from index contention with a smaller identity-dispatch control. If the targeted prototype works, run longer realistic-cadence and population/turnover tests, capacity searches, and the existing service/worker-failure checks before a deployment or architectural promotion decision.',
]
(base/'controlled-findings.md').write_text('\n'.join(lines)+'\n')
print(base/'controlled-findings.md')
