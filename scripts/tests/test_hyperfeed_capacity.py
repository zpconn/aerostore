#!/usr/bin/env python3
"""Independent capacity-policy regressions; no database or benchmark processes."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import assess_hyperfeed_capacity as capacity
import test_hyperfeed_qualification as fixtures
from test_arena_backing import metadata as arena_metadata, with_backing

POLICY = {"minimum_duration_seconds": 11, "warmup_seconds": 1}


def accounting(trial):
    config, run = trial['config'], trial['report']['runs'][0]
    seconds, rate, start = config['seconds'], config['arrival_rate'], run['admission_started_ns']
    windows = [dict(start_offset_ns=i*10**9, end_offset_ns=(i+1)*10**9,
        observed_end_offset_ns=(i+1)*10**9, fully_observed=True, planned_arrivals=rate,
        arrivals=rate, completions=rate, cumulative_arrivals=(i+1)*rate,
        cumulative_completions=(i+1)*rate, backlog_start=0, backlog_end=0,
        oldest_uncompleted_age_ns=None, arrival_cohort_completed=rate,
        arrival_cohort_unfinished=0, arrival_cohort_p99_us_including_retries=1000,
        completed_message_retries=0) for i in range(seconds)]
    maintenance = {}
    for name in ('projection', 'housekeeping'):
        selected = [x for x in run['maintenance_jobs'] if x['class'] == name]
        interval = config[name+'_interval_seconds']*10**9
        jobs = [dict(ordinal=i, scheduled_ns=x['scheduled_ns'], deadline_ns=x['scheduled_ns']+interval,
            received_ns=x['received_ns'], started_ns=x['started_ns'], finished_ns=x['finished_ns'],
            retries=x['retries'], completed=True, positive_effect=True) for i, x in enumerate(selected)]
        maintenance[name] = dict(offered=len(jobs), completed=len(jobs), positive_effect_jobs=len(jobs), jobs=jobs)
    return dict(format='capacity-accounting-v1', admission_start_ns=start,
        admission_end_ns=start+seconds*10**9, observed_until_ns=start+seconds*10**9+10**6,
        execution_completed=True, admission_fully_observed=True, window_ns=10**9,
        foreground=dict(offered=rate*seconds, completed_total=rate*seconds,
            received_in_admission=rate*seconds, in_admission_counts_final=True,
            completion_rate_in_admission=rate, remaining_at_admission_end=0,
            p99_us_including_retries=1000, in_admission_p99_us_including_retries=1000),
        windows=windows, maintenance=maintenance, completion_clock='coordinator_received_ns')


def evidence():
    item = fixtures.sweep_trial(evidence='metrics', seconds=11)
    guard = fixtures.sweep_trial(evidence='full', seconds=7)
    run = item['report']['runs'][0]
    run['capacity_accounting'] = accounting(item)
    index = dict(retired_postings=0, alloc_failures=0, gc_recycle_errors=0)
    storage = dict(arena_high_water_bytes=10000, row_fresh_allocations=1024,
        row_reuse_allocations=30, row_recycled=30, active_transactions=0, indexes=[index])
    run['retention_samples'] = [dict(elapsed_seconds=0, storage=copy.deepcopy(storage)),
        dict(elapsed_seconds=11, storage=copy.deepcopy(storage))]
    run['after_drain'] = copy.deepcopy(storage)
    run['native_audit'] = {'indexes': [{'index': 'family'}]}
    source = {'sha256': 'a'*64, 'files': {}}
    campaign = dict(format=capacity.gate.FORMAT, completed=True, passed=True, source_stable=True,
        source_before=source, source_after=copy.deepcopy(source), binary_before_sha256='b'*64,
        binary_after_sha256='b'*64, trials=[item])
    guard_campaign = copy.deepcopy(campaign); guard_campaign['trials'] = [guard]
    events = 'high 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n'
    envelope = dict(ready=True, controller_survived=True, controller_boot_before='boot',
        controller_boot_after='boot', reason='unit-completed', returncode=0,
        cleanup={'owned_processes_terminated': True}, memory_events_scope='unique-accounting-slice',
        minimum_mem_available_bytes=10*capacity.GIB, samples=2,
        final_accounting={'memory.peak': '1000000', 'memory.events': events, 'memory.events.local': events,
                          'memory.swap.current': '0', 'memory.swap.peak': '0'})
    samples = [dict(elapsed_seconds=i*12, **{'memory.current': '1000000',
        'memory.stat': 'anon 700000\nfile 300000\nshmem 100000\n', 'memory.swap.current': '0', 'memory.swap.peak': '0'},
        mem_available_bytes=10*capacity.GIB) for i in range(2)]
    return dict(trial=item, campaign=campaign, guardrail_trial=guard, guardrail_campaign=guard_campaign,
        envelope=envelope, readiness=dict(ready=True, boot_id='boot', memory_max_bytes=36*capacity.GIB,
                                         reserve_bytes=4*capacity.GIB), memory_samples=samples)


def account(data):
    return data['trial']['report']['runs'][0]['capacity_accounting']


def recalculate_windows(value):
    arrivals = completed = 0
    for window in value['windows']:
        window['backlog_start'] = arrivals-completed
        arrivals += window['arrivals']; completed += window['completions']
        window.update(cumulative_arrivals=arrivals, cumulative_completions=completed,
                      backlog_end=arrivals-completed,
                      oldest_uncompleted_age_ns=100_000_000 if arrivals>completed else None)
    value['foreground'].update(received_in_admission=completed,
        completion_rate_in_admission=completed/len(value['windows']), remaining_at_admission_end=arrivals-completed)


class CapacityTests(unittest.TestCase):
    def test_pass_is_conditional_structural_plus_short_guardrail(self):
        result = capacity.assess_trial(evidence(), POLICY)
        self.assertEqual(result['classification'], 'passed', result)
        self.assertTrue(result['conditional_capacity_passed'])
        self.assertFalse(result['correctness_coverage']['measured_run_history_verified'])
        self.assertEqual(set(result['correctness_coverage']['differences']), {'seconds'})
        self.assertFalse(result['actual_hyperfeed_replacement_qualified'])
        self.assertFalse(result['structural_assessment']['history_verified'])

    def test_drain_cannot_replace_completions_during_admission(self):
        data=evidence(); a=account(data); a['windows'][-1]['completions']-=2; recalculate_windows(a)
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'completed_policy_failure',result)
        self.assertEqual(result['accounting']['foreground_completed_including_drain'],66)
        self.assertTrue(any('in-admission' in x for x in result['reasons']))
        self.assertTrue(any('final-third' in x for x in result['reasons']))

    def test_foreground_fork_or_batch_count_inflation_rejected(self):
        for field in ('offered','completed_total'):
            data=evidence(); account(data)['foreground'][field]*=7
            with self.subTest(field=field):
                self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'invalid_evidence')

    def test_retry_inclusive_foreground_p99_is_required(self):
        data=evidence(); account(data)['foreground']['p99_us_including_retries']=51000
        data['trial']['report']['runs'][0]['workload_classes']['foreground']['p99_us_including_retries']=51000
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'completed_policy_failure',result)
        self.assertTrue(any('p99' in x for x in result['reasons']))

    def test_sampled_backlog_spike_fails_even_after_catching_up(self):
        data=evidence(); a=account(data)
        a['windows'][5]['completions']=0; a['windows'][6]['completions']=5; a['windows'][7]['completions']=13
        recalculate_windows(a)
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'completed_policy_failure',result)
        self.assertEqual(result['accounting']['completion_fraction'],1)
        self.assertTrue(any('backlog exceeds' in x for x in result['reasons']))

    def test_missing_or_nonmonotonic_windows_fail_closed(self):
        for change in (lambda a:a['windows'].pop(2), lambda a:a['windows'][2].update(backlog_end=99),
                       lambda a:a['windows'][-1].update(fully_observed=False)):
            data=evidence();change(account(data))
            self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'invalid_evidence')

    def test_maintenance_at_exact_next_tick_is_a_failure(self):
        data=evidence(); job=account(data)['maintenance']['projection']['jobs'][0]
        job['received_ns']=job['deadline_ns']
        result=capacity.accounting_assessment(account(data), data['trial']['config'], {},
            capacity.policy_with_defaults(POLICY), partial=True)
        self.assertTrue(any('next-tick' in x for x in result['reasons']))

    def test_completed_empty_jobs_do_not_supply_positive_coverage(self):
        data=evidence(); info=account(data)['maintenance']['housekeeping']
        for job in info['jobs']:job['positive_effect']=False
        info['positive_effect_jobs']=0
        # Independent sweep receipts make a falsified effect claim invalid,
        # rather than silently accepting the changed accounting counter.
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'invalid_evidence',result)

    def test_retry_exhaustion_at_higher_rate_is_operational_not_censored(self):
        data=evidence();data['trial']['config']['arrival_rate']=12
        data['trial'].update(exit_code=2,error='worker 2: message 9 after 128 retries: transaction conflict')
        data['envelope']['returncode']=1
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'operational_failure',result)
        self.assertTrue(result['operational_capacity_failure']);self.assertFalse(result['censored'])
        self.assertIn('arrival_rate',result['correctness_coverage']['differences'])

    def test_generator_oracle_and_resource_limits_are_distinct_censoring(self):
        for text,status in [('worker stopped at message_cap','censored_generator'),
                            ('oracle exceeded budget','censored_oracle')]:
            data=evidence();data['trial'].update(exit_code=2,error=text)
            self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],status)
        data=evidence();data['envelope']['final_accounting']['memory.swap.peak']='1'
        self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'censored_resources')

    def test_fatal_error_wrapper_is_not_retry_exhaustion(self):
        for text in ('message 9 after 0 retries: missing row; cleanup=Ok(())',
                     'message 9 after 128 retries: transport failed; cleanup=Ok(())'):
            data=evidence(); data['trial'].update(exit_code=2, error=text)
            self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'censored_operational')

    def test_malformed_partial_failure_cannot_create_capacity_bracket(self):
        data=evidence();data['trial'].update(exit_code=2,error='message 9 after 128 retries: transaction conflict')
        data['failure_progress']={'capacity_accounting': {'format':'wrong'}}
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'invalid_evidence')
        self.assertFalse(result['operational_capacity_failure'])

    def test_progress_stall_needs_observed_arrivals_and_maintenance_cap_is_operational(self):
        data=evidence();data['trial'].update(exit_code=2,error='no worker progress for 60 seconds')
        self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'censored_timeout')
        value=account(data);value['foreground']['offered_observed']=66
        data['failure_progress']={'capacity_accounting':value}
        self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'operational_failure')
        data['trial']['error']='maintenance job did not reach terminal query after 4096 batches'
        self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'operational_failure')

    def test_correctness_failure_is_not_overload(self):
        data=evidence();data['trial']['report']['runs'][0]['invariants']['passed']=False
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'correctness_failure')
        self.assertFalse(result['operational_capacity_failure'])

    def test_memory_growth_is_reported_without_flat_total_assumption(self):
        data=evidence();data['memory_samples'][-1]['memory.current']='2000000'
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'passed',result)
        self.assertGreater(result['resources']['trends']['current']['slope_per_second'],0)
        self.assertFalse(result['resources']['flat_memory_required'])

    def test_allocator_gc_errors_and_failed_final_audit_rejected(self):
        data=evidence();data['trial']['report']['runs'][0]['after_drain']['indexes'][0]['gc_recycle_errors']=1
        self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'operational_failure')
        data=evidence();data['trial']['report']['runs'][0].pop('native_audit')
        self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'invalid_evidence')

    def test_guardrail_must_match_source_binary_and_passing_rate(self):
        for change in (lambda d:d['guardrail_campaign'].update(binary_after_sha256='c'*64),
                       lambda d:d['guardrail_trial']['config'].update(arrival_rate=3),
                       lambda d:d['guardrail_trial']['config'].update(workers=1)):
            data=evidence();change(data)
            self.assertEqual(capacity.assess_trial(data,POLICY)['classification'],'invalid_evidence')

    def test_endpoint_needs_distinct_repeats_and_preserves_nonmonotonicity(self):
        result=capacity.assess_trial(evidence(),POLICY)
        self.assertFalse(capacity.summarize_capacity([result,result],POLICY)['configurations'][0]['capacity_lower_bound_established'])
        second=copy.deepcopy(result);second['config']['seed']+=1
        summary=capacity.summarize_capacity([result,second],POLICY)['configurations'][0]
        self.assertEqual(summary['tested_capacity_lower_bound_inputs_per_second'],6)
        failures=[]
        for seed in (11,12):
            item=copy.deepcopy(result);item['config'].update(seed=seed,arrival_rate=3)
            item.update(classification='operational_failure',conditional_capacity_passed=False,operational_capacity_failure=True)
            failures.append(item)
        summary=capacity.summarize_capacity([result,second,*failures],POLICY)['configurations'][0]
        self.assertTrue(summary['nonmonotonic_tested_points']);self.assertFalse(summary['universal_upper_bound_established'])

    def test_hash_bound_cli_retains_negative_verdict_and_refuses_overwrite(self):
        data=evidence();manifest={'format':capacity.INPUT_FORMAT,'policy':POLICY,'trials':[]}
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);entry={'id':'fixture'}
            for name,key in [('campaign','campaign'),('guardrail','guardrail_campaign'),('envelope','envelope'),
                             ('readiness','readiness'),('memory_samples','memory_samples')]:
                path=root/(name+'.json')
                text='\n'.join(json.dumps(x) for x in data[key]) if name=='memory_samples' else json.dumps(data[key])
                path.write_text(text);entry[name]={'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
            manifest['trials']=[entry]; source=root/'input.json';source.write_text(json.dumps(manifest));out=root/'result.json'
            self.assertEqual(capacity.main(['--input',str(source),'--output',str(out)]),0)
            self.assertTrue(json.loads(out.read_text())['all_trials_passed'])
            with self.assertRaises(SystemExit):capacity.main(['--input',str(source),'--output',str(out)])
            (root/'campaign.json').write_text('{}')
            with self.assertRaisesRegex(ValueError,'hash mismatch'):capacity.assess_manifest(manifest)

    def test_policy_rejects_nonfinite_bool_unknown_and_duration_screen(self):
        for policy in ({'foreground_p99_ms':float('nan')},{'required_repeats':True},{'unknown':1}):
            with self.assertRaises(ValueError):capacity.policy_with_defaults(policy)
        result=capacity.assess_trial(evidence())
        self.assertEqual(result['classification'],'censored_duration')


class MessageCapGuardrailTests(unittest.TestCase):
    def coverage(self, data, failure=False):
        return capacity.guardrail_assessment(data['trial'], data['campaign'],
            data['guardrail_trial'], data['guardrail_campaign'],
            capacity.policy_with_defaults(POLICY), failure=failure)

    def test_larger_metrics_cap_is_explicit_and_exactly_nonbinding(self):
        data=evidence();data['trial']['config']['max_messages']=1_000_000
        data['trial']['report']['config']['max_messages']=1_000_000
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'passed',result)
        proof=result['correctness_coverage']['nonbinding_message_cap_difference']
        self.assertEqual(proof['measured']['worker_counts'],[44,22,10,5])
        self.assertEqual(proof['guardrail']['worker_counts'],[28,14,6,3])
        self.assertEqual(proof['measured']['total_inputs'],81)
        self.assertEqual(proof['guardrail']['total_inputs'],51)
        self.assertEqual(result['correctness_coverage']['differences']['max_messages'],
            {'measured':1_000_000,'guardrail':100_000})
        self.assertFalse(result['correctness_coverage']['measured_run_history_verified'])

    def test_exact_cap_boundary_fits_but_average_owner_estimate_is_insufficient(self):
        data=evidence();data['trial']['config']['max_messages']=44
        self.assertTrue(self.coverage(data)['nonbinding_message_cap_difference']['passed'])
        data['trial']['config']['max_messages']=33
        # 66 foreground / two workers = 33, but identity routing assigns 44 to worker 0.
        with self.assertRaisesRegex(ValueError,'exact dispatched or timer corpus'):
            self.coverage(data)

    def test_timer_workers_are_checked_independently_of_foreground_average(self):
        data=evidence();config=data['trial']['config']
        config.update(arrival_rate=1,seconds=11,workers=32,max_messages=9)
        with self.assertRaisesRegex(ValueError,'exact dispatched or timer corpus'):
            self.coverage(data)
        config['max_messages']=10
        proof=capacity.guardrail_message_cap_exception(config,data['guardrail_trial']['config'])['measured']
        self.assertEqual(len(proof['worker_counts']),34)
        self.assertEqual(proof['worker_counts'][-2:],[10,5])

    def test_signature_affinity_uses_exact_owner_routing(self):
        data=evidence();config=data['trial']['config']
        config.update(seconds=3,dispatch='signature-affinity',affinity_ttl_ms=10000,
                      signature_pattern='mixed',max_messages=10)
        proof=capacity.guardrail_message_cap_exception(config,data['guardrail_trial']['config'])
        self.assertEqual(proof['measured']['worker_counts'],[8,10,2,1])
        config['max_messages']=9
        with self.assertRaisesRegex(ValueError,'exact dispatched or timer corpus'):
            capacity.guardrail_message_cap_exception(config,data['guardrail_trial']['config'])

    def test_both_caps_have_strict_integer_evidence_mode_bounds(self):
        for side,bad in [('trial',True),('trial',0),('trial',1.0),('trial','1000000'),
                         ('trial',1_000_001),('guardrail_trial',100_001),
                         ('guardrail_trial',27)]:
            with self.subTest(side=side,bad=bad):
                data=evidence();data['trial']['config']['max_messages']=1_000_000
                data[side]['config']['max_messages']=bad
                if side=='guardrail_trial':data[side]['report']['config']['max_messages']=bad
                with self.assertRaises(ValueError):self.coverage(data)

    def test_exception_only_allows_calibrated_metrics_to_full_direction(self):
        for side,field,value in [('trial','evidence','full'),('trial','workload','fleet'),
                                  ('guardrail_trial','evidence','metrics'),
                                  ('guardrail_trial','workload','fleet')]:
            with self.subTest(side=side,field=field):
                data=evidence();data['trial']['config']['max_messages']=1_000_000
                config=data[side]['config'];config[field]=value
                with self.assertRaises(ValueError):
                    capacity.guardrail_message_cap_exception(data['trial']['config'],data['guardrail_trial']['config'])

    def test_other_transaction_and_workload_differences_still_rejected(self):
        for field,value in [('workers',3),('families',8),('projection_batch_size',8),
                            ('maintenance_selection','prefix'),('max_backlog',2000),
                            ('pg_write_mode','direct')]:
            with self.subTest(field=field):
                data=evidence();data['trial']['config'].update(max_messages=1_000_000)
                data['trial']['config'][field]=value
                with self.assertRaisesRegex(ValueError,'beyond declared short-run differences'):
                    self.coverage(data)

    def test_failure_can_use_lower_rate_guardrail_without_excusing_binding_caps(self):
        data=evidence();data['trial']['config'].update(arrival_rate=12,max_messages=1_000_000)
        result=self.coverage(data,failure=True)
        self.assertEqual(set(result['differences']),{'seconds','arrival_rate','max_messages'})
        self.assertEqual(result['nonbinding_message_cap_difference']['measured']['worker_counts'],[88,44,10,5])
        data['trial']['config']['max_messages']=87
        with self.assertRaisesRegex(ValueError,'exact dispatched or timer corpus'):
            self.coverage(data,failure=True)

    def test_exact_companion_and_repeat_keys_still_bind_message_cap(self):
        data=evidence();config=data['trial']['config'];other=copy.deepcopy(config)
        other['max_messages']=1_000_000
        self.assertNotEqual(capacity.gate.key(config),capacity.gate.key(other))
        result=capacity.assess_trial(data,POLICY)
        second=copy.deepcopy(result);second['config'].update(max_messages=1_000_000,seed=999)
        groups=capacity.summarize_capacity([result,second],POLICY)['configurations']
        self.assertEqual(len(groups),2)
        self.assertTrue(all(not group['capacity_lower_bound_established'] for group in groups))

    def test_unchanged_caps_keep_historical_guardrail_behavior(self):
        result=self.coverage(evidence())
        self.assertEqual(set(result['differences']),{'seconds'})
        self.assertIsNone(result['nonbinding_message_cap_difference'])


class ArenaCapacityTests(unittest.TestCase):
    def test_observed_backing_and_filesystem_bind_short_guardrail(self):
        for backing in ("file", "memfd"):
            data = evidence()
            with_backing(data["trial"], backing); with_backing(data["guardrail_trial"], backing)
            self.assertTrue(capacity.assess_trial(data, POLICY)["conditional_capacity_passed"])
            data["guardrail_trial"]["report"]["runs"][0]["arena_backing_metadata"]["filesystem_magic"] = "0x794c7630"
            data["guardrail_trial"]["report"]["runs"][0]["arena_backing_metadata"]["filesystem_type"] = "other"
            self.assertEqual(capacity.assess_trial(data, POLICY)["classification"], "invalid_evidence")

    def test_repeat_groups_keep_observed_filesystems_and_backings_separate(self):
        data = evidence(); with_backing(data["trial"], "file"); with_backing(data["guardrail_trial"], "file")
        first = capacity.assess_trial(data, POLICY)
        for backing, filesystem in (("memfd", "tmpfs"), ("file", "tmpfs")):
            second = copy.deepcopy(first); second["config"].update(seed=999, arena_backing=backing)
            second["arena_backing"]["metadata"] = arena_metadata(backing, filesystem=filesystem)
            groups = capacity.summarize_capacity([first, second], POLICY)["configurations"]
            self.assertEqual(len(groups), 2)
            self.assertTrue(all(not row["capacity_lower_bound_established"] for row in groups))

    def test_instance_identity_does_not_split_repeat_groups(self):
        data = evidence(); with_backing(data["trial"], "memfd"); with_backing(data["guardrail_trial"], "memfd")
        first = capacity.assess_trial(data, POLICY)
        second = copy.deepcopy(first); second["config"]["seed"] = 999
        first["arena_backing"]["metadata"].update(owner_pid=100, inode=10, path="/one")
        second["arena_backing"]["metadata"].update(owner_pid=200, inode=20, path="/two")
        groups = capacity.summarize_capacity([first, second], POLICY)["configurations"]
        self.assertEqual(len(groups), 1)
        self.assertTrue(groups[0]["capacity_lower_bound_established"])

    def test_retry_exhaustion_requires_observed_placement_for_modern_configuration(self):
        data = evidence(); with_backing(data["trial"], "memfd"); with_backing(data["guardrail_trial"], "memfd")
        data["trial"].update(exit_code=2, error=None)
        data["trial"]["report"].update(passed=False, error="after 128 retries: transaction conflict")
        result = capacity.assess_trial(data, POLICY)
        self.assertEqual(result["classification"], "operational_failure", result)
        self.assertEqual(result["arena_backing"]["status"], "observed")
        data["trial"]["report"]["runs"] = []
        result = capacity.assess_trial(data, POLICY)
        self.assertEqual(result["classification"], "invalid_evidence", result)
        self.assertFalse(result["operational_capacity_failure"])


class ReportLevelFailureTests(unittest.TestCase):
    def failed(self, message):
        data=evidence()
        data['trial'].update(exit_code=2,error=None)
        data['trial']['report'].update(passed=False,error=message,runs=[])
        return data

    def test_report_only_generator_bound_is_censored_before_guardrail_checks(self):
        data=self.failed('calibrated per-process corpus exceeds max_messages')
        data.pop('guardrail_trial');data.pop('guardrail_campaign')
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'censored_generator',result)
        self.assertTrue(result['censored'])
        self.assertFalse(result['operational_capacity_failure'])
        self.assertIn('max_messages',result['error_text'])

    def test_report_only_retry_exhaustion_keeps_specific_operational_verdict(self):
        data=self.failed('worker 2: message 9 after 128 retries: transaction conflict; cleanup=Ok(())')
        result=capacity.assess_trial(data,POLICY)
        self.assertEqual(result['classification'],'operational_failure',result)
        self.assertEqual(result['reasons'],['retry_exhaustion'])
        self.assertTrue(result['operational_capacity_failure'])

    def test_report_only_oracle_budget_is_checker_censoring(self):
        result=capacity.assess_trial(self.failed('oracle search budget exhausted; inconclusive'),POLICY)
        self.assertEqual(result['classification'],'censored_oracle',result)
        self.assertFalse(result['operational_capacity_failure'])

    def test_report_only_fatal_wrapper_does_not_become_retry_exhaustion(self):
        message='worker 2: message 9 after 0 retries: fatal PostgreSQL connection failed; cleanup=Ok(())'
        result=capacity.assess_trial(self.failed(message),POLICY)
        self.assertEqual(result['classification'],'censored_operational',result)
        self.assertFalse(result['operational_capacity_failure'])
        self.assertIn(message,result['error_text'])


if __name__=='__main__':unittest.main()
