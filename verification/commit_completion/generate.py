#!/usr/bin/env python3
"""Join native row/index publication with native deregistration and stamping."""
from pathlib import Path
import argparse
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "completion.rs"
OUTPUT = HERE / "completion.verus.rs"
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"


def component(name):
    spec = importlib.util.spec_from_file_location("completion_" + name, ROOT / "verification" / name / "generate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def finish_body(source):
    adapter = component("lookup")
    body = adapter.method(source, "finish_transaction", "fn finish_transaction(&self,tx:&mut OccTransaction<T>)->Result<(),Error>")
    for hook in ("TRANSACTION_FINISHING_HOOK", "TRANSACTION_FINISHED_HOOK"):
        body = adapter.replace(body, "#[cfg(test)] " + hook + ".with(|hook| { if let Some(hook) = hook.borrow_mut().take() { hook(); } });", "", 1)
    body = adapter.replace(body, "self.shm.end_transaction(registration)?;", """
        let ended = lifecycle::end_transaction(driver, registration);
        driver.release_lifecycle();
        ended?;
    """, 1)
    if set(body).intersection({"self", "unsafe", "assume", "admit", "external_body", "#", "proof", "ghost", "tracked"}):
        raise ValueError("unsupported finish_transaction operation")
    return adapter.show(body)


def check_completion_order(source):
    # The whole driver adapter validates wrappers, policy selection and supported
    # syntax. This additional exact suffix check binds the joined finish/stamp
    # ordering, including poison on either failed operation and release afterward.
    adapter = component("concurrent")
    adapter.render(source)
    body = adapter.method_body(source, "    fn commit_with_record_impl<", "\n    fn prepare_before_publish<")
    suffix = adapter.tokenize("""
        let finish = match self.finish_transaction(tx) {
            Ok(()) => self.publish_index_stamps(&index_changes),
            Err(err) => Err(err),
        };
        if let Err(err) = finish {
            self.poison_indexes();
            return Err(err.into());
        }
        drop(locks);
        drop(index_locks);
        Ok(commit_record)
    """)
    if body[-len(suffix):] != suffix:
        raise ValueError("native completion/poison/release suffix changed")


def check_join_order(template):
    # State postconditions alone cannot distinguish schedules in disjoint
    # projections. Bind this explicit composition to native operation order too.
    calls = ("native_ordinary_data_segment(storage,plan,ordinary_plan)",
             "finish_transaction(&mut publisher.lifecycle,token)",
             "predicate::publish_index_stamps::<scenario::Bridge<L,I>,C>(publisher,&changes)")
    positions = []
    compact = "".join(template.split())
    for call in calls:
        call = "".join(call.split())
        if compact.count(call) != 1:
            raise ValueError("missing or duplicate completion join operation")
        positions.append(compact.index(call))
    if positions != sorted(positions):
        raise ValueError("completion join order differs from native publication/finish/stamp order")


def render(template=None):
    # All embedded modules are generated from the same on-disk source. Synthetic
    # source probes use finish_body/check_completion_order directly, avoiding a
    # mixed-source generated crate.
    source = SOURCE.read_text()
    check_completion_order(source)
    data = component("commit_data")
    scenario = component("lifecycle_scenario")
    shared = scenario.render()
    for name in ("lifecycle", "predicate", "capture"):
        shared = shared.replace("mod " + name + " {", "pub mod " + name + " {", 1)
    result = TEMPLATE.read_text() if template is None else template
    check_join_order(result)
    marker = "/* NATIVE_FINISH */"
    if result.count(marker) != 1:
        raise ValueError("missing or duplicate native finish marker")
    result = result.replace(marker, finish_body(source))
    return ("// Generated source-bound one-write publication/completion join.\n"
            + data.render() + "\npub mod scenario {\n" + shared + "\n}\n" + result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != expected:
            raise SystemExit("stale commit completion join")
    else:
        OUTPUT.write_text(expected)


if __name__ == "__main__":
    main()
