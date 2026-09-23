#!/usr/bin/env python3
"""Cut native lifecycle methods at their actual first mutex acquisition."""
from pathlib import Path
import argparse
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SOURCE = ROOT / "aerostore_core/src/procarray.rs"
TEMPLATE = HERE / "contracts.rs"
OUTPUT = HERE / "interference.verus.rs"
spec = importlib.util.spec_from_file_location("lifecycle_acquired_adapter", ROOT / "verification/lifecycle/generate.py")
lifecycle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lifecycle)
SELECTED = ["begin_transaction", "end_transaction", "create_transaction_snapshot", "snapshot_locked"]


def acquired_function(generated, name):
    start = generated.index("pub fn " + name + "<")
    end = generated.find("\npub ", start + 1)
    if end < 0:
        raise ValueError("missing function boundary: " + name)
    function = generated[start:end]
    function = function.replace("pub fn " + name + "<", "pub fn " + name + "_acquired<", 1)
    function = function.replace("D: LifecyclePrimitives", "D: LockedPrimitives", 1)
    if name != "snapshot_locked":
        needle = "!old(driver).state().lifecycle_held"
        if function.count(needle) != 1:
            raise ValueError("changed lifecycle lock precondition")
        function = function.replace(needle, "old(driver).state().lifecycle_held", 1)
        lock = "driver . lock_lifecycle ( ) ;"
        if function.count(lock) != 1:
            raise ValueError("missing or repeated first native acquisition")
        function = function.replace(lock, "proof { assert(driver.state().lifecycle_held); }", 1)
    function = function.replace("snapshot_locked ( driver )", "snapshot_locked_acquired ( driver )")
    function = function.replace("ensures ", "ensures final(driver).identity() == old(driver).identity(), ", 1)
    if name == "begin_transaction":
        function = function.replace("ensures ", "ensures final(driver).state().sampled_clock == old(driver).state().sampled_clock, result.is_ok() ==> final(driver).state().clock == result.unwrap().txid + 1, ", 1)
        function = function.replace("well_formed(reserved),", "well_formed(reserved), reserved.sampled_clock == initial.sampled_clock, reserved.clock == txid + 1,")
    function = function.replace("initial == old(driver).state(),", "initial == old(driver).state(), driver.identity() == old(driver).identity(),")
    return function


def render(source=None, template=None):
    source = SOURCE.read_text() if source is None else source
    # Reject a missing, delayed, or duplicated acquisition before cutting it.
    for name in SELECTED:
        if name == "snapshot_locked":
            continue
        checked = source
        if name == "create_transaction_snapshot":
            hook = '''#[cfg(test)]
        SNAPSHOT_ACQUIRING_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });'''
            if checked.count(hook) != 1:
                raise ValueError("snapshot acquisition test-hook changed")
            checked = checked.replace(hook, "", 1)
        if name == "begin_transaction":
            hook_start = checked.index("        #[cfg(test)]", checked.index("    pub fn begin_transaction("))
            hook_end = checked.index("\n\n        for ", hook_start)
            checked = checked[:hook_start] + checked[hook_end:]
        tokens = lifecycle.method(checked, name)
        prefix = lifecycle.tokenize("let _lifecycle = self.lifecycle.lock();")
        if tokens[:len(prefix)] != prefix or sum(tokens[i:i+len(prefix)] == prefix for i in range(len(tokens))) != 1:
            raise ValueError("native acquisition is not first and unique: " + name)
    generated = lifecycle.render(source)
    if source == SOURCE.read_text() and lifecycle.OUTPUT.read_text() != generated:
        raise ValueError("stale lifecycle component")
    result = TEMPLATE.read_text() if template is None else template
    for marker, text in [("LIFECYCLE_MODULE", generated),
                         ("ACQUIRED_FUNCTIONS", "\n".join(acquired_function(generated, n) for n in SELECTED))]:
        needle = "/* " + marker + " */"
        if result.count(needle) != 1:
            raise ValueError("missing or duplicate marker: " + marker)
        result = result.replace(needle, text)
    return "// Generated source-bound lifecycle acquisition-interference composition.\n" + result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    result = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != result:
            raise SystemExit("stale acquisition-interference source")
    else:
        OUTPUT.write_text(result)


if __name__ == "__main__":
    main()
