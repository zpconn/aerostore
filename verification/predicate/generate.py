#!/usr/bin/env python3
"""Restricted source adapter for three native predicate algorithms.

Every executable statement is retained except the documented collection,
registry, key, and atomic projections. Native for loops become indexed loops;
the fixed [before, after].flatten() iteration is unrolled in that exact order.
"""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"
HELPER = ROOT / "aerostore_verified/src/lib.rs"
CONTRACTS = Path(__file__).with_name("contracts.rs")
OUTPUT = Path(__file__).with_name("predicate.verus.rs")
SIGNATURES = {
    "index_lock_keys": "fn index_lock_keys(&self, tx: &OccTransaction<T>, changes: &[IndexChange],) -> Result<Vec<(usize, usize)>, Error>",
    "index_read_conflict": "fn index_read_conflict(&self, tx: &OccTransaction<T>) -> Result<bool, Error>",
    "publish_index_stamps": "fn publish_index_stamps(&self, changes: &[IndexChange]) -> Result<(), Error>",
}
_spec = importlib.util.spec_from_file_location("commit_adapter", ROOT / "verification/concurrent/generate.py")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
_module.TOKEN = re.compile(_module.TOKEN.pattern.replace("|==|!=|", "|==|!=|<=|>=|"))
tokenize, replace, balanced_end, show_tokens = _module.tokenize, _module.replace, _module.balanced_end, _module.show_tokens


def body(source: str, name: str) -> list[str]:
    marker = "    fn " + name + "("
    if source.count(marker) != 1:
        raise ValueError("native method must occur exactly once: " + name)
    selected = source[source.index(marker):]
    # Lex only through the next method/comment boundary; find the selected
    # brace closure before lexing arbitrary unrelated production Rust.
    selected = selected[:selected.index("\n    fn ", 1)] if "\n    fn " in selected[1:] else selected
    opening = selected.index("{")
    depth = 1
    end = opening + 1
    while depth:
        if end >= len(selected):
            raise ValueError("unclosed selected method")
        if selected.startswith("//", end):
            end = selected.index("\n", end)
            continue
        if selected[end] == "{": depth += 1
        elif selected[end] == "}": depth -= 1
        end += 1
    tokens = tokenize(selected[:end])
    opening = tokens.index("{")
    if tokens[:opening] != tokenize(SIGNATURES[name]):
        raise ValueError("native signature changed: " + name)
    if balanced_end(tokens, opening) != len(tokens):
        raise ValueError("unexpected selected method delimiter")
    result = tokens[opening + 1:-1]
    if any(t in result for t in ("#", "unsafe", "assume", "admit", "external_body", "proof", "invariant", "decreases")):
        raise ValueError("unsupported native syntax or verification bypass")
    return result


def loop(tokens: list[str], header: str, prefix: str, suffix: str) -> list[str]:
    needle = tokenize(header + " {")
    matches = [i for i in range(len(tokens)) if tokens[i:i+len(needle)] == needle]
    if len(matches) != 1:
        raise ValueError("expected one native loop: " + header)
    start = matches[0]
    opening = start + len(needle) - 1
    end = balanced_end(tokens, opening)
    # Proof annotations have syntax outside the native tokenizer. Preserve as
    # an opaque generated token, never as caller-supplied source text.
    return tokens[:start] + [prefix] + tokens[opening+1:end-1] + [suffix] + tokens[end:]


def unroll_keys(tokens: list[str]) -> list[str]:
    header = "for key in [change.before.as_ref(), change.after.as_ref()].into_iter().flatten() {"
    needle = tokenize(header)
    matches = [i for i in range(len(tokens)) if tokens[i:i+len(needle)] == needle]
    if len(matches) != 1:
        raise ValueError("unsupported native two-key iteration")
    start = matches[0]
    opening = start + len(needle) - 1
    end = balanced_end(tokens, opening)
    inner = tokens[opening+1:end-1]
    lowered = (tokenize("if let Some(key) = &change.before {") + inner + tokenize("}")
        + tokenize("if let Some(key) = &change.after {") + inner + tokenize("}"))
    return tokens[:start] + lowered + tokens[end:]


def render(source: str, helper: str | None = None) -> str:
    lock, conflict, publish = [body(source, n) for n in ("index_lock_keys", "index_read_conflict", "publish_index_stamps")]
    registry = "self.indexes.iter().position(|bound| bound.index.header_offset() == read.index_offset).ok_or(Error::IndexBindingsIncomplete)?"
    lock = replace(lock, registry, "driver.find_binding(read.index_offset)?", 1)
    conflict = replace(conflict, registry.replace("position", "find"), "driver.find_binding(read.index_offset)?", 1)
    lock = replace(lock, "let mut keys = BTreeSet::new();", "let mut keys = C::new();", 1)
    lock = replace(lock, "let index = &self.indexes[change.binding].index;", "let index = change.binding;", 1)
    lock = replace(lock, "index.transactional_key_bucket(key)?", "driver.transactional_key_bucket(index, key)?", 1)
    lock = replace(lock, "keys.into_iter().collect()", "keys.into_vec()", 1)
    conflict = replace(conflict, "bound.index.transactional_stamp(read.bucket)?", "driver.transactional_stamp(bound, read.bucket)?", 1)
    conflict = replace(conflict, "aerostore_verified::stamp_precedes_snapshot", "stamp_precedes_snapshot", 1)
    publish = replace(publish, "self.shm.global_txid().fetch_add(1, Ordering::AcqRel)", "driver.reserve_stamp()", 1)
    publish = replace(publish, "BTreeSet::new()", "C::new()", 1)
    publish = replace(publish, "self.indexes[change.binding].index.transactional_key_bucket(key)?", "driver.transactional_key_bucket(change.binding, key)?", 1)
    publish = replace(publish, "self.indexes[binding].index.transactional_publish_stamp(", "driver.transactional_publish_stamp(binding,", 1)

    lock = loop(lock, "for read in &tx.index_reads", """
        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, ri as int),
                all_reads_bound(driver.state(), tx.index_reads@, ri as int),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
    """, """
            proof { reveal_with_fuel(read_keys, 2); }
            ri = ri + 1;
        }
    """)
    lock = unroll_keys(lock)
    lock = loop(lock, "for change in changes", """
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), changes_valid(driver.state(), changes@),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)
                    .union(change_keys(driver.state(), changes@, ci as int)),
                all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
    """, """
            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
    """)
    conflict = loop(conflict, "for read in &tx.index_reads", """
        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(), !tx.index_conflict,
                reads_good(driver.state(), tx.index_reads@, tx.txid, ri as int),
                read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
            proof { read_key_member(driver.state(), tx.index_reads@, ri as int, tx.index_reads.len() as int); }
    """, """
            ri = ri + 1;
        }
    """)
    publish = ["proof { stamp_unchanged(initial.stamps, initial.clock); }", *publish]
    publish = unroll_keys(publish)
    publish = loop(publish, "for change in changes", """
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state() == (State { clock: (initial.clock + 1) as u64, ..initial }),
                initial.deregistered, initial.clock < u64::MAX, stamp == initial.clock,
                touched.contents() == change_keys(initial, changes@, ci as int),
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                changes.len() > 0,
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
    """, """
            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
    """)
    publish = loop(publish, "for (binding, bucket) in touched", """
        proof { stamp_unchanged(initial.stamps, stamp); }
        let ordered = touched.into_vec();
        let mut pi: usize = 0;
        proof { assert(ordered@.take(0).to_set() =~= Set::<Pair>::empty()); }
        while pi < ordered.len()
            invariant pi <= ordered.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state().binding_count == initial.binding_count,
                canonical(ordered@, change_keys(initial, changes@, changes.len() as int)),
                driver.state().bindings == initial.bindings, driver.state().held == initial.held,
                driver.state().key_buckets == initial.key_buckets,
                driver.state().deregistered == initial.deregistered,
                driver.state().clock == initial.clock + 1,
                initial.deregistered, initial.clock < u64::MAX, stamp == initial.clock,
                changes.len() > 0,
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                stamp_relation(initial.stamps, driver.state().stamps, ordered@.take(pi as int).to_set(), stamp),
                forall|p: Pair| !change_keys(initial, changes@, changes.len() as int).contains(p) ==>
                    driver.state().stamps.contains_key(p) == initial.stamps.contains_key(p)
                    && (initial.stamps.contains_key(p) ==> driver.state().stamps[p] == initial.stamps[p]),
            decreases ordered.len() - pi,
        {
            let (binding, bucket) = ordered[pi];
            let ghost previous_stamps = driver.state().stamps;
            proof {
                assert(ordered@.contains((binding, bucket)));
                changed_binding_in_range(initial, changes@, changes.len() as int, (binding, bucket));
            }
    """, """
            proof {
                stamp_update(initial.stamps, previous_stamps, ordered@.take(pi as int).to_set(), (binding, bucket), stamp);
                assert(ordered@.take(pi as int + 1).to_set() =~= ordered@.take(pi as int).to_set().insert((binding, bucket)));
            }
            pi = pi + 1;
        }
    """)
    # Initial state is specification-only and cannot affect executable code.
    publish.insert(0, "let ghost initial = driver.state();")
    for operation in (lock, conflict, publish):
        if "self" in operation:
            raise ValueError("unadapted native receiver")
    helper = HELPER.read_text() if helper is None else helper
    match = re.search(r"pub fn stamp_precedes_snapshot\(stamp: u64, transaction_id: u64\) -> bool\s*\{([^}]+)\}", helper)
    if not match:
        raise ValueError("unsupported native scalar stamp helper")
    scalar = show_tokens(tokenize(match[1]))
    if any(word in tokenize(match[1]) for word in ("assume", "admit", "unsafe", "external_body", "#")):
        raise ValueError("unsupported stamp helper")
    result = CONTRACTS.read_text()
    result = result.replace("/* NATIVE_LOCK_BODY */", show_tokens(lock))
    result = result.replace("/* NATIVE_CONFLICT_BODY */", show_tokens(conflict))
    result = result.replace("/* NATIVE_PUBLISH_BODY */", show_tokens(publish))
    return ("// Generated from native indexed transaction operations; see generate.py.\n" + result
        + "\nverus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (result: bool)\n"
        + "ensures result == (stamp < transaction_id),\n{\n" + scalar + "\n} }\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render(SOURCE.read_text())
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != rendered:
            raise SystemExit("stale predicate adapter: run verification/predicate/generate.py")
    else:
        OUTPUT.write_text(rendered)


if __name__ == "__main__":
    main()
