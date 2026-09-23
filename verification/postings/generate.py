#!/usr/bin/env python3
"""Restricted source adapter for native posting preparation/rollback/removal."""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"
CONTRACTS = Path(__file__).with_name("contracts.rs")
OUTPUT = Path(__file__).with_name("postings.verus.rs")
_spec = importlib.util.spec_from_file_location("predicate_adapter", ROOT / "verification/predicate/generate.py")
adapter = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(adapter)
tokenize, replace, loop, show_tokens = adapter.tokenize, adapter.replace, adapter.loop, adapter.show_tokens
SIGNATURES = {
    "prepare_index_destinations": "fn prepare_index_destinations(&self, changes: &[IndexChange]) -> Result<Vec<usize>, Error>",
    "rollback_index_destinations": "fn rollback_index_destinations(&self, changes: &[IndexChange], inserted: &[usize],) -> Result<(), Error>",
    "remove_index_sources": "fn remove_index_sources(&self, changes: &[IndexChange]) -> Result<(), Error>",
}


def body(source: str, name: str) -> list[str]:
    # Reuse the same strict body lexer and signature check, with this campaign's
    # selected signatures. The imported module is local to this process.
    adapter.SIGNATURES.update(SIGNATURES)
    result = adapter.body(source, name)
    if any(t in result for t in ("proof", "ghost", "tracked", "assume", "admit", "external_body")):
        raise ValueError("proof bypass in native posting operation")
    return result


def optional_replace(tokens: list[str], old: str, new: str) -> list[str]:
    needle = tokenize(old)
    count = sum(tokens[i:i+len(needle)] == needle for i in range(len(tokens)))
    if count > 1:
        raise ValueError("duplicated native operation: " + old)
    return replace(tokens, old, new, count)


def render(source: str) -> str:
    # Production excludes this exact cfg(test) hook; stripping any other test
    # code would require a reviewed adapter change.
    hook = '''#[cfg(test)]
                INDEX_DESTINATION_HOOK.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().as_mut() {
                        hook(inserted.len());
                    }
                });'''
    if source.count(hook) != 1:
        raise ValueError("destination test-hook boundary changed")
    production = source.replace(hook, "", 1)
    prepare = body(production, "prepare_index_destinations")
    rollback = body(production, "rollback_index_destinations")
    remove = body(production, "remove_index_sources")
    prepare = replace(prepare, "let index = &self.indexes[change.binding].index;", "let index = change.binding;", 1)
    prepare = replace(prepare, "index.transactional_insert(", "driver.transactional_insert(index,", 1)
    prepare = optional_replace(prepare, "self.rollback_index_destinations(changes, &inserted)", "rollback_index_destinations(driver, changes, &inserted)")
    prepare = optional_replace(prepare, "err.into()", "err")
    prepare = replace(prepare, 'Error::Index(format!("destination insertion failed ({err}); rollback failed ({undo}); table poisoned"))', "Error::Index", 1)
    rollback = replace(rollback, "self.indexes[previous.binding].index.transactional_remove(", "driver.transactional_remove(previous.binding,", 1)
    rollback = optional_replace(rollback, "undo.to_string()", "undo")
    rollback = replace(rollback, 'Error::Index(format!("destination rollback failed ({undo}); table poisoned"))', "Error::Index", 1)
    remove = optional_replace(remove, "self.indexes[change.binding].index.transactional_remove(", "driver.transactional_remove(change.binding,")
    remove = replace(remove, 'Error::Index(format!("source removal failed ({err}); table poisoned"))', "Error::Index", 1)
    for operation in (rollback, remove):
        replaced = optional_replace(operation, "self.poison_indexes()", "driver.poison_indexes()")
        operation[:] = replaced

    rollback = loop(rollback, "for idx in inserted.iter().rev()", """
        let mut ri = inserted.len();
        proof { assert(recorded(changes@, inserted@.subrange(ri as int, inserted.len() as int)) =~= Set::<Posting>::empty()); }
        while ri > 0
            invariant ri <= inserted.len(), initial == old(driver).state(),
                stable(initial, driver.state()), valid(initial, changes@), valid(driver.state(), changes@),
                valid_indices(changes@, inserted@),
                removed_only(initial.postings, driver.state().postings, recorded(changes@, inserted@)),
                rollback_error.is_none() ==> driver.state().postings == initial.postings.difference(
                    recorded(changes@, inserted@.subrange(ri as int, inserted.len() as int))),
            decreases ri,
        {
            ri = ri - 1;
            let idx = &inserted[ri];
            proof {
                recorded_suffix(changes@, inserted@, ri as int);
                recorded_member(changes@, inserted@, ri as int);
                assert(recorded(changes@, inserted@).contains(posting(changes@[*idx as int], changes@[*idx as int].after.unwrap())));
            }
    """, """
        }
        proof { assert(inserted@.subrange(0, inserted.len() as int) == inserted@); }
    """)
    prepare = loop(prepare, "for (change_idx, change) in changes.iter().enumerate()", """
        let mut change_idx: usize = 0;
        proof { assert(recorded(changes@, inserted@) =~= Set::<Posting>::empty()); }
        while change_idx < changes.len()
            invariant change_idx <= changes.len(), initial == old(driver).state(),
                stable(initial, driver.state()), valid(initial, changes@), valid(driver.state(), changes@),
                unique_destinations(changes@),
                initial.postings.disjoint(destinations(changes@, changes.len() as int)),
                driver.state().postings == initial.postings.union(destinations(changes@, change_idx as int)),
                prefix_record(changes@, inserted@, change_idx as int),
            decreases changes.len() - change_idx,
        {
            let change = &changes[change_idx];
            let ghost previous_indices = inserted@;
            proof {
                destination_prefix_subset(changes@, change_idx as int, changes.len() as int);
                assert(initial.postings.disjoint(destinations(changes@, change_idx as int)));
                if change.after.is_some() {
                    destination_member(changes@, change_idx as int, changes.len() as int);
                    destination_new(changes@, change_idx as int);
                }
            }
    """, """
            proof {
                if change.after.is_some() { prefix_record_push(changes@, previous_indices, change_idx as int); }
                else { prefix_record_skip(changes@, previous_indices, change_idx as int); }
                reveal_with_fuel(destinations, 2);
            }
            change_idx = change_idx + 1;
        }
    """)
    rollback_call = tokenize("if let Err(undo) = rollback_index_destinations(driver, changes, &inserted)")
    for pos in range(len(prepare)):
        if prepare[pos:pos+len(rollback_call)] == rollback_call:
            prepare[pos:pos] = ["proof { assert(driver.state().postings == initial.postings.union(recorded(changes@, inserted@))); }"]
            break
    # The cleanup lemma checks, rather than assumes, that the actual rollback
    # result removes only the recorded additions and restores the original set.
    result_tokens = tokenize("return Err")
    for pos in reversed(range(len(prepare))):
        if prepare[pos:pos+len(result_tokens)] == result_tokens:
            prepare[pos:pos] = ["proof { restoration_frame(initial.postings, recorded(changes@, inserted@), driver.state().postings); }"]
    # Insert only a proof fact about the preceding vector contents. Mutating
    # the native push expression does not change the claimed record update.
    prepare = optional_replace(prepare, "inserted.push(change_idx);", "inserted.push(change_idx);")
    # A proof annotation must not pass through the restricted Rust tokenizer.
    push = tokenize("inserted.push(change_idx);")
    for pos in range(len(prepare)):
        if prepare[pos:pos+len(push)] == push:
            prepare[pos:pos] = ["proof { recorded_push(changes@, previous_indices, change_idx); }"]
            break
    remove = loop(remove, "for change in changes", """
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                stable(initial, driver.state()), valid(initial, changes@), valid(driver.state(), changes@),
                driver.state().postings == initial.postings.difference(sources(changes@, ci as int)),
                removed_only(initial.postings, driver.state().postings, sources(changes@, changes.len() as int)),
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
            proof { if change.before.is_some() { source_member(changes@, ci as int, changes.len() as int); } }
    """, """
            proof { reveal_with_fuel(sources, 2); }
            ci = ci + 1;
        }
    """)
    for operation in (prepare, rollback, remove):
        operation.insert(0, "let ghost initial = driver.state();")
        if "self" in operation:
            raise ValueError("unadapted native posting receiver")
    result = CONTRACTS.read_text()
    for name, operation in (("PREPARE", prepare), ("ROLLBACK", rollback), ("REMOVE", remove)):
        result = result.replace("/* " + name + "_BODY */", show_tokens(operation))
    return "// Generated from native posting preparation/rollback/removal. See generate.py.\n" + result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generated = render(SOURCE.read_text())
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != generated:
            raise SystemExit("stale posting adapter: run verification/postings/generate.py")
    else:
        OUTPUT.write_text(generated)


if __name__ == "__main__":
    main()
