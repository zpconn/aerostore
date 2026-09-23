#!/usr/bin/env python3
"""Restricted source adapter for unlink_node's cached and fallback detach loops.

This is a reviewed lowering, not general Rust extraction. Offset reads and CAS
are contracted together; marking/counters before the selected loops are checked
but not proved. The native branch conditions and retry/retire order are retained.
"""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("commit_adapter", ROOT / "verification/concurrent/generate.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)
SOURCE = ROOT / "aerostore_core/src/shm_skiplist.rs"
CONTRACTS = Path(__file__).with_name("contracts.rs")
OUTPUT = Path(__file__).with_name("detach.verus.rs")

PREFIX = '''
    let node = self.node_ref(node_offset).ok_or(ShmSkipListError::InvalidNode(node_offset))?;
    let mut flags = node.flags.load(AtomicOrdering::Acquire);
    let mut newly_marked = false;
    loop {
        if flags & NODE_FLAG_MARKED != 0 { break; }
        match node.flags.compare_exchange(flags, flags | NODE_FLAG_MARKED,
            AtomicOrdering::AcqRel, AtomicOrdering::Acquire,) {
            Ok(_) => { newly_marked = true; break; }
            Err(observed) => flags = observed,
        }
    }
    if newly_marked { self.mark_tombstone_seen(); self.decrement_distinct_key_count(); }
    let height = node.height as usize;
    for level in (0..height).rev() {
        self.lane_ref(node_offset, node, level)?.marked.store(1, AtomicOrdering::Release);
    }
'''


def read_for(tokens, start):
    prefix = adapter.tokenize("for level in (0..")
    if tokens[start:start+len(prefix)] != prefix:
        raise ValueError("expected descending native lane scan")
    opening = tokens.index("{", start)
    header = tokens[start:opening]
    end = adapter.balanced_end(tokens, opening)
    # A numeric upper bound is allowed so an incomplete-lane mutant reaches the
    # proof. Unknown loop directions/ranges fail closed instead of being hidden.
    bound = header[len(prefix)]
    if header != adapter.tokenize(f"for level in (0..{bound}).rev()"):
        raise ValueError("unsupported lane iteration")
    if bound != "height" and not bound.isdecimal():
        raise ValueError("unsupported lane upper bound")
    return bound, tokens[opening+1:end-1], end


def lower_scan(body, cached):
    body = adapter.replace(body, "succs[level] != node_offset", "!engine.window_contains(level)", 1)
    pre = '''let pred_lane = self.lane_ref_by_offset(preds[level], level)?;
        let next = self.node_next_offset(node_offset, level)?;'''
    body = adapter.replace(body, pre, "", 1)
    cas = '''pred_lane.next.compare_exchange(node_offset, next,
        AtomicOrdering::AcqRel, AtomicOrdering::Acquire,)'''
    if cached:
        body = adapter.replace(body, cas + ".is_err()", "!engine.detach_lane(level)?", 1)
        # Exiting the native fast scan returns its current flag to the caller.
        body = adapter.replace(body, "break;", "return Ok(detached_all);", 2)
    else:
        body = adapter.replace(body, cas, "engine.detach_lane(level)?", 1)
    if any(token in body for token in ("unsafe", "assume", "admit", "external_body", "#")):
        raise ValueError("unsupported proof bypass")
    return adapter.show_tokens(body)


def loop_code(bound, body, cached):
    covers = "" if cached else "engine.view().attached.subset_of(engine.view().window),"
    return '''let mut detached_all = true;
    let mut level = ''' + bound + ''';
    while level > 0
        invariant
            level <= height,
            valid(engine.view()), height == engine.view().height,
            frame(old(engine).view(), engine.view()),
            ''' + covers + '''
            detached_all ==> forall|lane: usize| level <= lane < height ==>
                !engine.view().attached.contains(lane),
        decreases level,
    {
        level -= 1;
''' + body + '''
    }
    Ok(detached_all)'''


def render(source):
    if not re.search(r"const MAX_HEIGHT: usize = 32;", source):
        raise ValueError("review the native height bound")
    start = source.index("    fn unlink_node(")
    opening = source.index("{", start)
    signature = adapter.tokenize(source[start:opening])
    if signature != adapter.tokenize('''fn unlink_node(
        &self, key: &K, node_offset: u32,
        preds: &mut [u32; MAX_HEIGHT], succs: &mut [u32; MAX_HEIGHT],
    ) -> Result<(), ShmSkipListError>'''):
        raise ValueError("native unlink signature changed; review the primitive boundary")
    body = adapter.method_body(source, "    fn unlink_node(", "\n    fn decrement_distinct_key_count(")
    prefix = adapter.tokenize(PREFIX)
    if body[:len(prefix)] != prefix:
        raise ValueError("native marking/height boundary changed; review primitive assumptions")
    body = body[len(prefix):]
    initial = adapter.tokenize("let mut detached_all = true;")
    if body[:len(initial)] != initial:
        raise ValueError("expected native detach flag initialization")
    bound, cached, end = read_for(body, len(initial))
    tail = body[end:]
    # Preserve outer retry condition and final break, including semantic mutants.
    while_start = adapter.tokenize("while")
    if tail[:1] != while_start:
        raise ValueError("expected fallback retry loop")
    opening = tail.index("{")
    end = adapter.balanced_end(tail, opening)
    inner = tail[opening+1:end-1]
    find = adapter.tokenize("let _ = self.find(key, preds, succs)?; detached_all = true;")
    if inner[:len(find)] != find:
        raise ValueError("fallback must refresh the window before its scan")
    scan_bound, scan, scan_end = read_for(inner, len(find))
    remainder = inner[scan_end:]
    suffix = tail[end:]
    if suffix != adapter.tokenize("self.retire_node(node_offset); Ok(())"):
        raise ValueError("native retirement boundary changed")
    remainder_code = adapter.show_tokens(remainder)
    if any(token in tail for token in ("unsafe", "assume", "admit", "external_body", "#")):
        raise ValueError("unsupported proof bypass")
    condition = adapter.show_tokens(tail[1:opening])
    driver = '''let mut detached_all = cached_detach(engine, height)?;
    while ''' + condition + '''
        invariant valid(engine.view()), height == engine.view().height,
            frame(old(engine).view(), engine.view()),
            detached_all ==> detached(engine.view()),
        ensures detached(engine.view()),
            valid(engine.view()), height == engine.view().height,
            frame(old(engine).view(), engine.view()),
    {
        engine.refresh_window()?;
        detached_all = refreshed_detach(engine, height)?;
''' + remainder_code + '''
    }
    engine.retire_node();
    Ok(())'''
    text = CONTRACTS.read_text()
    for name, code in (
        ("CACHED_BODY", loop_code(bound, lower_scan(cached, True), True)),
        ("REFRESHED_BODY", loop_code(scan_bound, lower_scan(scan, False), False)),
        ("DRIVER_BODY", driver),
    ):
        text = text.replace("/* " + name + " */", code)
    return "// Generated from native unlink_node; see generate.py and README.md.\n" + text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = render(SOURCE.read_text())
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != result:
            raise SystemExit("stale detach adapter: run verification/skiplist_detach/generate.py")
    else:
        OUTPUT.write_text(result)


if __name__ == "__main__":
    main()
