#!/usr/bin/env python3
"""Source-bound, conditional event refinement of the native commit driver.

This is a trusted, deliberately restricted source adapter, not Rust extraction.
It preserves the selected body modulo the explicitly enumerated abstractions.
It does not prove native implementations satisfy the abstract primitive trait.
"""
from pathlib import Path
import argparse
import re

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"
CONTRACTS = Path(__file__).with_name("contracts.rs")
OUTPUT = Path(__file__).with_name("commit.verus.rs")
TOKEN = re.compile(r'\s+|//[^\n]*|/\*|"(?:\\.|[^"\\])*"|[A-Za-z_][A-Za-z_0-9]*|[0-9]+|::|->|=>|&&|\|\||==|!=|[{}()\[\].,;:#&?!<>|=+*\-/]')


def tokenize(source: str) -> list[str]:
    result = []
    offset = 0
    while offset < len(source):
        match = TOKEN.match(source, offset)
        if not match:
            raise ValueError(f"unsupported Rust syntax at offset {offset}: {source[offset:offset+30]!r}")
        token = match[0]
        offset = match.end()
        if token == "/*":
            depth = 1
            while depth:
                nested = source.find("/*", offset)
                end = source.find("*/", offset)
                if end < 0:
                    raise ValueError("unterminated block comment")
                if nested >= 0 and nested < end:
                    depth += 1
                    offset = nested + 2
                else:
                    depth -= 1
                    offset = end + 2
        elif not token.isspace() and not token.startswith("//"):
            result.append(token)
    return result


def balanced_end(tokens: list[str], start: int) -> int:
    pairs = {"{": "}", "(": ")", "[": "]"}
    stack = []
    for position in range(start, len(tokens)):
        token = tokens[position]
        if token in pairs:
            stack.append(pairs[token])
        elif token in pairs.values():
            if not stack or stack.pop() != token:
                raise ValueError("unbalanced source delimiters")
            if not stack:
                return position + 1
    raise ValueError("unclosed source delimiters")


def method_body(source: str, marker: str, end_marker: str) -> list[str]:
    # Read only the selected body: the rest of the module has unsupported
    # lifetimes/macros that this adapter intentionally never claims to parse.
    if source.count(marker) != 1:
        raise ValueError("expected exactly one production commit_with_record_impl")
    start = source.index(marker)
    end = source.find(end_marker, start)
    if end < 0:
        raise ValueError("missing following publication function")
    tokens = tokenize(source[start:end])
    opening = tokens.index("{")
    if balanced_end(tokens, opening) != len(tokens):
        raise ValueError("extra items after selected operation")
    return tokens[opening + 1:-1]


def replace(tokens: list[str], old: str, new: str, count: int) -> list[str]:
    needle, replacement = tokenize(old), tokenize(new)
    found = 0
    result = []
    position = 0
    while position < len(tokens):
        if tokens[position:position + len(needle)] == needle:
            result.extend(replacement)
            position += len(needle)
            found += 1
        else:
            result.append(tokens[position])
            position += 1
    if found != count:
        raise ValueError(f"abstraction expected {count} occurrences, found {found}: {old}")
    return result


def show_tokens(tokens: list[str]) -> str:
    # Formatting changes only whitespace; make proof diagnostics/review usable.
    lines = []
    line = []
    indent = 0
    for token in tokens:
        if token == "}":
            if line:
                lines.append("    " * indent + " ".join(line))
                line = []
            indent = max(0, indent - 1)
        line.append(token)
        if token in {";", "{", "}"}:
            lines.append("    " * indent + " ".join(line))
            line = []
            if token == "{":
                indent += 1
    if line:
        lines.append("    " * indent + " ".join(line))
    return "\n".join(lines)


def render(source: str) -> str:
    ordinary = method_body(source, "    pub fn commit_with_record(", "\n    pub(crate) fn commit_with_record_before_publish<")
    durable = method_body(source, "    pub(crate) fn commit_with_record_before_publish<", "\n    pub(crate) fn commit_with_record_prepared<")
    prepared = method_body(source, "    pub(crate) fn commit_with_record_prepared<", "\n    fn commit_with_record_impl<")
    if ordinary != tokenize("self.commit_with_record_impl::<false, Error, _, _>(tx, |_| { Ok(|_: &OccCommitRecord<T>| Ok(())) })"):
        raise ValueError("ordinary commit wrapper changed its verified policy binding")
    if durable != tokenize("self.commit_with_record_prepared::<E, _, F>(tx, |_| Ok(before_publish))"):
        raise ValueError("write-ahead commit wrapper changed its verified policy binding")
    if prepared != tokenize("self.commit_with_record_impl::<true, E, P, F>(tx, prepare)"):
        raise ValueError("prepared commit wrapper changed its verified policy binding")
    body = method_body(source, "    fn commit_with_record_impl<", "\n    fn prepare_before_publish<")
    # cfg(test) has no production executable body. Exact hook content is
    # checked so other conditional compilation cannot silently disappear.
    body = replace(body, '''#[cfg(test)]
        INDEX_PUBLICATION_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() { hook(); }
        });''', "", 1)
    body = replace(body, "drop(locks);", "self.release_partition_locks(locks);", 3)
    body = replace(body, "drop(index_locks);", "self.release_index_locks(index_locks);", 4)
    body = replace(body, "self.shm.flush_local_recycle_caches()", "self.flush_local_recycle_caches()", 1)
    # P's native FnOnce result selects F; the abstract trait omits FnOnce and
    # therefore needs that same type argument written explicitly.
    body = replace(body, "self.prepare_before_publish(tx, &final_write_indices, prepare)",
                   "self.prepare_before_publish::<P, F>(tx, &final_write_indices, prepare)", 1)
    body = replace(body, '''Error::Index(format!(
        "row publication failed after index preparation ({err}); table poisoned"
    ))''', "Error::Index", 1)
    if any(token in body for token in ("unsafe", "assume", "admit", "external_body", "#")):
        raise ValueError("unsupported macro, attribute, or proof bypass in operation")
    if any(token == "!" and i > 0 and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", body[i - 1])
           and body[i - 1] not in {"if", "while", "return", "match"}
           for i, token in enumerate(body)):
        raise ValueError("unsupported macro in operation")
    body = ["driver" if token == "self" else token for token in body]
    rollback = method_body(source, "    fn rollback_prepared_commit(", "\n    fn prepare_commit_record(")
    prepare = method_body(source, "    fn prepare_before_publish<", "\n    fn abort_preparation(")
    abort_preparation = method_body(source, "    fn abort_preparation(", "\n    fn invoke_before_publish<")
    callback = method_body(source, "    fn invoke_before_publish<", "\n    fn rollback_prepared_commit(")
    callback = replace(callback,
        "std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| before_publish(record)))",
        "self.run_before_publish(record, before_publish)", 1)
    callback = replace(callback, "std::panic::resume_unwind(panic)", "self.resume_unwind(panic)", 1)
    prepare = replace(prepare,
        "std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| prepare(&record)))",
        "self.run_prepare(&record, prepare)", 1)
    prepare = replace(prepare, "std::panic::resume_unwind(panic)", "self.resume_unwind(panic)", 1)
    for method in [callback, rollback, prepare, abort_preparation]:
        if any(token in method for token in ("unsafe", "assume", "admit", "external_body", "#")):
            raise ValueError("unsupported syntax or proof bypass in callback cleanup")
    contracts = (CONTRACTS.read_text()
        .replace("/* NATIVE_ROLLBACK_BODY */", show_tokens(rollback))
        .replace("/* NATIVE_CALLBACK_BODY */", show_tokens(callback))
        .replace("/* NATIVE_PREPARE_BODY */", show_tokens(prepare))
        .replace("/* NATIVE_ABORT_PREPARATION_BODY */", show_tokens(abort_preparation)))
    return ("// Generated from commit_with_record. See generate.py for every abstraction.\n"
            + contracts + "\nverus! {\n"
            + "pub fn commit_with_record_impl<T, D: CommitPrimitives<T>, const WRITE_AHEAD: bool, P, F>(driver: &mut D, tx: &mut OccTransaction<T>, prepare: P)\n"
            + " -> (result: Result<OccCommitRecord<T>, Error>)\n"
            + " requires initial(old(driver).events()), old(driver).events().write_ahead == WRITE_AHEAD,\n"
            + " ensures safe_release(final(driver).events()),\n"
            + "   !final(driver).events().health_cleanup_required,\n"
            + "   result.is_ok() ==> completed(final(driver).events()),\n"
            + "   result.is_err() && final(driver).events().rows_published ==> final(driver).events().poisoned,\n"
            + "{\n" + show_tokens(body) + "\n}\n}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generated = render(SOURCE.read_text())
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != generated:
            raise SystemExit("stale concurrent adapter: run verification/concurrent/generate.py")
    else:
        OUTPUT.write_text(generated)

if __name__ == "__main__":
    main()
