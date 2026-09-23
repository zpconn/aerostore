#!/usr/bin/env python3
"""Restricted adapter for actual bucket selection and guard acquisition loops."""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"
INDEX = ROOT / "aerostore_core/src/shm_index.rs"
CONTRACTS = HERE / "contracts.rs"
OUTPUT = HERE / "guards.verus.rs"
spec = importlib.util.spec_from_file_location("guard_native_adapter", ROOT / "verification/concurrent/generate.py")
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)
adapter.TOKEN = re.compile(adapter.TOKEN.pattern.replace("::|->", "<=|>=|::|->")
                           .replace("|[0-9]+|", "|0x[0-9a-fA-F]+|[0-9]+|"))


def method(source, name, signature):
    marker = "fn " + name + "("
    if source.count(marker) != 1:
        raise ValueError("expected one native " + name)
    start = source.index(marker)
    opening = source.index("{", start)
    # Lifetimes are checked exactly before the body tokenizer sees them.
    if re.sub(r"\s+", "", source[start:opening]) != re.sub(r"\s+", "", signature):
        raise ValueError("native signature changed: " + name)
    depth, end = 1, opening + 1
    # Only tokenize through the method, allowing braces in comments via tokenization.
    next_method = re.search(r"\n    (?:pub(?:\([^\n]*\))? )?fn ", source[opening + 1:])
    tail = source[opening:opening + 1 + next_method.start()] if next_method else source[opening:]
    tokens = adapter.tokenize(tail)
    end = adapter.balanced_end(tokens, 0)
    return tokens[1:end - 1]


def reject(tokens):
    if set(tokens) & {"unsafe", "assume", "admit", "external_body", "proof", "ghost", "tracked", "#"}:
        raise ValueError("unsupported syntax/proof bypass")


def render(source=None, index=None):
    source = SOURCE.read_text() if source is None else source
    index = INDEX.read_text() if index is None else index
    constant = re.findall(r"const INDEX_LOCK_SPIN_LIMIT: u32 = (\d+);", source)
    if len(constant) != 1 or int(constant[0]) <= 0:
        raise ValueError("missing bounded retry limit")
    body = method(index, "transactional_try_lock_bucket", """fn transactional_try_lock_bucket(
        &self, bucket: usize,
    ) -> Result<Option<ShmMutexGuard<'_>>, ShmIndexError>""")
    body = adapter.replace(body, "self.publication_header()", "driver.publication_header(binding)", 1)
    body = adapter.replace(body, "header.poisoned.load(AtomicOrdering::Acquire)", "driver.poisoned(header)", 1)
    body = adapter.replace(body, "header.buckets.get(bucket)", "driver.bucket(header, bucket)", 1)
    body = adapter.replace(body, "bucket.lock.try_lock()", "driver.try_lock(bucket)", 1)
    body = ["Error" if t == "ShmIndexError" else t for t in body]
    reject(body)
    try_body = adapter.show_tokens(body)

    body = method(source, "acquire_index_bucket", """fn acquire_index_bucket(
        index: &SecondaryIndex<usize>, bucket: usize,
    ) -> Result<ShmMutexGuard<'_>, Error>""")
    body = adapter.replace(body, """#[cfg(test)] if attempt == 0 {
        INDEX_BUCKET_CONTENDED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() { hook(); }
        });
    }""", "", 1)
    body = adapter.replace(body, "index.transactional_try_lock_bucket(bucket)",
                           "transactional_try_lock_bucket(driver, binding, bucket)", 1)
    body = adapter.replace(body, "std::thread::yield_now()", "driver.yield_now()", 1)
    body = adapter.replace(body, "std::hint::spin_loop()", "driver.spin_loop()", 1)
    opening = adapter.tokenize("for attempt in 0..INDEX_LOCK_SPIN_LIMIT {")
    if body[:len(opening)] != opening:
        raise ValueError("unsupported retry iteration")
    end = adapter.balanced_end(body, len(opening) - 1)
    inside = body[len(opening):end - 1]
    reject(inside + body[end:])
    bucket_body = (f"let mut attempt: u32 = 0;\nwhile attempt < {constant[0]}\n"
                   f"invariant attempt <= {constant[0]}, binding < driver.registry().len(),\n"
                   f"decreases {constant[0]} - attempt,\n{{\n" + adapter.show_tokens(inside)
                   + "\nattempt += 1;\n}\n" + adapter.show_tokens(body[end:]))

    body = method(source, "acquire_index_locks", """fn acquire_index_locks(
        &self, keys: &[(usize, usize)],
    ) -> Result<Vec<ShmMutexGuard<'_>>, Error>""")
    prefix = adapter.tokenize("let mut guards = Vec::with_capacity(keys.len()); for (binding, bucket) in keys {")
    if body[:len(prefix)] != prefix:
        raise ValueError("unsupported guard collection prefix")
    end = adapter.balanced_end(body, len(prefix) - 1)
    inside = body[len(prefix):end - 1]
    inside = adapter.replace(inside, "Self::acquire_index_bucket(&self.indexes[*binding].index, *bucket,)",
                             "acquire_index_bucket(driver, *binding, *bucket)", 1)
    reject(inside + body[end:])
    locks_body = """let mut guards: Vec<Guard> = Vec::with_capacity(keys.len());
    let mut pos: usize = 0;
    while pos < keys.len()
        invariant pos <= keys.len(), guards.len() == pos,
            keys_valid(driver.registry(), keys@),
            forall|i: int| 0 <= i < pos ==> guards[i].physical == physical(driver.registry(), keys[i]),
            forall|i: int| 0 <= i < pos ==> guards[i].arena == driver.arena(),
            forall|i: int| 0 <= i < pos ==> keys[i].1 < driver.bucket_count(),
        decreases keys.len() - pos,
    {
        let (binding, bucket) = &keys[pos];
""" + adapter.show_tokens(inside) + "\npos += 1;\n}\n" + adapter.show_tokens(body[end:])
    template = CONTRACTS.read_text()
    for marker, value in [("/* NATIVE_TRY_LOCK */", try_body), ("/* NATIVE_ACQUIRE_BUCKET */", bucket_body),
                          ("/* NATIVE_ACQUIRE_LOCKS */", locks_body)]:
        if template.count(marker) != 1:
            raise ValueError("missing/duplicate native placeholder")
        template = template.replace(marker, value)
    return "// Generated from native guard acquisition; see generate.py.\n" + template


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    output = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != output:
            raise SystemExit("stale native guard proof")
    else:
        OUTPUT.write_text(output)


if __name__ == "__main__":
    main()
