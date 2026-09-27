#!/usr/bin/env python3
"""Restricted source adapter for the native index_lookup capture loop.

Only iterator lowering and the independently proved scalar stamp helper are
adapted. The surrounding lookup is checked for the guard/capture/raw-lookup
order; row materialization and the guard implementations are not proved here.
"""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("commit_adapter", ROOT / "verification/concurrent/generate.py")
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)
adapter.TOKEN = re.compile(adapter.TOKEN.pattern.replace("::|->", "<=|>=|::|->"))
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"
CONTRACTS = Path(__file__).with_name("contracts.rs")
OUTPUT = Path(__file__).with_name("capture.verus.rs")
HELPER = ROOT / "aerostore_verified/src/lib.rs"
SELECTOR = ROOT / "aerostore_core/src/shm_index.rs"
KERNEL_SPEC = importlib.util.spec_from_file_location("capture_bucket_kernel_adapter", ROOT / "verification/verus/generate.py")
kernels = importlib.util.module_from_spec(KERNEL_SPEC)
KERNEL_SPEC.loader.exec_module(kernels)
LEXER_SPEC = importlib.util.spec_from_file_location("capture_selector_lexer", ROOT / "verification/lookup_native/check_production_equivalence.py")
selector_lexer = importlib.util.module_from_spec(LEXER_SPEC)
LEXER_SPEC.loader.exec_module(selector_lexer)

# This is a source check of the routing into canonicalization, not a proof of
# the raw key mapping or Rust's standard sort/dedup. All successful variants
# must pass through the reviewed canonicalizer. Unknown edits fail closed.
SELECTOR_BODY = '''
    let buckets = match predicate {
        IndexCompare::Eq(value) => vec![self.transactional_key_bucket(value)?],
        IndexCompare::In(values) => values.iter()
            .map(|value| self.transactional_key_bucket(value))
            .collect::<Result<Vec<_>, _>>()?,
        IndexCompare::Gt(value) | IndexCompare::Gte(value)
        | IndexCompare::Lt(value) | IndexCompare::Lte(value) => {
            EncodedKey::from_index_value(value)?;
            match (self.publication_policy, value) {
                (IndexPublicationPolicy::OrderedI64 { origin, width }, IndexValue::I64(value),) => {
                    let boundary = ordered_i64_bucket(*value, origin, width);
                    match predicate {
                        IndexCompare::Lt(_) | IndexCompare::Lte(_) => (0..=boundary).collect(),
                        _ => (boundary..INDEX_TX_BUCKETS).collect(),
                    }
                }
                _ => (0..INDEX_TX_BUCKETS).collect(),
            }
        }
    };
    #[cfg(feature = "verified-buckets-sort")]
    {
        return aerostore_verified::canonical_buckets_sort(&buckets, INDEX_TX_BUCKETS)
            .map_err(ShmIndexError::InvalidBucket);
    }
    #[cfg(feature = "verified-buckets-bitmap")]
    {
        return aerostore_verified::canonical_buckets_bitmap(&buckets, INDEX_TX_BUCKETS)
            .map_err(ShmIndexError::InvalidBucket);
    }
    #[cfg(not(any(feature = "verified-buckets-sort", feature = "verified-buckets-bitmap")))]
    {
        let mut buckets = buckets;
        buckets.sort_unstable();
        buckets.dedup();
        Ok(buckets)
    }
'''


def check_selector(source: str):
    tokens = selector_lexer.rust_tokens(source)
    positions = selector_lexer.locations(tokens, ["fn", "transactional_bucket_ids", "("])
    if len(positions) != 1:
        raise ValueError("missing or duplicated native bucket selector")
    start = positions[0] - 4
    opening = tokens.index("{", positions[0])
    if tokens[start:opening] != selector_lexer.rust_tokens('''
        pub(crate) fn transactional_bucket_ids(&self, predicate: &IndexCompare,)
            -> Result<Vec<usize>, ShmIndexError>'''):
        raise ValueError("native bucket selector signature changed")
    end = selector_lexer.balanced_end(tokens, opening)
    if tokens[opening + 1:end - 1] != selector_lexer.rust_tokens(SELECTOR_BODY):
        raise ValueError("native bucket selector canonicalization boundary changed")
    expected = selector_lexer.rust_tokens("const INDEX_TX_BUCKETS: usize = 4096;")
    positions = selector_lexer.locations(tokens, ["const", "INDEX_TX_BUCKETS"])
    if len(positions) != 1 or tokens[positions[0]:positions[0]+len(expected)] != expected:
        raise ValueError("native bucket selector domain changed")


def render(source: str, helper: str | None = None, selector: str | None = None) -> str:
    if helper is None:
        helper = HELPER.read_text()
    check_selector(SELECTOR.read_text() if selector is None else selector)
    kernel_code = kernels.render(helper)
    # The helper's executable expression is included, not replaced by its claim.
    marker = "pub fn stamp_precedes_snapshot("
    if helper.count(marker) != 1:
        raise ValueError("expected one native stamp helper")
    start = helper.index(marker)
    tokens = adapter.tokenize(helper[start:])
    opening = tokens.index("{")
    if tokens[:opening] != adapter.tokenize("pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> bool"):
        raise ValueError("native stamp helper signature changed")
    end = adapter.balanced_end(tokens, opening)
    scalar = tokens[opening + 1:end - 1]
    banned = {"unsafe", "assume", "admit", "external_body", "#", "proof", "invariant", "decreases", "ghost", "tracked"}
    if banned.intersection(scalar):
        raise ValueError("unsupported stamp helper syntax or proof bypass")
    marker = "    pub fn index_lookup("
    start = source.index(marker)
    signature = adapter.tokenize(source[start:source.index("{", start)])
    expected_signature = adapter.tokenize('''pub fn index_lookup(
        &self, tx: &mut OccTransaction<T>, index: &SecondaryIndex<usize>, predicate: &IndexCompare,
    ) -> Result<Vec<usize>, Error>''')
    if signature != expected_signature:
        raise ValueError("native index_lookup signature changed")
    body = adapter.method_body(source, "    pub fn index_lookup(", "\n    fn validate_index_bindings(")
    prefix = adapter.tokenize('''
        self.ensure_open(tx)?;
        if !self.index_is_bound(index) {
            return Err(Error::Index("query index is not bound to this table".into(),));
        }
        let binding = self.indexes.iter()
            .find(|bound| bound.index.header_offset() == index.header_offset())
            .expect("bound index checked above");
        index.transactional_check_owner(self.shared_header_offset())?;
        let buckets = index.transactional_bucket_ids(predicate)?;
        let mut guards = Vec::with_capacity(buckets.len());
        for bucket in &buckets {
            let guard = match Self::acquire_index_bucket(index, *bucket) {
                Ok(guard) => guard,
                Err(Error::SerializationFailure) => {
                    tx.index_conflict = true;
                    return Err(Error::SerializationFailure);
                }
                Err(err) => return Err(err),
            };
            guards.push(guard);
        }
    ''')
    if body[:len(prefix)] != prefix:
        raise ValueError("native lookup guard/capture prefix changed; review the primitive boundary")
    remaining = body[len(prefix):]
    declaration = adapter.tokenize("let prior_read_len =")
    if remaining[:len(declaration)] != declaration:
        raise ValueError("missing frozen prior dependency length")
    declaration_end = remaining.index(";") + 1
    prior_declaration = remaining[:declaration_end]
    if banned.intersection(prior_declaration):
        raise ValueError("unsupported prior-length syntax or proof bypass")
    remaining = remaining[declaration_end:]
    loop = adapter.tokenize("for bucket in &buckets {")
    if remaining[:len(loop)] != loop:
        raise ValueError("expected unconditional dependency capture before candidate lookup")
    end = adapter.balanced_end(remaining, len(loop) - 1)
    tail = remaining[end:]
    candidate = adapter.tokenize("let candidates = index.transactional_raw_lookup(predicate)?; drop(guards);")
    if tail[:len(candidate)] != candidate:
        raise ValueError("raw lookup must follow complete capture and precede guard release")
    expected_tail = adapter.tokenize('''
        let candidates = index.transactional_raw_lookup(predicate)?;
        drop(guards);
        #[cfg(test)]
        INDEX_CANDIDATES_CAPTURED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() { hook(); }
        });
        let mut candidates: BTreeSet<usize> = candidates.into_iter().collect();
        candidates.extend(tx.write_set.iter().map(|write| write.row_id));
        let mut result = Vec::new();
        for row_id in candidates {
            if let Some(value) = self.read(tx, row_id)? {
                if (binding.key)(&value).as_ref()
                    .is_some_and(|key| index_predicate_matches(predicate, key))
                { result.push(row_id); }
            }
        }
        Ok(result)
    ''')
    if tail != expected_tail:
        raise ValueError("native candidate/materialization boundary changed; review before reproving capture")
    capture = remaining[len(loop):end - 1]
    capture = adapter.replace(capture,
        "tx.index_reads[..prior_read_len].iter().find(|read| read.index_offset == index.header_offset() && read.bucket == *bucket)",
        "find_read_prefix(&tx.index_reads, prior_read_len, index.header_offset(), *bucket)", 1)
    capture = adapter.replace(capture,
        "tx.index_reads.iter().find(|read| read.index_offset == index.header_offset() && read.bucket == *bucket)",
        "find_read_prefix(&tx.index_reads, tx.index_reads.len(), index.header_offset(), *bucket)", 1)
    # Namespace lowering is optional so deleting the check reaches the solver.
    scalar_name = adapter.tokenize("aerostore_verified::stamp_precedes_snapshot")
    scalar_count = sum(capture[i:i+len(scalar_name)] == scalar_name for i in range(len(capture)))
    capture = adapter.replace(capture, "aerostore_verified::stamp_precedes_snapshot", "stamp_precedes_snapshot", scalar_count)
    if banned.intersection(capture):
        raise ValueError("unsupported capture syntax or proof bypass")
    # This proof lemma is placed before the unchanged push, with the exact
    # current record. It does not establish membership for a mutated push.
    code = adapter.show_tokens(capture)
    full_search = "find_read_prefix ( & tx . index_reads , tx . index_reads . len ( ) , index . header_offset ( ) , * bucket )"
    if code.count(full_search) != 1:
        raise ValueError("missing or duplicated full-vector search arm")
    # Retain the native branch and prove that this search has exactly the same
    # bound as the frozen prefix. Do not erase the branch as an adapter axiom.
    code = code.replace(full_search, '''{
        proof { assert(tx.index_reads.len() == prior_read_len); }
        ''' + full_search + '''
    }''', 1)
    push = "tx . index_reads . push ( IndexRead {"
    code = code.replace(push, '''proof {
        prior_prefix_covers_current_bucket(tx.index_reads@, prior_read_len as int,
            index.offset(), buckets@, bucket_pos as int);
        appended_read_preserves_unique(tx.index_reads@, IndexRead {
            index_offset: index.offset(), bucket: *bucket, stamp });
    }
    ''' + push)
    loop_code = '''let ghost initial_reads = tx.index_reads@;
    ''' + adapter.show_tokens(prior_declaration) + '''
    let mut bucket_pos = 0;
    while bucket_pos < buckets.len()
        invariant
            bucket_pos <= buckets.len(), unique(tx.index_reads@),
            unique_buckets(buckets@), prior_read_len == initial_reads.len(),
            initial_reads == old(tx).index_reads@, extends(initial_reads, tx.index_reads@),
            suffix_from_processed(tx.index_reads@, prior_read_len as int, index.offset(),
                buckets@.take(bucket_pos as int)),
            permitted_additions(initial_reads, tx.index_reads@, index.offset(), buckets@, index.stamps()),
            tx.index_reads.len() <= initial_reads.len() + bucket_pos,
            tx.txid == old(tx).txid,
            old(tx).index_conflict ==> tx.index_conflict,
            tx.index_conflict == old(tx).index_conflict,
            forall|j: int| 0 <= j < buckets.len() ==>
                index.held().contains(buckets[j]) && index.stamps().contains_key(buckets[j]),
            forall|j: int| 0 <= j < bucket_pos ==>
                captured(tx.index_reads@, index.offset(), buckets[j], index.stamps()[buckets[j]])
                    && index.stamps()[buckets[j]] < tx.txid,
        decreases buckets.len() - bucket_pos,
    {
        let bucket = &buckets[bucket_pos];
''' + code + '''
        bucket_pos += 1;
    }
    Ok(())'''
    helper_code = "verus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (r: bool)\nensures r == (stamp < transaction_id),\n{ " + " ".join(scalar) + " } }\n"
    return "// Generated from native index_lookup; see generate.py for the boundary.\n" + CONTRACTS.read_text().replace("/* BUCKET_KERNELS */", kernel_code).replace("/* NATIVE_CAPTURE_BODY */", loop_code) + helper_code


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = render(SOURCE.read_text())
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != result:
            raise SystemExit("stale capture adapter: run verification/predicate_capture/generate.py")
    else:
        OUTPUT.write_text(result)


if __name__ == "__main__":
    main()
