// Contract composition over two represented states. This harness is proof-only;
// it does not implement a production transaction or establish a concurrent trace.
mod capture {
    /* CAPTURE_MODULE */
}
mod predicate {
    /* PREDICATE_MODULE */
}
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

#[derive(PartialEq, Eq)]
pub enum ReadResult { CaptureError, ValidationError, Checked(bool) }

pub open spec fn translated(read: capture::IndexRead) -> predicate::IndexRead {
    predicate::IndexRead { index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp }
}
pub open spec fn translated_reads(before: Seq<capture::IndexRead>, after: Seq<predicate::IndexRead>) -> bool {
    before.len() == after.len()
    && forall|i: int| 0 <= i < before.len() ==> after[i] == translated(before[i])
}
pub open spec fn reads_held(s: predicate::State, reads: Seq<capture::IndexRead>) -> bool {
    forall|i: int| 0 <= i < reads.len() ==>
        s.bindings.contains_key(reads[i].index_offset)
        && s.held.contains((s.bindings[reads[i].index_offset], reads[i].bucket))
}
pub open spec fn has_late_publication(s: predicate::State, offset: usize, buckets: Seq<usize>, start: u64) -> bool {
    exists|i: int| 0 <= i < buckets.len()
        && s.stamps.contains_key((s.bindings[offset], buckets[i]))
        && s.stamps[(s.bindings[offset], buckets[i])] >= start
}

// The native publication theorem exports stamp_relation. This bridge turns
// that exact data relation into the hypothesis consumed by the capture/check
// composition. The temporal stamp >= start premise is still explicit.
pub proof fn publication_supplies_late_bucket(before: predicate::State, after: predicate::State,
    offset: usize, buckets: Seq<usize>, bucket: usize, touched: Set<predicate::Pair>,
    stamp: u64, start: u64)
    requires before.bindings == after.bindings, before.bindings.contains_key(offset),
        buckets.contains(bucket), touched.contains((before.bindings[offset], bucket)),
        predicate::stamp_relation(before.stamps, after.stamps, touched, stamp), stamp >= start,
    ensures has_late_publication(after, offset, buckets, start),
{
    let i = choose|i: int| 0 <= i < buckets.len() && buckets[i] == bucket;
    assert(after.stamps.contains_key((after.bindings[offset], buckets[i])));
    assert(after.stamps[(after.bindings[offset], buckets[i])] >= start);
}

pub proof fn read_keys_are_held(s: predicate::State, reads: Seq<predicate::IndexRead>, n: int)
    requires 0 <= n <= reads.len(),
        forall|i: int| 0 <= i < n ==> s.held.contains(predicate::read_pair(s, reads[i])),
    ensures predicate::read_keys(s, reads, n).subset_of(s.held),
    decreases n,
{
    if n > 0 { read_keys_are_held(s, reads, n - 1); }
}

pub proof fn capture_preserves_guard_coverage(s: predicate::State, before: Seq<capture::IndexRead>,
    after: Seq<capture::IndexRead>, offset: usize, buckets: Seq<usize>, stamps: Map<usize, u64>)
    requires reads_held(s, before), s.bindings.contains_key(offset),
        capture::permitted_additions(before, after, offset, buckets, stamps),
        forall|i: int| 0 <= i < buckets.len() ==> s.held.contains((s.bindings[offset], buckets[i])),
    ensures reads_held(s, after),
{
    assert forall|j: int| 0 <= j < after.len() implies
        s.bindings.contains_key(after[j].index_offset)
        && s.held.contains((s.bindings[after[j].index_offset], after[j].bucket)) by {
        assert(after.contains(after[j]));
        if before.contains(after[j]) {
            let i = choose|i: int| 0 <= i < before.len() && before[i] == after[j];
            assert(s.held.contains((s.bindings[before[i].index_offset], before[i].bucket)));
        } else {
            assert(buckets.contains(after[j].bucket));
            let i = choose|i: int| 0 <= i < buckets.len() && buckets[i] == after[j].bucket;
            assert(s.held.contains((s.bindings[offset], buckets[i])));
        }
    }
}

pub proof fn captured_read_survives_translation(before: Seq<capture::IndexRead>, after: Seq<predicate::IndexRead>,
    offset: usize, bucket: usize, stamp: u64)
    requires translated_reads(before, after), capture::captured(before, offset, bucket, stamp),
    ensures exists|i: int| 0 <= i < after.len() && after[i].index_offset == offset
        && after[i].bucket == bucket && after[i].stamp == stamp,
{
    let i = choose|i: int| 0 <= i < before.len()
        && before[i] == (capture::IndexRead { index_offset: offset, bucket, stamp });
    assert(after[i] == translated(before[i]));
}

// The current native capture operation runs against `index`; the current native
// validation runs against `later`. The caller must justify that these interfaces
// represent the actual two states, including guard lifetime and registry mapping.
// No assumption states the desired conflict conclusion.
pub fn capture_then_validate<I: capture::CaptureIndex, D: predicate::Primitives>(
    index: &I, later: &D, tx: &mut capture::Transaction, buckets: &Vec<usize>) -> (result: ReadResult)
    requires capture::unique(old(tx).index_reads@),
        later.state().bindings.contains_key(index.offset()),
        reads_held(later.state(), old(tx).index_reads@),
        forall|i: int| 0 <= i < buckets.len() ==>
            index.held().contains(buckets[i]) && index.stamps().contains_key(buckets[i])
            && later.state().held.contains((later.state().bindings[index.offset()], buckets[i])),
    ensures result == ReadResult::Checked(false) ==>
        !has_late_publication(later.state(), index.offset(), buckets@, old(tx).txid),
{
    let ghost initial_reads = tx.index_reads@;
    match capture::capture_dependencies(index, tx, buckets) {
        Err(_) => return ReadResult::CaptureError,
        Ok(()) => {},
    }
    proof {
        capture_preserves_guard_coverage(later.state(), initial_reads, tx.index_reads@,
            index.offset(), buckets@, index.stamps());
    }
    let mut reads = Vec::new();
    let mut i: usize = 0;
    while i < tx.index_reads.len()
        invariant i <= tx.index_reads.len(), reads.len() == i,
            forall|j: int| 0 <= j < i ==> reads[j] == translated(tx.index_reads[j]),
            reads_held(later.state(), initial_reads), reads_held(later.state(), tx.index_reads@),
            capture::extends(initial_reads, tx.index_reads@),
            later.state().bindings.contains_key(index.offset()),
            forall|j: int| 0 <= j < buckets.len() ==>
                later.state().held.contains((later.state().bindings[index.offset()], buckets[j])),
        decreases tx.index_reads.len() - i,
    {
        let read = &tx.index_reads[i];
        reads.push(predicate::IndexRead { index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp });
        i += 1;
    }
    let validated = predicate::OccTransaction { txid: tx.txid, index_conflict: tx.index_conflict, index_reads: reads };
    proof {
        assert(translated_reads(tx.index_reads@, validated.index_reads@));
        // Provenance from the capture contract restricts additions to the
        // guarded buckets, preserving guard coverage during translation.
        assert(reads_held(later.state(), tx.index_reads@));
        assert forall|j: int| 0 <= j < validated.index_reads.len()
            implies later.state().held.contains(predicate::read_pair(later.state(), validated.index_reads[j])) by {
            assert(validated.index_reads[j] == translated(tx.index_reads[j]));
        }
        read_keys_are_held(later.state(), validated.index_reads@, validated.index_reads.len() as int);
    }
    let conflict = match predicate::index_read_conflict(later, &validated) {
        Err(_) => return ReadResult::ValidationError,
        Ok(conflict) => conflict,
    };
    proof {
        if !conflict && has_late_publication(later.state(), index.offset(), buckets@, tx.txid) {
            let b = choose|j: int| 0 <= j < buckets.len()
                && later.state().stamps.contains_key((later.state().bindings[index.offset()], buckets[j]))
                && later.state().stamps[(later.state().bindings[index.offset()], buckets[j])] >= tx.txid;
            captured_read_survives_translation(tx.index_reads@, validated.index_reads@,
                index.offset(), buckets[b], index.stamps()[buckets[b]]);
            let r = choose|j: int| 0 <= j < validated.index_reads.len()
                && validated.index_reads[j].index_offset == index.offset()
                && validated.index_reads[j].bucket == buckets[b]
                && validated.index_reads[j].stamp == index.stamps()[buckets[b]];
            assert(!predicate::read_good(later.state(), validated.index_reads[r], tx.txid));
            assert(false);
        }
    }
    ReadResult::Checked(conflict)
}
}
