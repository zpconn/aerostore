// Conditional proof of the native predicate dependency-capture loop.
// Primitive stamp visibility under held bucket guards remains an assumption.
use vstd::prelude::*;
mod bucket_kernels {
    /* BUCKET_KERNELS */
}
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

#[derive(PartialEq, Eq, Debug)]
pub enum Error { SerializationFailure, Index }
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct IndexRead { pub index_offset: usize, pub bucket: usize, pub stamp: u64 }
pub struct Transaction { pub txid: u64, pub index_conflict: bool, pub index_reads: Vec<IndexRead> }

pub open spec fn same_key(a: IndexRead, b: IndexRead) -> bool {
    a.index_offset == b.index_offset && a.bucket == b.bucket
}
pub open spec fn unique(reads: Seq<IndexRead>) -> bool {
    forall|i: int, j: int| 0 <= i < j < reads.len() ==> !same_key(reads[i], reads[j])
}
pub open spec fn unique_buckets(buckets: Seq<usize>) -> bool {
    forall|i: int, j: int| 0 <= i < j < buckets.len() ==> buckets[i] != buckets[j]
}
pub open spec fn suffix_from_processed(reads: Seq<IndexRead>, prior_len: int,
    offset: usize, processed: Seq<usize>) -> bool {
    forall|i: int| prior_len <= i < reads.len() ==>
        reads[i].index_offset == offset && processed.contains(reads[i].bucket)
}

// These proof-only callers use the actual extracted production kernels. Their
// postconditions supply the new capture premise, not an independent axiom.
pub fn canonical_sort_for_capture(input: &[usize]) -> (r: Result<Vec<usize>, usize>)
    ensures r.is_ok() ==> unique_buckets(r.unwrap()@),
{
    let r = bucket_kernels::canonical_buckets_sort(input, 4096);
    r
}
pub fn canonical_bitmap_for_capture(input: &[usize]) -> (r: Result<Vec<usize>, usize>)
    ensures r.is_ok() ==> unique_buckets(r.unwrap()@),
{
    let r = bucket_kernels::canonical_buckets_bitmap(input, 4096);
    r
}

pub proof fn prior_prefix_covers_current_bucket(reads: Seq<IndexRead>, prior_len: int,
    offset: usize, buckets: Seq<usize>, position: int)
    requires 0 <= prior_len <= reads.len(), 0 <= position < buckets.len(),
        unique_buckets(buckets),
        suffix_from_processed(reads, prior_len, offset, buckets.take(position)),
    ensures forall|i: int| prior_len <= i < reads.len() ==>
        reads[i].index_offset != offset || reads[i].bucket != buckets[position],
{
    assert forall|i: int| prior_len <= i < reads.len() implies
        reads[i].index_offset != offset || reads[i].bucket != buckets[position] by {
        let j = choose|j: int| 0 <= j < buckets.take(position).len()
            && buckets.take(position)[j] == reads[i].bucket;
        assert(buckets[j] != buckets[position]);
    }
}

pub proof fn duplicate_bucket_requires_canonicalization(read: IndexRead)
    ensures !unique_buckets(seq![read.bucket, read.bucket]),
        !unique(seq![read, read]),
{
    let repeated = seq![read, read];
    assert(same_key(repeated[0], repeated[1]));
    assert(!unique(repeated));
}
pub open spec fn captured(reads: Seq<IndexRead>, offset: usize, bucket: usize, stamp: u64) -> bool {
    reads.contains(IndexRead { index_offset: offset, bucket, stamp })
}
pub open spec fn extends(before: Seq<IndexRead>, after: Seq<IndexRead>) -> bool {
    before.len() <= after.len()
        && forall|i: int| 0 <= i < before.len() ==> before[i] == after[i]
}
pub open spec fn permitted_additions(before: Seq<IndexRead>, after: Seq<IndexRead>,
    offset: usize, buckets: Seq<usize>, stamps: Map<usize, u64>) -> bool {
    forall|r: IndexRead| after.contains(r) ==> before.contains(r)
        || (r.index_offset == offset && buckets.contains(r.bucket)
            && stamps.contains_key(r.bucket) && r.stamp == stamps[r.bucket])
}

// Abstracts only the standard iterator's first matching element, not native
// validation. The lowering is executable and itself verified here.
pub fn find_read_prefix(reads: &Vec<IndexRead>, prior_len: usize, offset: usize, bucket: usize)
    -> (r: Option<IndexRead>)
    requires prior_len <= reads.len(),
    ensures
        r.is_some() ==> exists|i: int| 0 <= i < prior_len && reads[i] == r.unwrap()
            && r.unwrap().index_offset == offset && r.unwrap().bucket == bucket
            && forall|j: int| 0 <= j < i ==>
                reads[j].index_offset != offset || reads[j].bucket != bucket,
        r.is_none() ==> forall|i: int| 0 <= i < prior_len ==>
            reads[i].index_offset != offset || reads[i].bucket != bucket,
{
    let mut i = 0;
    while i < prior_len
        invariant i <= prior_len <= reads.len(),
            forall|j: int| 0 <= j < i ==> reads[j].index_offset != offset || reads[j].bucket != bucket,
        decreases prior_len - i,
    {
        let read = &reads[i];
        if read.index_offset == offset && read.bucket == bucket { return Some(*read); }
        i += 1;
    }
    None
}

pub trait CaptureIndex {
    spec fn offset(&self) -> usize;
    spec fn stamps(&self) -> Map<usize, u64>;
    spec fn held(&self) -> Set<usize>;
    fn header_offset(&self) -> (r: usize) ensures r == self.offset();
    fn transactional_stamp(&self, bucket: usize) -> (r: Result<u64, Error>)
        requires self.held().contains(bucket), self.stamps().contains_key(bucket),
        ensures r.is_ok() ==> r.unwrap() == self.stamps()[bucket],
            r.is_err() ==> r == Err(Error::Index);
}

pub proof fn appended_read_preserves_unique(reads: Seq<IndexRead>, next: IndexRead)
    requires unique(reads),
        forall|i: int| 0 <= i < reads.len() ==> !same_key(reads[i], next),
    ensures unique(reads.push(next)),
{
    assert forall|i: int, j: int| 0 <= i < j < reads.push(next).len()
        implies !same_key(reads.push(next)[i], reads.push(next)[j]) by {
        if j < reads.len() { assert(!same_key(reads[i], reads[j])); }
    }
}

pub fn capture_dependencies<I: CaptureIndex>(index: &I, tx: &mut Transaction, buckets: &Vec<usize>)
    -> (result: Result<(), Error>)
    requires unique(old(tx).index_reads@), unique_buckets(buckets@),
        forall|i: int| 0 <= i < buckets.len() ==>
            index.held().contains(buckets[i]) && index.stamps().contains_key(buckets[i]),
    ensures
        final(tx).txid == old(tx).txid,
        old(tx).index_conflict ==> final(tx).index_conflict,
        result.is_ok() ==> final(tx).index_conflict == old(tx).index_conflict,
        unique(final(tx).index_reads@), extends(old(tx).index_reads@, final(tx).index_reads@),
        permitted_additions(old(tx).index_reads@, final(tx).index_reads@, index.offset(), buckets@, index.stamps()),
        final(tx).index_reads.len() <= old(tx).index_reads.len() + buckets.len(),
        result == Err(Error::SerializationFailure) ==> final(tx).index_conflict,
        result.is_ok() ==> forall|i: int| 0 <= i < buckets.len() ==>
            captured(final(tx).index_reads@, index.offset(), buckets[i], index.stamps()[buckets[i]])
                && index.stamps()[buckets[i]] < final(tx).txid,
{
    /* NATIVE_CAPTURE_BODY */
}
}
