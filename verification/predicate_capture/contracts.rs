// Conditional proof of the native predicate dependency-capture loop.
// Primitive stamp visibility under held bucket guards remains an assumption.
use vstd::prelude::*;
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
pub fn find_read(reads: &Vec<IndexRead>, offset: usize, bucket: usize)
    -> (r: Option<IndexRead>)
    ensures
        r.is_some() ==> reads@.contains(r.unwrap())
            && r.unwrap().index_offset == offset && r.unwrap().bucket == bucket,
        r.is_none() ==> forall|i: int| 0 <= i < reads.len() ==>
            reads[i].index_offset != offset || reads[i].bucket != bucket,
{
    let mut i = 0;
    while i < reads.len()
        invariant i <= reads.len(),
            forall|j: int| 0 <= j < i ==> reads[j].index_offset != offset || reads[j].bucket != bucket,
        decreases reads.len() - i,
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
    requires unique(old(tx).index_reads@),
        forall|i: int| 0 <= i < buckets.len() ==>
            index.held().contains(buckets[i]) && index.stamps().contains_key(buckets[i]),
    ensures
        final(tx).txid == old(tx).txid,
        old(tx).index_conflict ==> final(tx).index_conflict,
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
