//! Small production kernels with independently checked functional contracts.
//!
//! The `/*@ ... @*/` comments are proof annotations consumed by the Verus
//! adapter. Ordinary Rust and the Lean extractor compile these exact bodies.

/// Return the input's sorted, duplicate-free bucket IDs, or an invalid ID.
///
/// This insertion implementation is a verification candidate. Callers choose
/// it explicitly; the engine's default remains its existing standard sort.
pub fn canonical_buckets_sort(input: &[usize], bucket_count: usize) -> Result<Vec<usize>, usize> {
    let mut output: Vec<usize> = Vec::new();
    let mut i: usize = 0;
    while i < input.len()
    /*@
        invariant
            i <= input.len(),
            output.len() <= i,
            sorted(output@),
            in_range(output@, bucket_count),
            forall|k: int| 0 <= k < i ==> input@[k] < bucket_count,
            forall|b: usize| output@.contains(b) <==> input@.take(i as int).contains(b),
        decreases input.len() - i,
    @*/
    {
        let bucket = input[i];
        if bucket >= bucket_count {
            /*@ proof { first_invalid_witness(input@, bucket_count, i as int); } @*/
            return Err(bucket);
        }
        let mut position: usize = 0;
        while position < output.len() && output[position] < bucket
        /*@
            invariant
                position <= output.len(),
                sorted(output@),
                forall|b: usize| output@.contains(b) <==> input@.take(i as int).contains(b),
                forall|j: int| 0 <= j < position ==> output@[j] < bucket,
            decreases output.len() - position,
        @*/
        {
            position += 1;
        }
        if position == output.len() || output[position] != bucket {
            /*@ proof {
                assert(forall|j: int| position <= j < output.len() ==> bucket < output@[j]);
                insertion_membership(output@, position as int, bucket);
            } @*/
            /*@ let ghost original = output@; @*/
            output.push(bucket);
            let mut shift = output.len() - 1;
            while shift > position
            /*@
                invariant
                    position <= shift <= original.len(),
                    output.len() == original.len() + 1,
                    forall|j: int| 0 <= j < shift ==> output@[j] == original[j],
                    forall|j: int| shift < j < output.len() ==> output@[j] == original[j - 1],
                decreases shift - position,
            @*/
            {
                output[shift] = output[shift - 1];
                shift -= 1;
            }
            output[position] = bucket;
            /*@ proof { assert(output@ =~= original.insert(position as int, bucket)); } @*/
        } else {
            /*@ proof { assert(output@.contains(bucket)); } @*/
        }
        /*@ proof {
            assert(input@.take((i + 1) as int) == input@.take(i as int).push(bucket));
            assert forall|b: usize| output@.contains(b) <==> input@.take((i + 1) as int).contains(b) by {};
        } @*/
        i += 1;
    }
    /*@ proof { sorted_range_bound(output@, bucket_count); } @*/
    Ok(output)
}

/// Return the same canonical bucket set using one presence flag per bucket.
///
/// Invalid inputs are rejected before allocating the flags. Allocation failure
/// retains Rust's normal allocator behavior; these helpers do not implement a
/// fallible allocator or promise success for unbounded `bucket_count` values.
pub fn canonical_buckets_bitmap(input: &[usize], bucket_count: usize) -> Result<Vec<usize>, usize> {
    let mut i: usize = 0;
    while i < input.len()
    /*@
        invariant
            i <= input.len(),
            forall|j: int| 0 <= j < i ==> input@[j] < bucket_count,
        decreases input.len() - i,
    @*/
    {
        if input[i] >= bucket_count {
            /*@ proof { first_invalid_witness(input@, bucket_count, i as int); } @*/
            return Err(input[i]);
        }
        i += 1;
    }
    let mut present: Vec<bool> = Vec::new();
    let mut bucket: usize = 0;
    while bucket < bucket_count
    /*@
        invariant
            bucket <= bucket_count,
            present.len() == bucket,
            forall|j: int| 0 <= j < present.len() ==> !present@[j],
        decreases bucket_count - bucket,
    @*/
    {
        present.push(false);
        bucket += 1;
    }
    i = 0;
    while i < input.len()
    /*@
        invariant
            i <= input.len(),
            present.len() == bucket_count,
            forall|j: int| 0 <= j < input.len() ==> input@[j] < bucket_count,
            forall|b: usize| b < bucket_count ==> (present@[b as int] <==> input@.take(i as int).contains(b)),
        decreases input.len() - i,
    @*/
    {
        present[input[i]] = true;
        /*@ proof {
            assert(input@.take((i + 1) as int) == input@.take(i as int).push(input@[i as int]));
        } @*/
        i += 1;
    }
    let mut output: Vec<usize> = Vec::new();
    bucket = 0;
    while bucket < bucket_count
    /*@
        invariant
            bucket <= bucket_count,
            present.len() == bucket_count,
            sorted(output@),
            output.len() <= bucket,
            forall|j: int| 0 <= j < output.len() ==> output@[j] < bucket,
            forall|j: int| 0 <= j < input.len() ==> input@[j] < bucket_count,
            forall|b: usize| b < bucket_count ==> (present@[b as int] <==> input@.contains(b)),
            forall|b: usize| output@.contains(b) <==> (b < bucket && input@.contains(b)),
        decreases bucket_count - bucket,
    @*/
    {
        if present[bucket] {
            output.push(bucket);
        }
        bucket += 1;
    }
    /*@ proof { membership_length_bound(output@, input@); } @*/
    Ok(output)
}

/// Whether a recorded publication predates this transaction's start ID.
///
/// This proves a scalar decision, not atomic publication or memory ordering.
#[inline]
pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> bool {
    stamp < transaction_id
}

#[cfg(test)]
mod tests;
