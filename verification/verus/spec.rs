// This contract and its proof-library roots are frozen by the acceptance gate.
broadcast use vstd::seq_lib::group_seq_properties;

pub open spec fn sorted(values: Seq<usize>) -> bool {
    forall|i: int, j: int| 0 <= i < j < values.len() ==> values[i] < values[j]
}

pub open spec fn in_range(values: Seq<usize>, count: usize) -> bool {
    forall|i: int| 0 <= i < values.len() ==> values[i] < count
}

pub open spec fn first_invalid(input: Seq<usize>, count: usize, bad: usize) -> bool {
    exists|i: int| 0 <= i < input.len()
        && input[i] == bad && bad >= count
        && forall|j: int| 0 <= j < i ==> input[j] < count
}

pub open spec fn canonical(input: Seq<usize>, count: usize, output: Seq<usize>) -> bool {
    sorted(output) && in_range(output, count)
        && output.len() <= input.len() && output.len() <= count
        && forall|b: usize| output.contains(b) <==> input.contains(b)
}

pub open spec fn bucket_result(input: Seq<usize>, count: usize, result: Result<Vec<usize>, usize>) -> bool {
    match result {
        Ok(output) => canonical(input, count, output@),
        Err(bad) => first_invalid(input, count, bad),
    }
}

proof fn first_invalid_witness(input: Seq<usize>, count: usize, i: int)
    requires 0 <= i < input.len(), input[i] >= count,
        forall|j: int| 0 <= j < i ==> input[j] < count,
    ensures first_invalid(input, count, input[i]),
{
    assert(exists|k: int| 0 <= k < input.len()
        && input[k] == input[i] && input[i] >= count
        && forall|j: int| 0 <= j < k ==> input[j] < count);
}

proof fn sorted_index_bound(values: Seq<usize>, i: int)
    requires sorted(values), 0 <= i < values.len(),
    ensures i <= values[i],
    decreases i,
{
    if i > 0 {
        sorted_index_bound(values, i - 1);
        assert(values[i - 1] < values[i]);
    }
}

proof fn insertion_membership(values: Seq<usize>, position: int, value: usize)
    requires 0 <= position <= values.len(),
    ensures forall|b: usize| #[trigger] values.insert(position, value).contains(b)
        <==> values.contains(b) || b == value,
{
    let inserted = values.insert(position, value);
    assert forall|b: usize| inserted.contains(b) <==> values.contains(b) || b == value by {
        if inserted.contains(b) {
            let j = choose|j: int| 0 <= j < inserted.len() && inserted[j] == b;
            if j < position {
                assert(values[j] == b);
            } else if j > position {
                assert(values[j - 1] == b);
            }
        }
        if values.contains(b) {
            let j = choose|j: int| 0 <= j < values.len() && values[j] == b;
            if j < position {
                assert(inserted[j] == b);
            } else {
                assert(inserted[j + 1] == b);
            }
        }
        if b == value { assert(inserted[position] == b); }
    }
}

proof fn sorted_range_bound(values: Seq<usize>, count: usize)
    requires sorted(values), in_range(values, count),
    ensures values.len() <= count,
{
    if values.len() > 0 {
        sorted_index_bound(values, values.len() - 1);
    }
}

proof fn membership_length_bound(values: Seq<usize>, input: Seq<usize>)
    requires sorted(values),
        forall|b: usize| values.contains(b) ==> input.contains(b),
    ensures values.len() <= input.len(),
{
    assert(values.no_duplicates());
    values.unique_seq_to_set();
    input.lemma_cardinality_of_set();
    values.to_set_ensures();
    input.to_set_ensures();
    assert(values.to_set().subset_of(input.to_set()));
    vstd::set_lib::lemma_len_subset(values.to_set(), input.to_set());
}

proof fn tail_membership(values: Seq<usize>)
    requires sorted(values), values.len() > 0,
    ensures forall|b: usize| #[trigger] values.drop_first().contains(b)
        <==> values.contains(b) && b != values[0],
{
    assert forall|b: usize| values.drop_first().contains(b)
        <==> values.contains(b) && b != values[0] by {
        if values.drop_first().contains(b) {
            let i = choose|i: int| 0 <= i < values.drop_first().len()
                && values.drop_first()[i] == b;
            assert(values[i + 1] == b);
        }
        if values.contains(b) && b != values[0] {
            let i = choose|i: int| 0 <= i < values.len() && values[i] == b;
            assert(i > 0);
            assert(values.drop_first()[i - 1] == b);
        }
    }
}

pub proof fn canonical_sequence_unique(left: Seq<usize>, right: Seq<usize>)
    requires sorted(left), sorted(right),
        forall|b: usize| left.contains(b) <==> right.contains(b),
    ensures left == right,
    decreases left.len() + right.len(),
{
    if left.len() == 0 {
        if right.len() > 0 { assert(left.contains(right[0])); }
        assert(left =~= right);
    } else {
        assert(right.contains(left[0]));
        assert(right.len() > 0);
        assert(left.contains(right[0]));
        let i = choose|i: int| 0 <= i < left.len() && left[i] == right[0];
        let j = choose|j: int| 0 <= j < right.len() && right[j] == left[0];
        assert(left[0] <= left[i]);
        assert(right[0] <= right[j]);
        assert(left[0] == right[0]);
        tail_membership(left);
        tail_membership(right);
        canonical_sequence_unique(left.drop_first(), right.drop_first());
        assert(left =~= right);
    }
}

pub proof fn first_invalid_unique(input: Seq<usize>, count: usize, left: usize, right: usize)
    requires first_invalid(input, count, left), first_invalid(input, count, right),
    ensures left == right,
{
    let i = choose|i: int| 0 <= i < input.len() && input[i] == left && left >= count
        && forall|j: int| 0 <= j < i ==> input[j] < count;
    let j = choose|j: int| 0 <= j < input.len() && input[j] == right && right >= count
        && forall|k: int| 0 <= k < j ==> input[k] < count;
    assert(i == j);
}

pub open spec fn equivalent_results(left: Result<Vec<usize>, usize>, right: Result<Vec<usize>, usize>) -> bool {
    match (left, right) {
        (Ok(a), Ok(b)) => a@ == b@,
        (Err(a), Err(b)) => a == b,
        _ => false,
    }
}

pub proof fn canonical_result_unique(input: Seq<usize>, count: usize,
    left: Result<Vec<usize>, usize>, right: Result<Vec<usize>, usize>)
    requires bucket_result(input, count, left), bucket_result(input, count, right),
    ensures equivalent_results(left, right),
{
    match (left, right) {
        (Ok(a), Ok(b)) => canonical_sequence_unique(a@, b@),
        (Err(a), Err(b)) => first_invalid_unique(input, count, a, b),
        (Ok(a), Err(b)) | (Err(b), Ok(a)) => {
            let i = choose|i: int| 0 <= i < input.len() && input[i] == b && b >= count
                && forall|j: int| 0 <= j < i ==> input[j] < count;
            assert(input.contains(b));
            assert(a@.contains(b));
            let j = choose|j: int| 0 <= j < a.len() && a@[j] == b;
            assert(a@[j] < count);
        }
    }
}
