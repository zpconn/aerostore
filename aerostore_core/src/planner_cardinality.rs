use std::cmp::Reverse;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EqualityCandidateKind {
    PrimaryKey,
    SecondaryIndex,
}

#[derive(Clone, Debug)]
pub(crate) struct EqualityCandidate<T> {
    pub(crate) filter_idx: usize,
    pub(crate) distinct_key_count: usize,
    pub(crate) kind: EqualityCandidateKind,
    pub(crate) payload: T,
}

#[inline]
pub(crate) fn best_equality_candidate_index<T>(
    candidates: &[EqualityCandidate<T>],
) -> Option<usize> {
    fn ordering_key<T>(candidate: &EqualityCandidate<T>) -> (Reverse<usize>, u8, usize) {
        let kind_rank = match candidate.kind {
            EqualityCandidateKind::PrimaryKey => 0,
            EqualityCandidateKind::SecondaryIndex => 1,
        };
        (
            Reverse(candidate.distinct_key_count),
            kind_rank,
            candidate.filter_idx,
        )
    }

    candidates
        .iter()
        .enumerate()
        .min_by_key(|(_, candidate)| ordering_key(candidate))
        .map(|(idx, _)| idx)
}

#[cfg(test)]
mod tests {
    use super::{best_equality_candidate_index, EqualityCandidate, EqualityCandidateKind};

    #[test]
    fn prefers_highest_distinct_count() {
        let candidates = vec![
            EqualityCandidate {
                filter_idx: 0,
                distinct_key_count: 5,
                kind: EqualityCandidateKind::SecondaryIndex,
                payload: "aircraft_type",
            },
            EqualityCandidate {
                filter_idx: 1,
                distinct_key_count: 10_000,
                kind: EqualityCandidateKind::SecondaryIndex,
                payload: "flight_id",
            },
        ];
        let idx = best_equality_candidate_index(&candidates).expect("missing candidate");
        assert_eq!(candidates[idx].payload, "flight_id");
    }

    #[test]
    fn tie_breaks_to_primary_key_then_filter_order() {
        let candidates = vec![
            EqualityCandidate {
                filter_idx: 3,
                distinct_key_count: 200,
                kind: EqualityCandidateKind::SecondaryIndex,
                payload: "dest",
            },
            EqualityCandidate {
                filter_idx: 5,
                distinct_key_count: 200,
                kind: EqualityCandidateKind::PrimaryKey,
                payload: "flight_id_pk",
            },
            EqualityCandidate {
                filter_idx: 1,
                distinct_key_count: 200,
                kind: EqualityCandidateKind::PrimaryKey,
                payload: "flight_id_pk_earlier",
            },
        ];
        let idx = best_equality_candidate_index(&candidates).expect("missing candidate");
        assert_eq!(candidates[idx].payload, "flight_id_pk_earlier");
    }
}
