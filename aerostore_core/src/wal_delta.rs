use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord {
    UpdateFull {
        pk: String,
        payload: Vec<u8>,
    },
    UpdateDelta {
        pk: String,
        dirty_mask: u64,
        delta_bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalDeltaError {
    Codec(String),
    InvalidDeltaLength {
        expected_min: usize,
        expected_max: usize,
        actual: usize,
    },
}

impl fmt::Display for WalDeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalDeltaError::Codec(msg) => write!(f, "delta codec error: {}", msg),
            WalDeltaError::InvalidDeltaLength {
                expected_min,
                expected_max,
                actual,
            } => write!(
                f,
                "delta payload length mismatch (expected between {} and {}, actual {})",
                expected_min, expected_max, actual
            ),
        }
    }
}

impl std::error::Error for WalDeltaError {}

pub trait WalDeltaCodec: Clone + Serialize + DeserializeOwned {
    const COLUMN_COUNT: u8 = 64;

    fn wal_primary_key(row_id_hint: usize, _value: &Self) -> String {
        row_id_hint.to_string()
    }

    fn compute_dirty_mask(base_value: &Self, new_value: &Self) -> Result<u64, WalDeltaError> {
        compute_dirty_mask_for_values(base_value, new_value)
    }

    fn encode_changed_fields(
        value: &Self,
        dirty_mask: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), WalDeltaError> {
        let bytes =
            bincode::serialize(value).map_err(|err| WalDeltaError::Codec(err.to_string()))?;
        out.extend_from_slice(pack_delta_bytes(bytes.as_slice(), dirty_mask).as_slice());
        Ok(())
    }

    fn apply_changed_fields(
        base_value: &mut Self,
        dirty_mask: u64,
        delta_bytes: &[u8],
    ) -> Result<(), WalDeltaError> {
        let base_bytes =
            bincode::serialize(base_value).map_err(|err| WalDeltaError::Codec(err.to_string()))?;
        let overlaid = apply_delta_bytes(base_bytes.as_slice(), dirty_mask, delta_bytes)?;
        *base_value = bincode::deserialize(overlaid.as_slice())
            .map_err(|err| WalDeltaError::Codec(err.to_string()))?;
        Ok(())
    }
}

macro_rules! impl_scalar_wal_delta_codec {
    ($ty:ty) => {
        impl WalDeltaCodec for $ty {
            const COLUMN_COUNT: u8 = 1;

            fn compute_dirty_mask(
                base_value: &Self,
                new_value: &Self,
            ) -> Result<u64, WalDeltaError> {
                Ok(if base_value != new_value { 1 } else { 0 })
            }

            fn encode_changed_fields(
                value: &Self,
                dirty_mask: u64,
                out: &mut Vec<u8>,
            ) -> Result<(), WalDeltaError> {
                if dirty_mask & 1 != 0 {
                    out.extend_from_slice(value.to_le_bytes().as_slice());
                }
                Ok(())
            }

            fn apply_changed_fields(
                base_value: &mut Self,
                dirty_mask: u64,
                delta_bytes: &[u8],
            ) -> Result<(), WalDeltaError> {
                if dirty_mask & 1 == 0 {
                    if delta_bytes.is_empty() {
                        return Ok(());
                    }
                    return Err(WalDeltaError::InvalidDeltaLength {
                        expected_min: 0,
                        expected_max: 0,
                        actual: delta_bytes.len(),
                    });
                }
                if delta_bytes.len() != std::mem::size_of::<$ty>() {
                    return Err(WalDeltaError::InvalidDeltaLength {
                        expected_min: std::mem::size_of::<$ty>(),
                        expected_max: std::mem::size_of::<$ty>(),
                        actual: delta_bytes.len(),
                    });
                }
                let mut arr = [0_u8; std::mem::size_of::<$ty>()];
                arr.copy_from_slice(delta_bytes);
                *base_value = <$ty>::from_le_bytes(arr);
                Ok(())
            }
        }
    };
}

impl_scalar_wal_delta_codec!(u64);
impl_scalar_wal_delta_codec!(i32);
impl_scalar_wal_delta_codec!(u16);
impl_scalar_wal_delta_codec!(u8);

impl WalDeltaCodec for bool {
    const COLUMN_COUNT: u8 = 1;

    fn compute_dirty_mask(base_value: &Self, new_value: &Self) -> Result<u64, WalDeltaError> {
        Ok(if base_value != new_value { 1 } else { 0 })
    }

    fn encode_changed_fields(
        value: &Self,
        dirty_mask: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), WalDeltaError> {
        if dirty_mask & 1 != 0 {
            out.push(u8::from(*value));
        }
        Ok(())
    }

    fn apply_changed_fields(
        base_value: &mut Self,
        dirty_mask: u64,
        delta_bytes: &[u8],
    ) -> Result<(), WalDeltaError> {
        if dirty_mask & 1 == 0 {
            if delta_bytes.is_empty() {
                return Ok(());
            }
            return Err(WalDeltaError::InvalidDeltaLength {
                expected_min: 0,
                expected_max: 0,
                actual: delta_bytes.len(),
            });
        }
        if delta_bytes.len() != 1 {
            return Err(WalDeltaError::InvalidDeltaLength {
                expected_min: 1,
                expected_max: 1,
                actual: delta_bytes.len(),
            });
        }
        *base_value = delta_bytes[0] != 0;
        Ok(())
    }
}

#[inline]
pub fn serialize_wal_record(record: &WalRecord) -> Result<Vec<u8>, WalDeltaError> {
    bincode::serialize(record).map_err(|err| WalDeltaError::Codec(err.to_string()))
}

#[inline]
pub fn deserialize_wal_record(payload: &[u8]) -> Result<WalRecord, WalDeltaError> {
    bincode::deserialize(payload).map_err(|err| WalDeltaError::Codec(err.to_string()))
}

pub fn build_update_record<T: WalDeltaCodec>(
    pk: String,
    base_value: &T,
    new_value: &T,
    _dirty_mask_hint: u64,
) -> Result<WalRecord, WalDeltaError> {
    let base_bytes =
        bincode::serialize(base_value).map_err(|err| WalDeltaError::Codec(err.to_string()))?;
    let new_bytes =
        bincode::serialize(new_value).map_err(|err| WalDeltaError::Codec(err.to_string()))?;

    // Variable-length encodings cannot be safely overlaid by position.
    if base_bytes.len() != new_bytes.len() {
        return Ok(WalRecord::UpdateFull {
            pk,
            payload: new_bytes,
        });
    }

    // Dirty mask is derived from the codec's semantic diff only.
    // The caller hint remains accepted for API compatibility but does not
    // influence emitted delta masks.
    let computed_mask = T::compute_dirty_mask(base_value, new_value)?;
    let column_count = usize::from(T::COLUMN_COUNT).clamp(1, 64);
    let dirty_mask = normalize_dirty_mask(column_count, computed_mask);
    let mut delta_bytes = Vec::new();
    T::encode_changed_fields(new_value, dirty_mask, &mut delta_bytes)?;

    // Guard against buggy codec implementations. If applying the encoded delta
    // to the base cannot reconstruct the new value exactly, degrade to full-row WAL.
    let mut reconstructed = base_value.clone();
    match T::apply_changed_fields(&mut reconstructed, dirty_mask, delta_bytes.as_slice()) {
        Ok(()) => match bincode::serialize(&reconstructed) {
            Ok(reconstructed_bytes) if reconstructed_bytes == new_bytes => {}
            _ => {
                return Ok(WalRecord::UpdateFull {
                    pk,
                    payload: new_bytes,
                });
            }
        },
        Err(_) => {
            return Ok(WalRecord::UpdateFull {
                pk,
                payload: new_bytes,
            });
        }
    }

    Ok(WalRecord::UpdateDelta {
        pk,
        dirty_mask,
        delta_bytes,
    })
}

pub fn apply_update_record<T>(
    base_value: &T,
    record: &WalRecord,
) -> Result<(String, T), WalDeltaError>
where
    T: WalDeltaCodec,
{
    match record {
        WalRecord::UpdateFull { pk, payload } => {
            let value = bincode::deserialize(payload.as_slice())
                .map_err(|err| WalDeltaError::Codec(err.to_string()))?;
            Ok((pk.clone(), value))
        }
        WalRecord::UpdateDelta {
            pk,
            dirty_mask,
            delta_bytes,
        } => {
            let mut value = base_value.clone();
            T::apply_changed_fields(&mut value, *dirty_mask, delta_bytes)?;
            Ok((pk.clone(), value))
        }
    }
}

pub fn compute_dirty_mask_for_values<T: Serialize>(
    base_value: &T,
    new_value: &T,
) -> Result<u64, WalDeltaError> {
    let base_bytes =
        bincode::serialize(base_value).map_err(|err| WalDeltaError::Codec(err.to_string()))?;
    let new_bytes =
        bincode::serialize(new_value).map_err(|err| WalDeltaError::Codec(err.to_string()))?;
    if base_bytes.len() != new_bytes.len() {
        return Ok(u64::MAX);
    }
    Ok(compute_dirty_mask(
        base_bytes.as_slice(),
        new_bytes.as_slice(),
    ))
}

pub fn compute_dirty_mask(base_bytes: &[u8], new_bytes: &[u8]) -> u64 {
    if base_bytes.len() != new_bytes.len() {
        return u64::MAX;
    }
    if base_bytes.is_empty() {
        return 0;
    }

    let mut mask = 0_u64;
    let segs = segment_count(base_bytes.len());
    for idx in 0..segs {
        let (start, end) = segment_bounds(base_bytes.len(), segs, idx);
        if base_bytes[start..end] != new_bytes[start..end] {
            mask |= 1_u64 << idx;
        }
    }
    mask
}

pub fn pack_delta_bytes(new_bytes: &[u8], dirty_mask: u64) -> Vec<u8> {
    if new_bytes.is_empty() || dirty_mask == 0 {
        return Vec::new();
    }

    let segs = segment_count(new_bytes.len());
    let mut out = Vec::new();
    for idx in 0..segs {
        if dirty_mask & (1_u64 << idx) == 0 {
            continue;
        }
        let (start, end) = segment_bounds(new_bytes.len(), segs, idx);
        out.extend_from_slice(&new_bytes[start..end]);
    }
    out
}

pub fn apply_delta_bytes(
    base_bytes: &[u8],
    dirty_mask: u64,
    delta_bytes: &[u8],
) -> Result<Vec<u8>, WalDeltaError> {
    if base_bytes.is_empty() {
        if delta_bytes.is_empty() {
            return Ok(Vec::new());
        }
        return Err(WalDeltaError::InvalidDeltaLength {
            expected_min: 0,
            expected_max: 0,
            actual: delta_bytes.len(),
        });
    }

    let segs = segment_count(base_bytes.len());
    let mut out = base_bytes.to_vec();
    let mut cursor = 0_usize;
    let mut expected = 0_usize;

    for idx in 0..segs {
        if dirty_mask & (1_u64 << idx) == 0 {
            continue;
        }
        let (start, end) = segment_bounds(base_bytes.len(), segs, idx);
        let len = end - start;
        expected += len;
        if cursor + len > delta_bytes.len() {
            return Err(WalDeltaError::InvalidDeltaLength {
                expected_min: expected,
                expected_max: expected,
                actual: delta_bytes.len(),
            });
        }
        out[start..end].copy_from_slice(&delta_bytes[cursor..cursor + len]);
        cursor += len;
    }

    if cursor != delta_bytes.len() {
        return Err(WalDeltaError::InvalidDeltaLength {
            expected_min: cursor,
            expected_max: cursor,
            actual: delta_bytes.len(),
        });
    }
    Ok(out)
}

pub fn coarse_dirty_mask_for_copy<T: Copy>(base_value: &T, new_value: &T) -> u64 {
    let size = std::mem::size_of::<T>();
    if size == 0 {
        return 0;
    }

    // SAFETY:
    // We only use this for coarse change detection. Reading raw byte views is valid
    // for initialized, Copy row payloads used by OCC.
    let base_bytes = unsafe {
        std::slice::from_raw_parts(
            (base_value as *const T).cast::<u8>(),
            std::mem::size_of::<T>(),
        )
    };
    // SAFETY:
    // Same rationale as above for `base_bytes`.
    let new_bytes = unsafe {
        std::slice::from_raw_parts(
            (new_value as *const T).cast::<u8>(),
            std::mem::size_of::<T>(),
        )
    };
    compute_dirty_mask(base_bytes, new_bytes)
}

#[inline]
fn segment_count(total_len: usize) -> usize {
    total_len.clamp(1, 64)
}

#[inline]
fn segment_bounds(total_len: usize, segment_count: usize, segment_idx: usize) -> (usize, usize) {
    let start = segment_idx * total_len / segment_count;
    let end = (segment_idx + 1) * total_len / segment_count;
    (start, end)
}

#[inline]
fn normalize_dirty_mask(segs: usize, dirty_mask: u64) -> u64 {
    if segs >= 64 {
        return dirty_mask;
    }
    dirty_mask & ((1_u64 << segs) - 1)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_delta_bytes, apply_update_record, build_update_record, compute_dirty_mask,
        pack_delta_bytes, WalRecord,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
    struct TinyRow {
        a: u64,
        b: u64,
        c: u64,
        d: u64,
    }

    impl super::WalDeltaCodec for TinyRow {}

    #[test]
    fn computes_sparse_mask_and_round_trips_delta_overlay() {
        let base = TinyRow {
            a: 10,
            b: 20,
            c: 30,
            d: 40,
        };
        let updated = TinyRow {
            a: 10,
            b: 21,
            c: 30,
            d: 99,
        };

        let base_bytes = bincode::serialize(&base).expect("serialize base failed");
        let updated_bytes = bincode::serialize(&updated).expect("serialize updated failed");
        let mask = compute_dirty_mask(base_bytes.as_slice(), updated_bytes.as_slice());
        assert_ne!(mask, 0);

        let delta = pack_delta_bytes(updated_bytes.as_slice(), mask);
        assert!(!delta.is_empty());

        let overlaid = apply_delta_bytes(base_bytes.as_slice(), mask, delta.as_slice())
            .expect("overlay failed");
        let decoded: TinyRow = bincode::deserialize(overlaid.as_slice()).expect("decode failed");
        assert_eq!(decoded, updated);
    }

    #[test]
    fn build_and_apply_update_record_round_trips() {
        let base = TinyRow {
            a: 1,
            b: 2,
            c: 3,
            d: 4,
        };
        let updated = TinyRow {
            a: 1,
            b: 2,
            c: 99,
            d: 4,
        };

        let record =
            build_update_record("flight-42".to_string(), &base, &updated, 0).expect("build failed");
        match &record {
            WalRecord::UpdateDelta { dirty_mask, .. } => assert_ne!(*dirty_mask, 0),
            WalRecord::UpdateFull { .. } => panic!("fixed-size row should use UpdateDelta"),
        }

        let (pk, replayed) =
            apply_update_record(&base, &record).expect("apply_update_record should succeed");
        assert_eq!(pk, "flight-42");
        assert_eq!(replayed, updated);
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct VariableRow {
        payload: Vec<u8>,
    }

    impl super::WalDeltaCodec for VariableRow {}

    #[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
    struct SemanticMaskRow {
        altitude: i32,
        groundspeed: i32,
    }

    impl SemanticMaskRow {
        const MASK_ALTITUDE: u64 = 1_u64 << 0;
        const MASK_GROUNDSPEED: u64 = 1_u64 << 1;
    }

    impl super::WalDeltaCodec for SemanticMaskRow {
        const COLUMN_COUNT: u8 = 2;

        fn compute_dirty_mask(
            base_value: &Self,
            new_value: &Self,
        ) -> Result<u64, super::WalDeltaError> {
            let mut mask = 0_u64;
            if base_value.altitude != new_value.altitude {
                mask |= Self::MASK_ALTITUDE;
            }
            if base_value.groundspeed != new_value.groundspeed {
                mask |= Self::MASK_GROUNDSPEED;
            }
            Ok(mask)
        }

        fn encode_changed_fields(
            value: &Self,
            dirty_mask: u64,
            out: &mut Vec<u8>,
        ) -> Result<(), super::WalDeltaError> {
            if dirty_mask & Self::MASK_ALTITUDE != 0 {
                out.extend_from_slice(&value.altitude.to_le_bytes());
            }
            if dirty_mask & Self::MASK_GROUNDSPEED != 0 {
                out.extend_from_slice(&value.groundspeed.to_le_bytes());
            }
            Ok(())
        }

        fn apply_changed_fields(
            base_value: &mut Self,
            dirty_mask: u64,
            delta_bytes: &[u8],
        ) -> Result<(), super::WalDeltaError> {
            let mut cursor = 0_usize;
            let mut take = |len: usize| -> Result<&[u8], super::WalDeltaError> {
                if cursor + len > delta_bytes.len() {
                    return Err(super::WalDeltaError::InvalidDeltaLength {
                        expected_min: cursor + len,
                        expected_max: cursor + len,
                        actual: delta_bytes.len(),
                    });
                }
                let out = &delta_bytes[cursor..cursor + len];
                cursor += len;
                Ok(out)
            };

            if dirty_mask & Self::MASK_ALTITUDE != 0 {
                let mut bytes = [0_u8; 4];
                bytes.copy_from_slice(take(4)?);
                base_value.altitude = i32::from_le_bytes(bytes);
            }
            if dirty_mask & Self::MASK_GROUNDSPEED != 0 {
                let mut bytes = [0_u8; 4];
                bytes.copy_from_slice(take(4)?);
                base_value.groundspeed = i32::from_le_bytes(bytes);
            }
            if cursor != delta_bytes.len() {
                return Err(super::WalDeltaError::InvalidDeltaLength {
                    expected_min: cursor,
                    expected_max: cursor,
                    actual: delta_bytes.len(),
                });
            }
            Ok(())
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
    struct FaultyCodecRow {
        altitude: i32,
        groundspeed: i32,
    }

    impl super::WalDeltaCodec for FaultyCodecRow {
        const COLUMN_COUNT: u8 = 2;

        fn compute_dirty_mask(
            _base_value: &Self,
            _new_value: &Self,
        ) -> Result<u64, super::WalDeltaError> {
            // Deliberately incorrect: claims only altitude changed.
            Ok(1)
        }

        fn encode_changed_fields(
            value: &Self,
            _dirty_mask: u64,
            out: &mut Vec<u8>,
        ) -> Result<(), super::WalDeltaError> {
            out.extend_from_slice(&value.altitude.to_le_bytes());
            Ok(())
        }

        fn apply_changed_fields(
            base_value: &mut Self,
            _dirty_mask: u64,
            delta_bytes: &[u8],
        ) -> Result<(), super::WalDeltaError> {
            if delta_bytes.len() != 4 {
                return Err(super::WalDeltaError::InvalidDeltaLength {
                    expected_min: 4,
                    expected_max: 4,
                    actual: delta_bytes.len(),
                });
            }
            let mut bytes = [0_u8; 4];
            bytes.copy_from_slice(delta_bytes);
            base_value.altitude = i32::from_le_bytes(bytes);
            Ok(())
        }
    }

    #[test]
    fn falls_back_to_full_for_variable_length_rows() {
        let base = VariableRow {
            payload: vec![1, 2, 3],
        };
        let updated = VariableRow {
            payload: vec![1, 2, 3, 4, 5],
        };
        let record =
            build_update_record("pk".to_string(), &base, &updated, 0).expect("build failed");

        match record {
            WalRecord::UpdateFull { .. } => {}
            WalRecord::UpdateDelta { .. } => {
                panic!("variable-length rows must use full record fallback")
            }
        }
    }

    #[test]
    fn dirty_mask_hint_does_not_inflate_semantic_codec_mask() {
        let base = SemanticMaskRow {
            altitude: 30_000,
            groundspeed: 450,
        };
        let updated = SemanticMaskRow {
            altitude: 31_250,
            groundspeed: 450,
        };

        let record = build_update_record("UAL123".to_string(), &base, &updated, u64::MAX)
            .expect("build_update_record failed");
        match record {
            WalRecord::UpdateDelta {
                dirty_mask,
                delta_bytes,
                ..
            } => {
                assert_eq!(dirty_mask, SemanticMaskRow::MASK_ALTITUDE);
                assert_eq!(delta_bytes.len(), 4);
            }
            WalRecord::UpdateFull { .. } => panic!("fixed-size semantic row should emit delta"),
        }
    }

    #[test]
    fn faulty_codec_degrades_to_full_row_record() {
        let base = FaultyCodecRow {
            altitude: 30_000,
            groundspeed: 450,
        };
        let updated = FaultyCodecRow {
            altitude: 30_000,
            groundspeed: 455,
        };

        let record = build_update_record("UAL123".to_string(), &base, &updated, 0)
            .expect("build_update_record failed");
        match record {
            WalRecord::UpdateFull { payload, .. } => {
                let decoded: FaultyCodecRow =
                    bincode::deserialize(payload.as_slice()).expect("decode full payload failed");
                assert_eq!(decoded, updated);
            }
            WalRecord::UpdateDelta { .. } => {
                panic!("faulty codec mismatch must fall back to UpdateFull")
            }
        }
    }

    #[test]
    fn rejects_malformed_delta_when_payload_is_too_short() {
        let base = 41_i32;
        let record = WalRecord::UpdateDelta {
            pk: "pk".to_string(),
            dirty_mask: 1,
            delta_bytes: vec![1_u8, 2_u8, 3_u8],
        };

        let err = apply_update_record(&base, &record).expect_err("short payload must fail");
        match err {
            super::WalDeltaError::InvalidDeltaLength {
                expected_min,
                expected_max,
                actual,
            } => {
                assert_eq!(expected_min, std::mem::size_of::<i32>());
                assert_eq!(expected_max, std::mem::size_of::<i32>());
                assert_eq!(actual, 3);
            }
            other => panic!("expected InvalidDeltaLength, got {other:?}"),
        }
    }

    #[test]
    fn rejects_malformed_delta_when_mask_is_clean_but_payload_is_not_empty() {
        let base = true;
        let record = WalRecord::UpdateDelta {
            pk: "pk".to_string(),
            dirty_mask: 0,
            delta_bytes: vec![1_u8],
        };

        let err = apply_update_record(&base, &record).expect_err("unexpected payload must fail");
        match err {
            super::WalDeltaError::InvalidDeltaLength {
                expected_min,
                expected_max,
                actual,
            } => {
                assert_eq!(expected_min, 0);
                assert_eq!(expected_max, 0);
                assert_eq!(actual, 1);
            }
            other => panic!("expected InvalidDeltaLength, got {other:?}"),
        }
    }

    #[test]
    fn rejects_malformed_delta_when_payload_has_extra_bytes() {
        let base = 5_u16;
        let record = WalRecord::UpdateDelta {
            pk: "pk".to_string(),
            dirty_mask: 1,
            delta_bytes: vec![0_u8, 1_u8, 2_u8],
        };

        let err = apply_update_record(&base, &record).expect_err("extra payload bytes must fail");
        match err {
            super::WalDeltaError::InvalidDeltaLength {
                expected_min,
                expected_max,
                actual,
            } => {
                assert_eq!(expected_min, std::mem::size_of::<u16>());
                assert_eq!(expected_max, std::mem::size_of::<u16>());
                assert_eq!(actual, 3);
            }
            other => panic!("expected InvalidDeltaLength, got {other:?}"),
        }
    }
}
