use std::fmt;

use crate::execution::PrimaryKeyMapError;
use crate::occ::{Error as OccError, OccTable};
use crate::wal_delta::{apply_update_record, WalDeltaCodec, WalDeltaError, WalRecord};
use crate::ShmPrimaryKeyMap;
use crate::TxId;

#[derive(Debug)]
pub enum RecoveryDeltaError {
    Occ(OccError),
    Delta(WalDeltaError),
    MissingBaseRow { row_id: usize },
    InvalidPrimaryKey { pk: String },
    PrimaryKeyMap(String),
}

impl fmt::Display for RecoveryDeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryDeltaError::Occ(err) => write!(f, "occ error: {}", err),
            RecoveryDeltaError::Delta(err) => write!(f, "delta error: {}", err),
            RecoveryDeltaError::MissingBaseRow { row_id } => {
                write!(f, "missing base row for replayed delta row_id={}", row_id)
            }
            RecoveryDeltaError::InvalidPrimaryKey { pk } => {
                write!(f, "delta replay could not parse primary key '{}'", pk)
            }
            RecoveryDeltaError::PrimaryKeyMap(msg) => {
                write!(f, "primary key map error: {}", msg)
            }
        }
    }
}

impl std::error::Error for RecoveryDeltaError {}

impl From<OccError> for RecoveryDeltaError {
    fn from(value: OccError) -> Self {
        RecoveryDeltaError::Occ(value)
    }
}

impl From<WalDeltaError> for RecoveryDeltaError {
    fn from(value: WalDeltaError) -> Self {
        RecoveryDeltaError::Delta(value)
    }
}

impl From<PrimaryKeyMapError> for RecoveryDeltaError {
    fn from(value: PrimaryKeyMapError) -> Self {
        RecoveryDeltaError::PrimaryKeyMap(value.to_string())
    }
}

pub fn replay_update_record<T>(
    table: &OccTable<T>,
    txid: TxId,
    fallback_row_id: usize,
    record: &WalRecord,
) -> Result<usize, RecoveryDeltaError>
where
    T: WalDeltaCodec + Copy + Send + Sync + 'static,
{
    match record {
        WalRecord::UpdateFull { pk, payload } => {
            let row_id = parse_pk_row_id(pk.as_str(), fallback_row_id)?;
            let value = bincode::deserialize(payload.as_slice())
                .map_err(|err| RecoveryDeltaError::Delta(WalDeltaError::Codec(err.to_string())))?;
            table.apply_recovered_write(row_id, txid, value)?;
            Ok(row_id)
        }
        WalRecord::UpdateDelta { pk, .. } => {
            let row_id = parse_pk_row_id(pk.as_str(), fallback_row_id)?;
            let Some(base_value) = table.latest_value(row_id)? else {
                return Err(RecoveryDeltaError::MissingBaseRow { row_id });
            };
            let (_, value) = apply_update_record(&base_value, record)?;
            table.apply_recovered_write(row_id, txid, value)?;
            Ok(row_id)
        }
    }
}

pub fn replay_update_record_with_pk_map<T>(
    table: &OccTable<T>,
    pk_map: &ShmPrimaryKeyMap,
    txid: TxId,
    record: &WalRecord,
) -> Result<usize, RecoveryDeltaError>
where
    T: WalDeltaCodec + Copy + Send + Sync + 'static,
{
    let pk = match record {
        WalRecord::UpdateFull { pk, .. } | WalRecord::UpdateDelta { pk, .. } => pk,
    };
    if pk.is_empty() {
        return Err(RecoveryDeltaError::InvalidPrimaryKey { pk: pk.clone() });
    }
    let row_id = pk_map.get_or_insert(pk.as_str())?;
    let base_offset = table.row_head_offset(row_id)?;

    match record {
        WalRecord::UpdateFull { payload, .. } => {
            let value = bincode::deserialize(payload.as_slice())
                .map_err(|err| RecoveryDeltaError::Delta(WalDeltaError::Codec(err.to_string())))?;
            table.apply_recovered_write_cas(row_id, txid, base_offset, value)?;
            Ok(row_id)
        }
        WalRecord::UpdateDelta { .. } => {
            let Some(base_value) = table.latest_value(row_id)? else {
                return Err(RecoveryDeltaError::MissingBaseRow { row_id });
            };
            let (_, value) = apply_update_record(&base_value, record)?;
            table.apply_recovered_write_cas(row_id, txid, base_offset, value)?;
            Ok(row_id)
        }
    }
}

#[inline]
fn parse_pk_row_id(pk: &str, fallback_row_id: usize) -> Result<usize, RecoveryDeltaError> {
    if pk.is_empty() {
        return Ok(fallback_row_id);
    }
    match pk.parse::<usize>() {
        Ok(row_id) => Ok(row_id),
        Err(_) => Err(RecoveryDeltaError::InvalidPrimaryKey { pk: pk.to_string() }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::replay_update_record;
    use crate::occ::OccTable;
    use crate::shm::ShmArena;
    use crate::wal_delta::{build_update_record, WalRecord};

    #[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
    struct ReplayRow {
        flight: u64,
        altitude: i64,
        heading: i64,
    }

    impl crate::wal_delta::WalDeltaCodec for ReplayRow {}

    #[test]
    fn replay_update_delta_overlays_only_changed_segments() {
        let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shm arena"));
        let table = OccTable::<ReplayRow>::new(Arc::clone(&shm), 1).expect("create table failed");
        let base = ReplayRow {
            flight: 42,
            altitude: 10_000,
            heading: 90,
        };
        table.seed_row(0, base).expect("seed_row failed");

        let updated = ReplayRow {
            flight: 42,
            altitude: 11_500,
            heading: 90,
        };
        let record =
            build_update_record("0".to_string(), &base, &updated, 0).expect("build delta failed");
        match record {
            WalRecord::UpdateDelta { .. } => {}
            WalRecord::UpdateFull { .. } => panic!("fixed-size row should emit UpdateDelta"),
        }

        replay_update_record(&table, 99, 0, &record).expect("replay_update_record failed");
        let replayed = table
            .latest_value(0)
            .expect("latest_value failed")
            .expect("row missing after replay");
        assert_eq!(replayed, updated);
    }
}
