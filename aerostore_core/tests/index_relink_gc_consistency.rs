use std::collections::{BTreeMap, BTreeSet};

use aerostore_core::{IndexCompare, IndexValue, SecondaryIndex};

fn model_insert(model: &mut BTreeMap<i64, BTreeSet<u32>>, key: i64, row_id: u32) {
    model.entry(key).or_default().insert(row_id);
}

fn model_remove(model: &mut BTreeMap<i64, BTreeSet<u32>>, key: i64, row_id: u32) {
    if let Some(rows) = model.get_mut(&key) {
        rows.remove(&row_id);
        if rows.is_empty() {
            model.remove(&key);
        }
    }
}

fn model_move(model: &mut BTreeMap<i64, BTreeSet<u32>>, from: i64, to: i64, row_id: u32) {
    if from == to {
        model_insert(model, to, row_id);
        return;
    }
    let existed = model
        .get(&from)
        .map(|rows| rows.contains(&row_id))
        .unwrap_or(false);
    if existed {
        model_remove(model, from, row_id);
    }
    model_insert(model, to, row_id);
}

fn lookup_eq(index: &SecondaryIndex<u32>, key: i64) -> Vec<u32> {
    index.lookup(&IndexCompare::Eq(IndexValue::I64(key)))
}

#[test]
fn relink_and_gc_preserve_model_parity_under_churned_moves() {
    let index: SecondaryIndex<u32> = SecondaryIndex::new("event_ts");
    let mut model: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();

    let inserts = [(100_i64, 1_u32), (100, 2), (101, 3), (102, 4), (103, 5)];
    for (key, row_id) in inserts {
        index
            .try_insert(IndexValue::I64(key), row_id)
            .expect("insert");
        model_insert(&mut model, key, row_id);
    }

    index
        .try_move_payload(&IndexValue::I64(100), IndexValue::I64(101), &1)
        .expect("move 100->101 row 1");
    model_move(&mut model, 100, 101, 1);

    index
        .try_move_payload(&IndexValue::I64(100), IndexValue::I64(102), &2)
        .expect("move 100->102 row 2");
    model_move(&mut model, 100, 102, 2);

    index
        .try_move_payload(&IndexValue::I64(101), IndexValue::I64(103), &3)
        .expect("move 101->103 row 3");
    model_move(&mut model, 101, 103, 3);

    index
        .try_remove(&IndexValue::I64(103), &5)
        .expect("remove row 5 from 103");
    model_remove(&mut model, 103, 5);

    // Old key missing: move API should still ensure target posting exists.
    index
        .try_move_payload(&IndexValue::I64(999), IndexValue::I64(101), &9)
        .expect("move missing old key should upsert target");
    model_move(&mut model, 999, 101, 9);

    let _ = index.collect_garbage_once(usize::MAX);
    let _ = index.collect_garbage_once(usize::MAX);

    let mut keys = BTreeSet::new();
    keys.extend(model.keys().copied());
    keys.extend([100_i64, 101, 102, 103, 999]);

    for key in keys {
        let actual = lookup_eq(&index, key);
        let expected = model
            .get(&key)
            .map(|set| set.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        assert_eq!(
            actual, expected,
            "index/model mismatch for key {} after relink+gc churn",
            key
        );
        assert_eq!(
            index.lookup_posting_count(&IndexValue::I64(key)),
            expected.len(),
            "posting count mismatch for key {}",
            key
        );
    }
}
