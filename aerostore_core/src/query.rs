use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use crossbeam::epoch::{self, Guard};

use crate::index::{IndexCompare, IndexValue, IntoIndexValue, SecondaryIndex};
use crate::mvcc::{MvccError, MvccTable};
use crate::txn::{Transaction, TransactionManager};

type FilterPredicate<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;
type SortComparator<T> = Arc<dyn Fn(&T, &T) -> Ordering + Send + Sync>;
type IndexExtractor<T> = Arc<dyn Fn(&T) -> IndexValue + Send + Sync>;

#[derive(Clone, Copy)]
pub struct Field<T, V> {
    pub name: &'static str,
    accessor: fn(&T) -> V,
}

impl<T, V> Field<T, V> {
    pub fn new(name: &'static str, accessor: fn(&T) -> V) -> Self {
        Self { name, accessor }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

struct Filter<T> {
    field: &'static str,
    index_compare: Option<IndexCompare>,
    matches: FilterPredicate<T>,
}

struct SortSpec<T> {
    direction: SortDirection,
    compare: SortComparator<T>,
}

pub struct QueryEngine<K, V, S = std::collections::hash_map::RandomState>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + 'static,
{
    table: Arc<MvccTable<K, V, S>>,
    indexes: HashMap<&'static str, Arc<SecondaryIndex<K>>>,
    extractors: HashMap<&'static str, IndexExtractor<V>>,
}

impl<K, V, S> QueryEngine<K, V, S>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn new(table: Arc<MvccTable<K, V, S>>) -> Self {
        Self {
            table,
            indexes: HashMap::new(),
            extractors: HashMap::new(),
        }
    }

    pub fn create_index<F>(&mut self, field: &'static str, extractor: fn(&V) -> F)
    where
        F: IntoIndexValue + Send + Sync + 'static,
    {
        let extractor: IndexExtractor<V> = Arc::new(move |row| extractor(row).into_index_value());
        self.create_index_with_extractor(field, extractor);
    }

    pub fn create_index_with_extractor(
        &mut self,
        field: &'static str,
        extractor: Arc<dyn Fn(&V) -> IndexValue + Send + Sync>,
    ) {
        self.indexes
            .insert(field, Arc::new(SecondaryIndex::<K>::new(field)));
        self.extractors.insert(field, extractor);
    }

    pub fn insert(&self, key: K, value: V, tx: &Transaction) -> Result<(), MvccError> {
        let indexed_values: Vec<(Arc<SecondaryIndex<K>>, IndexValue)> = self
            .indexes
            .iter()
            .filter_map(|(field, index)| {
                self.extractors
                    .get(field)
                    .map(|extractor| (Arc::clone(index), extractor(&value)))
            })
            .collect();

        self.table.insert(key.clone(), value, tx)?;

        for (index, indexed_value) in indexed_values {
            index.insert(indexed_value, key.clone());
        }

        Ok(())
    }

    pub fn update(&self, key: &K, value: V, tx: &Transaction) -> Result<(), MvccError>
    where
        V: Clone,
    {
        let before = self.table.read_visible(key, tx);
        self.table.update(key, value, tx)?;
        let after = self.table.read_visible(key, tx);

        if let Some(prev) = before.as_ref() {
            self.apply_indexes_for_remove(key, prev);
        }
        if let Some(next) = after.as_ref() {
            self.apply_indexes_for_insert(key, next);
        }

        Ok(())
    }

    pub fn delete(&self, key: &K, tx: &Transaction) -> Result<(), MvccError>
    where
        V: Clone,
    {
        let before = self.table.read_visible(key, tx);
        self.table.delete(key, tx)?;

        if let Some(prev) = before.as_ref() {
            self.apply_indexes_for_remove(key, prev);
        }

        Ok(())
    }

    pub fn commit(&self, tx_manager: &TransactionManager, tx: &Transaction) -> usize {
        self.table.commit(tx_manager, tx)
    }

    pub fn read_visible(&self, key: &K, tx: &Transaction) -> Option<V>
    where
        V: Clone,
    {
        self.table.read_visible(key, tx)
    }

    pub fn snapshot_rows(&self, tx: &Transaction) -> Vec<(K, V)>
    where
        V: Clone,
    {
        let keys = self.table.all_keys();
        let mut rows = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(value) = self.table.read_visible(&key, tx) {
                rows.push((key, value));
            }
        }

        rows
    }

    pub fn query(&self) -> QueryBuilder<'_, K, V, S> {
        QueryBuilder {
            engine: self,
            filters: Vec::new(),
            sort: None,
            limit: None,
            offset: 0,
        }
    }

    fn apply_indexes_for_insert(&self, key: &K, value: &V) {
        for (field, index) in &self.indexes {
            if let Some(extractor) = self.extractors.get(field) {
                let indexed_value = extractor(value);
                index.insert(indexed_value, key.clone());
            }
        }
    }

    fn apply_indexes_for_remove(&self, key: &K, value: &V) {
        for (field, index) in &self.indexes {
            if let Some(extractor) = self.extractors.get(field) {
                let indexed_value = extractor(value);
                index.remove(&indexed_value, key);
            }
        }
    }
}

pub struct QueryBuilder<'a, K, V, S = std::collections::hash_map::RandomState>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + 'static,
{
    engine: &'a QueryEngine<K, V, S>,
    filters: Vec<Filter<V>>,
    sort: Option<SortSpec<V>>,
    limit: Option<usize>,
    offset: usize,
}

impl<'a, K, V, S> QueryBuilder<'a, K, V, S>
where
    K: Eq + Hash + Clone + Ord + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn eq<F>(mut self, field: Field<V, F>, value: F) -> Self
    where
        F: PartialEq + Clone + IntoIndexValue + Send + Sync + 'static,
    {
        let compare_value = value.clone();
        let accessor = field.accessor;
        let predicate: FilterPredicate<V> = Arc::new(move |row| accessor(row) == compare_value);

        self.filters.push(Filter {
            field: field.name,
            index_compare: Some(IndexCompare::Eq(value.into_index_value())),
            matches: predicate,
        });
        self
    }

    pub fn gt<F>(mut self, field: Field<V, F>, value: F) -> Self
    where
        F: PartialOrd + Clone + IntoIndexValue + Send + Sync + 'static,
    {
        let compare_value = value.clone();
        let accessor = field.accessor;
        let predicate: FilterPredicate<V> = Arc::new(move |row| accessor(row) > compare_value);

        self.filters.push(Filter {
            field: field.name,
            index_compare: Some(IndexCompare::Gt(value.into_index_value())),
            matches: predicate,
        });
        self
    }

    pub fn lt<F>(mut self, field: Field<V, F>, value: F) -> Self
    where
        F: PartialOrd + Clone + IntoIndexValue + Send + Sync + 'static,
    {
        let compare_value = value.clone();
        let accessor = field.accessor;
        let predicate: FilterPredicate<V> = Arc::new(move |row| accessor(row) < compare_value);

        self.filters.push(Filter {
            field: field.name,
            index_compare: Some(IndexCompare::Lt(value.into_index_value())),
            matches: predicate,
        });
        self
    }

    pub fn in_values<F>(mut self, field: Field<V, F>, values: Vec<F>) -> Self
    where
        F: PartialEq + Clone + IntoIndexValue + Send + Sync + 'static,
    {
        let accepted = values.clone();
        let accessor = field.accessor;
        let predicate: FilterPredicate<V> =
            Arc::new(move |row| accepted.iter().any(|v| *v == accessor(row)));

        self.filters.push(Filter {
            field: field.name,
            index_compare: Some(IndexCompare::In(
                values
                    .into_iter()
                    .map(IntoIndexValue::into_index_value)
                    .collect(),
            )),
            matches: predicate,
        });
        self
    }

    pub fn sort_by<F>(mut self, field: Field<V, F>, direction: SortDirection) -> Self
    where
        F: Ord + Send + Sync + 'static,
    {
        let accessor = field.accessor;
        let comparator: SortComparator<V> = Arc::new(move |left, right| {
            let l = accessor(left);
            let r = accessor(right);
            l.cmp(&r)
        });

        self.sort = Some(SortSpec {
            direction,
            compare: comparator,
        });
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    pub fn execute<'g>(&self, tx: &Transaction, guard: &'g Guard) -> Vec<&'g V> {
        let candidate_row_ids = self.select_candidates(tx);
        let mut visible_rows: Vec<&'g V> = Vec::new();

        for row_id in candidate_row_ids {
            let Some(row) = self
                .engine
                .table
                .read_visible_ref_with_guard(&row_id, tx, guard)
            else {
                continue;
            };

            if self.filters.iter().all(|filter| (filter.matches)(row)) {
                visible_rows.push(row);
            }
        }

        if let Some(sort_spec) = &self.sort {
            visible_rows.sort_unstable_by(|left, right| {
                let ord = (sort_spec.compare)(left, right);
                match sort_spec.direction {
                    SortDirection::Asc => ord,
                    SortDirection::Desc => ord.reverse(),
                }
            });
        }

        let start = self.offset.min(visible_rows.len());
        let end = self
            .limit
            .map(|limit| start.saturating_add(limit).min(visible_rows.len()))
            .unwrap_or(visible_rows.len());

        visible_rows[start..end].to_vec()
    }

    fn select_candidates(&self, tx: &Transaction) -> Vec<K> {
        for filter in &self.filters {
            let Some(index_compare) = filter.index_compare.as_ref() else {
                continue;
            };

            let Some(index) = self.engine.indexes.get(filter.field) else {
                continue;
            };

            return index.lookup(index_compare);
        }

        self.parallel_sequential_scan(tx)
    }

    fn parallel_sequential_scan(&self, tx: &Transaction) -> Vec<K> {
        let keys = self.engine.table.all_keys();
        if keys.is_empty() {
            return Vec::new();
        }

        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(16);
        let chunk_size = (keys.len() + workers - 1) / workers;

        let table = Arc::clone(&self.engine.table);
        let predicates: Arc<Vec<FilterPredicate<V>>> = Arc::new(
            self.filters
                .iter()
                .map(|f| Arc::clone(&f.matches))
                .collect(),
        );

        let mut matched_row_ids = Vec::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();

            for chunk in keys.chunks(chunk_size) {
                let table = Arc::clone(&table);
                let predicates = Arc::clone(&predicates);
                let row_ids = chunk.to_vec();

                handles.push(scope.spawn(move || {
                    let guard = epoch::pin();
                    let mut local_matches = Vec::new();

                    for row_id in row_ids {
                        let Some(row) = table.read_visible_ref_with_guard(&row_id, tx, &guard)
                        else {
                            continue;
                        };

                        if predicates.iter().all(|predicate| predicate(row)) {
                            local_matches.push(row_id);
                        }
                    }

                    local_matches
                }));
            }

            for handle in handles {
                matched_row_ids.extend(
                    handle
                        .join()
                        .expect("parallel sequential scan worker failed"),
                );
            }
        });

        matched_row_ids
    }
}
