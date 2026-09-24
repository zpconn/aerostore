// Source-bound MVCC selection and indexed candidate materialization.
// Heap validity, retained version projections, raw posting enumeration, and
// key extraction remain explicit storage obligations. No runtime additions.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub const EMPTY_PTR: u32 = 0;
pub const MAX_VISIBLE_CHAIN_STEPS: u32 = 262_144;
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error { SerializationFailure, Storage, RowOutOfBounds { row_id: usize, capacity: usize } }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Row { pub xmin: u64, pub xmax: u64, pub next: u32, pub value: Option<usize>, pub locked: bool, pub owner: u64 }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PendingWrite { pub row_id: usize, pub new_ptr: u32 }
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ReadRecord { pub row_id: usize, pub row_ptr: u32, pub xmin: u64 }
pub struct Transaction {
    pub txid: u64, pub snapshot_xmin: u64, pub snapshot_xmax: u64,
    pub snapshot_active: Vec<u64>, pub write_set: Vec<PendingWrite>, pub read_set: Vec<ReadRecord>,
}
pub struct Snapshot { pub txid:u64, pub xmin:u64, pub xmax:u64, pub active:Seq<u64> }
pub open spec fn snapshot(tx:Transaction) -> Snapshot {
    Snapshot {txid:tx.txid,xmin:tx.snapshot_xmin,xmax:tx.snapshot_xmax,active:tx.snapshot_active@}
}
pub struct Image {
    pub heads: Map<usize, u32>, pub rows: Map<u32, Row>, pub rank: Map<u32, nat>, pub capacity: usize,
}
pub open spec fn image_valid(s: Image) -> bool {
    (forall|id: usize| #![trigger s.heads.contains_key(id)] #![trigger s.heads[id]]
        id < s.capacity ==> s.heads.contains_key(id)
        && (s.heads[id] == 0 || s.rows.contains_key(s.heads[id])))
    && (forall|p: u32| #[trigger] s.rows.contains_key(p) ==> p != 0 && s.rank.contains_key(p)
        && (s.rows[p].next == 0 || s.rows.contains_key(s.rows[p].next)
            && s.rank[s.rows[p].next] < s.rank[p]))
}
pub open spec fn creator_visible(xmin: u64, tx: Snapshot) -> bool {
    xmin == tx.txid || xmin < tx.xmin
        || (xmin < tx.xmax && !tx.active.contains(xmin))
}
pub open spec fn visible(row: Row, tx: Snapshot) -> bool {
    row.xmin == tx.txid || (creator_visible(row.xmin, tx)
        && (row.xmax == 0 || (row.xmax != tx.txid
            && (row.xmax >= tx.xmax || tx.active.contains(row.xmax)))))
}
pub open spec fn first_visible(s: Image, p: u32, tx: Snapshot) -> Option<u32>
    recommends image_valid(s), p == 0 || s.rows.contains_key(p),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if !image_valid(s) || p == 0 || !s.rows.contains_key(p) { None }
    else if visible(s.rows[p], tx) { Some(p) }
    else { first_visible(s, s.rows[p].next, tx) }
}
pub open spec fn last_write(writes: Seq<PendingWrite>, id: usize, n: int) -> Option<u32>
    decreases n,
{
    if n <= 0 { None }
    else if writes[n-1].row_id == id { Some(writes[n-1].new_ptr) }
    else { last_write(writes, id, n-1) }
}
pub open spec fn transaction_valid(s: Image, tx: Transaction) -> bool {
    image_valid(s) && 0 < tx.txid < tx.snapshot_xmax
    && 0 < tx.snapshot_xmin <= tx.txid
    && forall|i: int| 0 <= i < tx.write_set.len() ==>
        tx.write_set[i].row_id < s.capacity && s.rows.contains_key(tx.write_set[i].new_ptr)
}
pub open spec fn selected_pointer(s: Image, tx: Transaction, id: usize) -> Option<u32> {
    let own = last_write(tx.write_set@, id, tx.write_set.len() as int);
    if own.is_some() { own } else { first_visible(s, s.heads[id], snapshot(tx)) }
}
pub open spec fn selected_value(s: Image, tx: Transaction, id: usize) -> Option<Option<usize>> {
    let p = selected_pointer(s, tx, id);
    if p.is_some() { Some(s.rows[p.unwrap()].value) } else { None }
}
pub open spec fn stored_snapshot_key(s:Image,tx:Transaction,id:usize) -> Option<usize> {
    let p=first_visible(s,s.heads[id],snapshot(tx));
    if p.is_some() {s.rows[p.unwrap()].value} else {None}
}
pub open spec fn transaction_view_same(a: Transaction, b: Transaction) -> bool {
    a.txid == b.txid && a.snapshot_xmin == b.snapshot_xmin && a.snapshot_xmax == b.snapshot_xmax
        && a.snapshot_active@ == b.snapshot_active@ && a.write_set@ == b.write_set@
}
pub open spec fn records_extend(before: Seq<ReadRecord>, after: Seq<ReadRecord>) -> bool {
    before.len() <= after.len()
        && forall|i:int| 0 <= i < before.len() ==> before[i] == after[i]
}
pub open spec fn recording(before: Seq<ReadRecord>, row_id: usize, row_ptr: u32, xmin: u64) -> Seq<ReadRecord> {
    if exists|i:int| 0 <= i < before.len() && before[i].row_ptr == row_ptr {
        before
    } else {
        before.push(ReadRecord { row_id, row_ptr, xmin })
    }
}
pub open spec fn observed_record(image: Image, tx: Transaction, entry: ReadRecord) -> bool {
    entry.row_id < image.capacity && image.rows.contains_key(entry.row_ptr)
        && first_visible(image, image.heads[entry.row_id], snapshot(tx)) == Some(entry.row_ptr)
        && entry.xmin == image.rows[entry.row_ptr].xmin
}
pub open spec fn records_have_provenance(image: Image, tx: Transaction,
    original: Seq<ReadRecord>, current: Seq<ReadRecord>) -> bool {
    forall|entry:ReadRecord| current.contains(entry) ==>
        original.contains(entry) || observed_record(image,tx,entry)
}

pub trait Storage {
    // A retained per-operation version-chain image. This does NOT assert that
    // the entire physical heap is frozen while a reader materializes candidates.
    // Native load-to-image correspondence and reclamation ownership stay open.
    spec fn image(&self) -> Image;
    fn ensure_open(&self, tx: &Transaction) -> Result<(), Error>;
    fn capacity(&self) -> (n: usize) ensures n == self.image().capacity;
    fn head(&self, id: usize) -> (r: Result<u32, Error>)
        requires id < self.image().capacity,
        ensures r.is_ok() ==> r.unwrap() == self.image().heads[id];
    fn resolve(&self, ptr: u32) -> (r: Result<Row, Error>)
        requires self.image().rows.contains_key(ptr),
        ensures r.is_ok() ==> r.unwrap() == self.image().rows[ptr];
    fn yield_now(&self);
}

pub fn active_contains(active: &Vec<u64>, id: u64) -> (r: bool)
    ensures r == active@.contains(id),
{
    let mut i = 0;
    while i < active.len()
        invariant i <= active.len(), forall|j:int| 0 <= j < i ==> active[j] != id,
        decreases active.len() - i,
    {
        if active[i] == id { return true; }
        i += 1;
    }
    false
}
pub fn is_visible(row: &Row, tx: &Transaction) -> (r: bool)
    ensures r == visible(*row, snapshot(*tx)),
{
    /* NATIVE_VISIBLE */
}
pub fn row_locked_by_other_tx(row: &Row, txid: u64) -> (r: bool)
    ensures r == (row.locked && row.owner != txid),
{
    /* NATIVE_ROW_LOCK */
}
pub proof fn selected_pointer_valid(s: Image, tx: Snapshot, p: u32)
    requires image_valid(s), p == 0 || s.rows.contains_key(p),
    ensures first_visible(s,p,tx).is_some() ==> s.rows.contains_key(first_visible(s,p,tx).unwrap()),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if p != 0 && !visible(s.rows[p],tx) { selected_pointer_valid(s,tx,s.rows[p].next); }
}
pub fn find_visible_row_ptr<D: Storage>(driver: &D, tx: &Transaction, row_id: usize)
    -> (r: Result<Option<u32>, Error>)
    requires image_valid(driver.image()), row_id < driver.image().capacity,
    ensures r.is_ok() ==> r.unwrap() == first_visible(driver.image(), driver.image().heads[row_id], snapshot(*tx)),
        r.is_ok() && r.unwrap().is_some() ==> driver.image().rows.contains_key(r.unwrap().unwrap()),
{
    /* NATIVE_FIND_VISIBLE */
}
pub fn latest_pending(writes: &Vec<PendingWrite>, row_id: usize) -> (r: Option<PendingWrite>)
    ensures r.is_some() ==> r.unwrap().row_id == row_id && writes@.contains(r.unwrap()),
        r.is_some() <==> last_write(writes@,row_id,writes.len() as int).is_some(),
        r.is_some() ==> r.unwrap().new_ptr == last_write(writes@,row_id,writes.len() as int).unwrap(),
{
    let mut i = writes.len();
    while i > 0
        invariant i <= writes.len(),
            last_write(writes@,row_id,writes.len() as int) == last_write(writes@,row_id,i as int),
        decreases i,
    {
        i -= 1;
        if writes[i].row_id == row_id { return Some(writes[i]); }
    }
    None
}
pub fn record_read(tx: &mut Transaction, row_id: usize, row_ptr: u32, xmin: u64)
    ensures transaction_view_same(*old(tx),*final(tx)),
        final(tx).read_set@ == recording(old(tx).read_set@,row_id,row_ptr,xmin),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        final(tx).read_set.len() <= old(tx).read_set.len() + 1,
        exists|i:int| 0 <= i < final(tx).read_set.len() && final(tx).read_set[i].row_ptr == row_ptr,
        forall|r:ReadRecord| old(tx).read_set@.contains(r) ==> final(tx).read_set@.contains(r),
{
    /* NATIVE_RECORD_READ */
}
pub fn read_already_recorded(reads:&Vec<ReadRecord>,ptr:u32) -> (found:bool)
    ensures found <==> exists|i:int| 0 <= i < reads.len() && reads[i].row_ptr == ptr,
{
    let mut i=0;
    while i < reads.len()
        invariant i <= reads.len(), forall|j:int| 0 <= j < i ==> reads[j].row_ptr != ptr,
        decreases reads.len()-i,
    {
        if reads[i].row_ptr == ptr {return true;}
        i+=1;
    }
    false
}
pub fn read<D: Storage>(driver: &D, tx: &mut Transaction, row_id: usize)
    -> (r: Result<Option<Option<usize>>, Error>)
    requires transaction_valid(driver.image(),*old(tx)),
    ensures transaction_view_same(*old(tx),*final(tx)),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        records_have_provenance(driver.image(),*old(tx),old(tx).read_set@,final(tx).read_set@),
        final(tx).read_set.len() <= old(tx).read_set.len() + 1,
        r.is_ok() ==> row_id < driver.image().capacity
            && r.unwrap() == selected_value(driver.image(),*old(tx),row_id),
        r.is_ok() && r.unwrap().is_some()
            && last_write(old(tx).write_set@,row_id,old(tx).write_set.len() as int).is_none() ==>
            exists|i:int| 0<=i<final(tx).read_set.len()
                && final(tx).read_set[i].row_ptr == first_visible(driver.image(),driver.image().heads[row_id],snapshot(*old(tx))).unwrap(),
        r.is_ok() && r.unwrap().is_some()
            && last_write(old(tx).write_set@,row_id,old(tx).write_set.len() as int).is_none() ==>
            final(tx).read_set@ == recording(old(tx).read_set@,row_id,
                first_visible(driver.image(),driver.image().heads[row_id],snapshot(*old(tx))).unwrap(),
                driver.image().rows[first_visible(driver.image(),driver.image().heads[row_id],snapshot(*old(tx))).unwrap()].xmin),
        r.is_err() || (r.is_ok() && (r.unwrap().is_none()
            || last_write(old(tx).write_set@,row_id,old(tx).write_set.len() as int).is_some())) ==>
            final(tx).read_set@ == old(tx).read_set@,
        forall|entry:ReadRecord| old(tx).read_set@.contains(entry) ==> final(tx).read_set@.contains(entry),
{
    /* NATIVE_READ */
}
pub open spec fn read_conflict(row:Row,entry:ReadRecord,tx:Transaction) -> bool {
    row.xmin!=entry.xmin || (row.xmax!=0 && row.xmax!=tx.txid
        && (row.xmax>=tx.snapshot_xmax || tx.snapshot_active@.contains(row.xmax)))
}
pub open spec fn reads_conflict(image:Image,tx:Transaction) -> bool {
    exists|i:int| 0<=i<tx.read_set.len() && read_conflict(image.rows[tx.read_set[i].row_ptr],tx.read_set[i],tx)
}
pub fn has_serialization_conflict<D:Storage>(driver:&D,tx:&Transaction) -> (r:Result<bool,Error>)
    requires forall|i:int| 0<=i<tx.read_set.len() ==> driver.image().rows.contains_key(tx.read_set[i].row_ptr),
    ensures r.is_ok() ==> r.unwrap()==reads_conflict(driver.image(),*tx),
{
    /* NATIVE_ROW_VALIDATION */
}
pub proof fn later_deletion_rejects_recorded_read(image:Image,tx:Transaction,i:int)
    requires 0<=i<tx.read_set.len(), image.rows.contains_key(tx.read_set[i].row_ptr),
        image.rows[tx.read_set[i].row_ptr].xmax!=0, image.rows[tx.read_set[i].row_ptr].xmax!=tx.txid,
        image.rows[tx.read_set[i].row_ptr].xmax>=tx.snapshot_xmax
            || tx.snapshot_active@.contains(image.rows[tx.read_set[i].row_ptr].xmax),
    ensures reads_conflict(image,tx),
{
    assert(read_conflict(image.rows[tx.read_set[i].row_ptr],tx.read_set[i],tx));
}

pub trait CandidateSet: Sized {
    spec fn contents(&self) -> Set<usize>;
    fn from_vec(rows: Vec<usize>) -> (s: Self) ensures s.contents() == rows@.to_set();
    fn insert(&mut self, row: usize) ensures final(self).contents() == old(self).contents().insert(row);
    fn into_vec(self) -> (rows: Vec<usize>)
        ensures rows@.to_set() == self.contents(),
            forall|i:int,j:int| 0 <= i < j < rows.len() ==> rows[i] < rows[j];
}
pub trait KeyPredicate {
    spec fn matches(&self, value: usize) -> bool;
    spec fn bucket(&self, value: usize) -> usize;
    fn evaluate(&self, value: &Option<usize>) -> (r: bool)
        ensures r == (value.is_some() && self.matches(value.unwrap()));
}
pub open spec fn own_ids(writes: Seq<PendingWrite>, n: int) -> Set<usize>
    decreases n,
{
    if n <= 0 { Set::empty() } else { own_ids(writes,n-1).insert(writes[n-1].row_id) }
}
pub open spec fn matches_row<P:KeyPredicate>(image: Image, tx: Transaction, p: P, id: usize) -> bool {
    id < image.capacity && selected_value(image,tx,id).is_some()
        && selected_value(image,tx,id).unwrap().is_some()
        && p.matches(selected_value(image,tx,id).unwrap().unwrap())
}
pub proof fn prefix_less(rows:Seq<usize>, n:int, id:usize)
    requires 0 <= n < rows.len(), rows.take(n).contains(id),
        forall|i:int,j:int| 0 <= i < j < rows.len() ==> rows[i] < rows[j],
    ensures id < rows[n],
{
    let i=choose|i:int| 0 <= i < rows.take(n).len() && rows.take(n)[i]==id;
    assert(rows[i]==id);
}
pub fn materialize<D:Storage,C:CandidateSet,P:KeyPredicate>(driver: &D, tx: &mut Transaction,
    candidates: Vec<usize>, predicate: &P) -> (r: Result<Vec<usize>,Error>)
    requires transaction_valid(driver.image(),*old(tx)),
        forall|id:usize| matches_row(driver.image(),*old(tx),*predicate,id) ==>
            candidates@.contains(id) || own_ids(old(tx).write_set@,old(tx).write_set.len() as int).contains(id),
    ensures transaction_view_same(*old(tx),*final(tx)),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        records_have_provenance(driver.image(),*old(tx),old(tx).read_set@,final(tx).read_set@),
        r.is_ok() ==> forall|id:usize| r.unwrap()@.contains(id)
            <==> matches_row(driver.image(),*old(tx),*predicate,id),
        r.is_ok() ==> forall|i:int,j:int| 0 <= i < j < r.unwrap().len() ==> r.unwrap()[i] < r.unwrap()[j],
{
    /* NATIVE_MATERIALIZE */
}

// A writer invisible to the retained snapshot may append a version and mark
// the predecessor deleted without changing that predecessor's visibility.
pub proof fn invisible_delete_preserves_visibility(row: Row, tx: Transaction, writer: u64)
    requires row.xmax == 0, writer != tx.txid,
        writer >= tx.snapshot_xmax || tx.snapshot_active@.contains(writer),
    ensures visible(row,snapshot(tx)) == visible(Row {xmax: writer,..row},snapshot(tx)),
{}
pub proof fn first_visible_frame(before:Image,after:Image,tx:Snapshot,ptr:u32)
    requires image_valid(before),image_valid(after),ptr==0 || before.rows.contains_key(ptr),
        forall|p:u32| #[trigger] before.rows.contains_key(p) ==> after.rows.contains_key(p)
            && before.rows[p].next==after.rows[p].next
            && visible(before.rows[p],tx)==visible(after.rows[p],tx),
    ensures first_visible(before,ptr,tx)==first_visible(after,ptr,tx),
    decreases if ptr==0 {0nat} else {before.rank[ptr]+1},
{
    if ptr!=0 && !visible(before.rows[ptr],tx) {
        first_visible_frame(before,after,tx,before.rows[ptr].next);
    }
}
pub proof fn invisible_head_append_preserves_snapshot(before:Image,after:Image,tx:Transaction,
    id:usize,ptr:u32,newrow:Row)
    requires image_valid(before),image_valid(after),id<before.capacity,after.capacity==before.capacity,
        before.heads[id]!=0,ptr!=0,!before.rows.contains_key(ptr),
        before.rows[before.heads[id]].xmax==0,
        newrow.xmin!=tx.txid,!creator_visible(newrow.xmin,snapshot(tx)),
        newrow.xmin>=tx.snapshot_xmax || tx.snapshot_active@.contains(newrow.xmin),
        newrow.next==before.heads[id],
        after.heads==before.heads.insert(id,ptr),
        after.rows==before.rows.insert(before.heads[id],Row{xmax:newrow.xmin,..before.rows[before.heads[id]]}).insert(ptr,newrow),
    ensures stored_snapshot_key(before,tx,id)==stored_snapshot_key(after,tx,id),
{
    let oldhead=before.heads[id];
    invisible_delete_preserves_visibility(before.rows[oldhead],tx,newrow.xmin);
    assert forall|p:u32| before.rows.contains_key(p) implies after.rows.contains_key(p)
        && before.rows[p].next==after.rows[p].next
        && visible(before.rows[p],snapshot(tx))==visible(after.rows[p],snapshot(tx)) by {
        if p==oldhead { assert(visible(before.rows[p],snapshot(tx))==visible(after.rows[p],snapshot(tx))); }
    }
    first_visible_frame(before,after,snapshot(tx),oldhead);
    selected_pointer_valid(before,snapshot(tx),oldhead);
    assert(!visible(newrow,snapshot(tx)));
    assert(first_visible(after,ptr,snapshot(tx))==first_visible(before,oldhead,snapshot(tx)));
    if first_visible(before,oldhead,snapshot(tx)).is_some() {
        let p=first_visible(before,oldhead,snapshot(tx)).unwrap();
        assert(before.rows.contains_key(p));
        assert(after.rows[p].value==before.rows[p].value);
    }
}
}
