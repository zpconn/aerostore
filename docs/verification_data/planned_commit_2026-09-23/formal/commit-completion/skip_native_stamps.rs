// Generated source-bound one-write publication/completion join.
// Generated single-index posting/row data composition.
pub mod lookup {
// Generated from native visibility, read, and index_lookup materialization.
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
    if row . xmin == tx . txid {
    return true ;
}
if row . xmin < tx . snapshot_xmin {
}
else if row . xmin >= tx . snapshot_xmax || active_contains ( & tx . snapshot_active , row . xmin ) {
    return false ;
}
let xmax = row . xmax ;
if xmax == 0 {
    return true ;
}
if xmax == tx . txid {
    return false ;
}
xmax >= tx . snapshot_xmax || active_contains ( & tx . snapshot_active , xmax )
}
pub fn row_locked_by_other_tx(row: &Row, txid: u64) -> (r: bool)
    ensures r == (row.locked && row.owner != txid),
{
    if ! row . locked {
    return false ;
}
let owner = row . owner ;
owner != txid
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
    let mut head_offset = driver . head ( row_id ) ? ;
let mut steps : u32 = 0 ;

    while head_offset != EMPTY_PTR
        invariant image_valid(driver.image()), row_id < driver.image().capacity,
            head_offset == 0 || driver.image().rows.contains_key(head_offset),
            steps <= MAX_VISIBLE_CHAIN_STEPS,
            first_visible(driver.image(),head_offset,snapshot(*tx))
                == first_visible(driver.image(),driver.image().heads[row_id],snapshot(*tx)),
        decreases MAX_VISIBLE_CHAIN_STEPS + 1 - steps,
    {
     steps = steps + 1 ;
if steps > MAX_VISIBLE_CHAIN_STEPS {
    driver . yield_now ( ) ;
    return Err ( Error :: SerializationFailure ) ;
}
let row_ptr = head_offset ;
let row = driver . resolve ( row_ptr ) ? ;
if is_visible ( & row , tx ) {
    return Ok ( Some ( row_ptr ) ) ;
}
head_offset = row . next ;
}
Ok ( None )
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
    let row_offset = row_ptr ;
if read_already_recorded ( & tx . read_set , row_offset ) {
    return ;
}
tx . read_set . push ( ReadRecord {
    row_id , row_ptr , xmin : xmin ,
}
) ;
proof { assert(tx.read_set[tx.read_set.len() as int-1].row_ptr == row_ptr); }

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
    driver . ensure_open ( tx ) ? ;
if row_id >= driver . capacity ( ) {
    return Err ( Error :: RowOutOfBounds {
        row_id , capacity : driver . capacity ( ) ,
    }
    ) ;
}
if let Some ( pending ) = latest_pending ( & tx . write_set , row_id ) {
    let pending_row = driver . resolve ( pending . new_ptr ) ? ;
    return Ok ( Some ( pending_row . value ) ) ;
}
if let Some ( row_ptr ) = find_visible_row_ptr ( driver , tx , row_id ) ? {
    let row = driver . resolve ( row_ptr ) ? ;
    if row_locked_by_other_tx ( & row , tx . txid ) {
        driver . yield_now ( ) ;
        return Err ( Error :: SerializationFailure ) ;
    }
    let observed_xmin = row . xmin ;
    record_read ( tx , row_id , row_ptr , observed_xmin ) ;
    return Ok ( Some ( row . value ) ) ;
}
Ok ( None )
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
    
    let mut ri=0;
    while ri<tx.read_set.len()
        invariant ri<=tx.read_set.len(),
            forall|i:int| 0<=i<tx.read_set.len() ==> driver.image().rows.contains_key(tx.read_set[i].row_ptr),
            forall|i:int| 0<=i<ri ==> !read_conflict(driver.image().rows[tx.read_set[i].row_ptr],tx.read_set[i],*tx),
        decreases tx.read_set.len()-ri,
    {
        let read=&tx.read_set[ri];
     let row = driver . resolve ( read . row_ptr ) ? ;
if row . xmin != read . xmin {
    return Ok ( true ) ;
}
let xmax = row . xmax ;
if xmax == 0 || xmax == tx . txid {
    ri = ri + 1 ;
    continue ;
}
let committed_after_snapshot = xmax >= tx . snapshot_xmax || active_contains ( & tx . snapshot_active , xmax ) ;
if committed_after_snapshot {
    return Ok ( true ) ;
}

        ri += 1;
    }
     Ok ( false )
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
    let ghost initial = *tx;
let ghost candidates_input = candidates;
let mut candidates = C :: from_vec ( candidates ) ;
let mut wi = 0 ;

    let ghost raw_ids = candidates.contents();
    while wi < tx.write_set.len()
        invariant wi <= tx.write_set.len(), initial == *old(tx), transaction_view_same(initial,*tx),
            tx.read_set@ == initial.read_set@,
            raw_ids == candidates_input@.to_set(),
            forall|id:usize| matches_row(driver.image(),initial,*predicate,id) ==>
                raw_ids.contains(id) || own_ids(initial.write_set@,initial.write_set.len() as int).contains(id),
            candidates.contents() == raw_ids.union(own_ids(tx.write_set@,wi as int)),
        decreases tx.write_set.len() - wi,
    {
        candidates.insert(tx.write_set[wi].row_id);
        wi += 1;
    }
    let ordered = candidates.into_vec();
    proof {
        assert forall|id:usize| matches_row(driver.image(),initial,*predicate,id)
            implies ordered@.contains(id) by {
            assert(raw_ids.contains(id) || own_ids(initial.write_set@,initial.write_set.len() as int).contains(id));
            assert(ordered@.to_set().contains(id));
        }
    }
     let mut result = Vec :: new ( ) ;

    let mut ci = 0;
    while ci < ordered.len()
        invariant ci <= ordered.len(), initial == *old(tx), transaction_view_same(initial,*tx),
            records_extend(initial.read_set@,tx.read_set@),
            records_have_provenance(driver.image(),initial,initial.read_set@,tx.read_set@),
            transaction_valid(driver.image(),*tx),
            ordered@.to_set() == raw_ids.union(own_ids(initial.write_set@,initial.write_set.len() as int)),
            forall|id:usize| matches_row(driver.image(),initial,*predicate,id) ==> ordered@.contains(id),
            forall|id:usize| result@.contains(id) <==>
                ordered@.take(ci as int).contains(id) && matches_row(driver.image(),initial,*predicate,id),
            forall|i:int,j:int| 0 <= i < j < ordered.len() ==> ordered[i] < ordered[j],
            forall|i:int,j:int| 0 <= i < j < result.len() ==> result[i] < result[j],
        decreases ordered.len() - ci,
    {
        let row_id = ordered[ci];
     if let Some ( value ) = read ( driver , tx , row_id ) ? {
    if predicate . evaluate ( & value ) {
        proof {
        assert forall|i:int| 0 <= i < result.len() implies result[i] < row_id by {
            assert(result@.contains(result[i]));
            assert(ordered@.take(ci as int).contains(result[i]));
            prefix_less(ordered@,ci as int,result[i]);
        }
    } result . push ( row_id ) ;
    }
}

        proof { assert(ordered@.take(ci as int+1) =~= ordered@.take(ci as int).push(row_id)); }
        ci += 1;
    }
     Ok ( result )
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

// Reproved in Verus; no unchecked transfer from Lean or TLC.
verus! {
pub struct Protocol {
    pub clock:u64, pub active:Set<u64>, pub ended:Set<u64>,
    pub published:Map<u64,u64>, pub observed:Map<u64,Set<u64>>,
}
pub enum Action { Start{txid:u64}, End{txid:u64}, Snapshot{reader:u64}, Publish{writer:u64,stamp:u64} }
pub open spec fn protocol_initial(p:Protocol) -> bool {
    p.clock>0 && p.active==Set::<u64>::empty() && p.ended==Set::<u64>::empty()
        && p.published==Map::<u64,u64>::empty() && p.observed==Map::<u64,Set<u64>>::empty()
}
pub open spec fn protocol_valid(p:Protocol) -> bool {
    p.clock>0 && p.active.disjoint(p.ended)
    && (forall|tx:u64| p.active.contains(tx) || p.ended.contains(tx) ==> 0<tx<p.clock)
    && (forall|writer:u64| #[trigger] p.published.contains_key(writer) ==> p.ended.contains(writer)
        && writer<p.published[writer]<p.clock)
    && (forall|reader:u64| p.observed.contains_key(reader) ==> 0<reader<p.clock)
    && (forall|reader:u64,writer:u64| p.observed.contains_key(reader) && p.observed[reader].contains(writer)
        && p.published.contains_key(writer) ==> reader<p.published[writer])
}
pub open spec fn protocol_step(a:Protocol,b:Protocol,action:Action) -> bool {
    match action {
        Action::Start{txid} => a.clock<=txid<u64::MAX
            && b==(Protocol{clock:(txid+1) as u64,active:a.active.insert(txid),..a}),
        Action::End{txid} => a.active.contains(txid)
            && b==(Protocol{active:a.active.remove(txid),ended:a.ended.insert(txid),..a}),
        Action::Snapshot{reader} => a.active.contains(reader)
            && b==(Protocol{observed:a.observed.insert(reader,a.active.remove(reader)),..a}),
        Action::Publish{writer,stamp} => a.ended.contains(writer) && !a.published.contains_key(writer)
            && a.clock<=stamp<u64::MAX
            && b==(Protocol{clock:(stamp+1) as u64,published:a.published.insert(writer,stamp),..a}),
    }
}
pub proof fn protocol_step_preserves(a:Protocol,b:Protocol,action:Action)
    requires protocol_valid(a),protocol_step(a,b,action),
    ensures protocol_valid(b),
{
    match action {
        Action::Start{txid} => {
            assert(!a.ended.contains(txid));
            assert(b.active.disjoint(b.ended));
        },
        Action::Snapshot{reader} => {
            assert forall|who:u64,writer:u64| b.observed.contains_key(who) && b.observed[who].contains(writer)
                && b.published.contains_key(writer) implies who<b.published[writer] by {
                if who==reader {
                    assert(a.active.contains(writer));
                    assert(a.ended.contains(writer));
                }
            }
        },
        _ => {},
    }
}
pub open spec fn protocol_history(states:Seq<Protocol>,actions:Seq<Action>) -> bool {
    states.len()==actions.len()+1 && protocol_initial(states[0])
        && forall|i:int| 0<=i<actions.len() ==> protocol_step(states[i],states[i+1],actions[i])
}
pub proof fn reachable_protocol_valid(states:Seq<Protocol>,actions:Seq<Action>,n:int)
    requires protocol_history(states,actions),0<=n<=actions.len(),
    ensures protocol_valid(states[n]),
    decreases n,
{
    if n>0 {
        reachable_protocol_valid(states,actions,n-1);
        protocol_step_preserves(states[n-1],states[n],actions[n-1]);
    }
}
pub struct Event { pub row:usize, pub before:Option<usize>, pub after:Option<usize>, pub creator:u64, pub stamp:u64 }
pub open spec fn key_matches<P:KeyPredicate>(p:P,key:Option<usize>) -> bool {
    key.is_some() && p.matches(key.unwrap())
}
pub open spec fn affects<P:KeyPredicate>(p:P,e:Event) -> bool {
    key_matches(p,e.before) || key_matches(p,e.after)
}
pub open spec fn event_buckets<P:KeyPredicate>(p:P,e:Event) -> Set<usize> {
    let old = if e.before.is_some() {Set::empty().insert(p.bucket(e.before.unwrap()))} else {Set::empty()};
    if e.after.is_some() {old.insert(p.bucket(e.after.unwrap()))} else {old}
}
pub open spec fn stamp_at<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,n:int,p:P,b:usize) -> u64
    decreases n,
{
    if n<=0 {initial[b]} else if event_buckets(p,events[n-1]).contains(b) {events[n-1].stamp}
    else {stamp_at(initial,events,n-1,p,b)}
}
pub open spec fn guarded_stamp_history<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,p:P) -> bool {
    forall|i:int,b:usize| 0<=i<events.len() && event_buckets(p,events[i]).contains(b) ==>
        initial.contains_key(b) && stamp_at(initial,events,i,p,b)<=events[i].stamp
}
pub proof fn stamp_history_covers<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,n:int,p:P,i:int,b:usize)
    requires 0<=i<n<=events.len(),guarded_stamp_history(initial,events,p),event_buckets(p,events[i]).contains(b),
    ensures events[i].stamp<=stamp_at(initial,events,n,p,b),
    decreases n,
{
    if i<n-1 {
        stamp_history_covers(initial,events,n-1,p,i,b);
        if event_buckets(p,events[n-1]).contains(b) {
            assert(stamp_at(initial,events,n-1,p,b)<=events[n-1].stamp);
        }
    }
}
pub proof fn accepted_stamps_force_early_events<P:KeyPredicate>(initial:Map<usize,u64>,events:Seq<Event>,p:P,
    buckets:Set<usize>,reader:u64)
    requires guarded_stamp_history(initial,events,p),
        forall|value:usize| p.matches(value) ==> buckets.contains(p.bucket(value)),
        forall|b:usize| buckets.contains(b) ==> stamp_at(initial,events,events.len() as int,p,b)<reader,
    ensures forall|i:int| 0<=i<events.len() && affects(p,events[i]) ==> events[i].stamp<reader,
{
    assert forall|i:int| 0<=i<events.len() && affects(p,events[i]) implies events[i].stamp<reader by {
        let e=events[i];
        let value=if key_matches(p,e.before) {e.before.unwrap()} else {e.after.unwrap()};
        assert(p.matches(value));
        let bucket=p.bucket(value);
        assert(event_buckets(p,e).contains(bucket));
        stamp_history_covers(initial,events,events.len() as int,p,i,bucket);
        assert(buckets.contains(bucket));
        assert(stamp_at(initial,events,events.len() as int,p,bucket)<reader);
    }
}
pub open spec fn live_rows(initial:Map<usize,Option<usize>>, events:Seq<Event>,n:int) -> Map<usize,Option<usize>>
    decreases n,
{
    if n<=0 {initial} else {live_rows(initial,events,n-1).insert(events[n-1].row,events[n-1].after)}
}
pub open spec fn snapshot_rows(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int,s:Snapshot) -> Map<usize,Option<usize>>
    decreases n,
{
    if n<=0 {initial} else if creator_visible(events[n-1].creator,s) {
        snapshot_rows(initial,events,n-1,s).insert(events[n-1].row,events[n-1].after)
    } else {snapshot_rows(initial,events,n-1,s)}
}
pub open spec fn postings(rows:Map<usize,Option<usize>>) -> Set<(usize,usize)> {
    rows.dom().filter(|id:usize| rows[id].is_some()).map(|id:usize| (rows[id].unwrap(),id))
}
pub open spec fn posting_step(before:Set<(usize,usize)>,e:Event) -> Set<(usize,usize)> {
    let removed = if e.before.is_some() {before.remove((e.before.unwrap(),e.row))} else {before};
    if e.after.is_some() {removed.insert((e.after.unwrap(),e.row))} else {removed}
}
pub open spec fn posting_replay(initial:Set<(usize,usize)>,events:Seq<Event>,n:int) -> Set<(usize,usize)>
    decreases n,
{
    if n<=0 {initial} else {posting_step(posting_replay(initial,events,n-1),events[n-1])}
}
pub open spec fn coherent(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int) -> bool {
    forall|i:int| 0<=i<n ==> initial.contains_key(events[i].row)
        && events[i].before==live_rows(initial,events,i)[events[i].row]
}
pub proof fn live_domain(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int)
    requires 0<=n<=events.len(), coherent(initial,events,n),
    ensures live_rows(initial,events,n).dom()==initial.dom(),
    decreases n,
{
    if n>0 {
        live_domain(initial,events,n-1);
        assert(initial.contains_key(events[n-1].row));
        assert(live_rows(initial,events,n).dom() =~= initial.dom());
    }
}
pub proof fn posting_update_exact(rows:Map<usize,Option<usize>>,e:Event)
    requires rows.contains_key(e.row),rows[e.row]==e.before,
    ensures posting_step(postings(rows),e)==postings(rows.insert(e.row,e.after)),
{
    assert forall|pair:(usize,usize)| posting_step(postings(rows),e).contains(pair)
        == postings(rows.insert(e.row,e.after)).contains(pair) by {
        posting_membership(rows,pair.0,pair.1);
        posting_membership(rows.insert(e.row,e.after),pair.0,pair.1);
    }
    assert(posting_step(postings(rows),e) =~= postings(rows.insert(e.row,e.after)));
}
pub proof fn posting_membership(rows:Map<usize,Option<usize>>,key:usize,id:usize)
    ensures postings(rows).contains((key,id)) <==> rows.contains_key(id) && rows[id]==Some(key),
{
    if postings(rows).contains((key,id)) {
        let r=choose|r:usize| rows.dom().filter(|r:usize|rows[r].is_some()).contains(r)
            && (rows[r].unwrap(),r)==(key,id);
        assert(r==id);
    } else if rows.contains_key(id) && rows[id]==Some(key) {
        assert(rows.dom().filter(|r:usize|rows[r].is_some()).contains(id));
    }
}
pub proof fn posting_history_exact(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int)
    requires 0<=n<=events.len(),coherent(initial,events,n),
    ensures posting_replay(postings(initial),events,n)==postings(live_rows(initial,events,n)),
    decreases n,
{
    if n>0 {
        posting_history_exact(initial,events,n-1);
        live_domain(initial,events,n-1);
        assert(initial.contains_key(events[n-1].row));
        posting_update_exact(live_rows(initial,events,n-1),events[n-1]);
    }
}
// These numeric facts are consequences of shared-clock/lifecycle history in
// the checked composition, not an assumption of historical candidate coverage.
pub open spec fn event_chronology(s:Snapshot,e:Event) -> bool {
    e.creator < e.stamp && (s.active.contains(e.creator) ==> e.stamp > s.txid)
}
pub proof fn event_chronology_from_history(states:Seq<Protocol>,actions:Seq<Action>,s:Snapshot,e:Event)
    requires protocol_history(states,actions),
        states.last().observed.contains_key(s.txid),s.active.to_set()==states.last().observed[s.txid],
        states.last().published.contains_key(e.creator),states.last().published[e.creator]==e.stamp,
    ensures event_chronology(s,e),
{
    reachable_protocol_valid(states,actions,actions.len() as int);
    let p=states.last();
    assert(e.creator<p.published[e.creator]);
    if s.active.contains(e.creator) {
        assert(p.observed[s.txid].contains(e.creator));
        assert(s.txid<p.published[e.creator]);
    }
}
pub proof fn early_publication_is_visible(s:Snapshot,e:Event)
    requires s.txid<s.xmax,event_chronology(s,e),e.stamp<s.txid,
    ensures creator_visible(e.creator,s),
{}
pub proof fn accepted_replay_matches<P:KeyPredicate>(initial:Map<usize,Option<usize>>,events:Seq<Event>,n:int,s:Snapshot,p:P)
    requires 0<=n<=events.len(),coherent(initial,events,n),s.txid<s.xmax,
        forall|i:int| 0<=i<n ==> event_chronology(s,events[i]),
        forall|i:int| 0<=i<n && affects(p,events[i]) ==> events[i].stamp<s.txid,
    ensures forall|id:usize| initial.contains_key(id) ==>
        key_matches(p,live_rows(initial,events,n)[id]) == key_matches(p,snapshot_rows(initial,events,n,s)[id]),
    decreases n,
{
    if n>0 {
        accepted_replay_matches(initial,events,n-1,s,p);
        let e=events[n-1];
        if affects(p,e) {early_publication_is_visible(s,e);}
        assert forall|id:usize| initial.contains_key(id) implies
            key_matches(p,live_rows(initial,events,n)[id]) == key_matches(p,snapshot_rows(initial,events,n,s)[id]) by {
            if id==e.row && !creator_visible(e.creator,s) {
                assert(!affects(p,e));
                assert(e.before==live_rows(initial,events,n-1)[id]);
            }
        }
    }
}
pub open spec fn raw_candidates<P:KeyPredicate>(p:P,index:Set<(usize,usize)>) -> Set<usize> {
    index.filter(|pair:(usize,usize)|p.matches(pair.0)).map(|pair:(usize,usize)|pair.1)
}
pub proof fn accepted_history_has_candidates<P:KeyPredicate>(initial:Map<usize,Option<usize>>,events:Seq<Event>,s:Snapshot,p:P)
    requires coherent(initial,events,events.len() as int),s.txid<s.xmax,
        forall|i:int| 0<=i<events.len() ==> event_chronology(s,events[i]),
        forall|i:int| 0<=i<events.len() && affects(p,events[i]) ==> events[i].stamp<s.txid,
    ensures forall|id:usize| initial.contains_key(id) && key_matches(p,snapshot_rows(initial,events,events.len() as int,s)[id]) ==>
        raw_candidates(p,posting_replay(postings(initial),events,events.len() as int)).contains(id),
{
    posting_history_exact(initial,events,events.len() as int);
    live_domain(initial,events,events.len() as int);
    accepted_replay_matches(initial,events,events.len() as int,s,p);
    assert forall|id:usize| initial.contains_key(id) && key_matches(p,snapshot_rows(initial,events,events.len() as int,s)[id]) implies
        raw_candidates(p,posting_replay(postings(initial),events,events.len() as int)).contains(id) by {
        let value=live_rows(initial,events,events.len() as int)[id].unwrap();
        posting_membership(live_rows(initial,events,events.len() as int),value,id);
        assert(postings(live_rows(initial,events,events.len() as int)).contains((value,id)));
        assert(posting_replay(postings(initial),events,events.len() as int)
            .filter(|pair:(usize,usize)|p.matches(pair.0)).contains((value,id)));
    }
}
pub proof fn absent_own_row_has_no_write(writes:Seq<PendingWrite>,id:usize,n:int)
    requires 0<=n<=writes.len(),!own_ids(writes,n).contains(id),
    ensures last_write(writes,id,n).is_none(),
    decreases n,
{
    if n>0 {absent_own_row_has_no_write(writes,id,n-1);}
}
pub proof fn matched_row_comes_from_snapshot_or_own<P:KeyPredicate>(s:Image,tx:Transaction,p:P,id:usize)
    requires matches_row(s,tx,p,id),!own_ids(tx.write_set@,tx.write_set.len() as int).contains(id),
    ensures key_matches(p,stored_snapshot_key(s,tx,id)),
{
    absent_own_row_has_no_write(tx.write_set@,id,tx.write_set.len() as int);
}
// Retained MVCC cells correspond to the visible-event history, while raw lookup
// enumerates current maintained postings. These are distinct storage mappings;
// neither is a premise that historical candidates are already complete.
pub open spec fn history_maps_snapshot(image:Image,tx:Transaction,initial:Map<usize,Option<usize>>,events:Seq<Event>) -> bool {
    forall|id:usize| id<image.capacity ==> initial.contains_key(id)
        && stored_snapshot_key(image,tx,id)==snapshot_rows(initial,events,events.len() as int,snapshot(tx))[id]
}
pub fn materialize_after_checked_history<D:Storage,C:CandidateSet,P:KeyPredicate>(driver:&D,tx:&mut Transaction,
    candidates:Vec<usize>,predicate:&P,
    Ghost(initial):Ghost<Map<usize,Option<usize>>>,Ghost(events):Ghost<Seq<Event>>,
    Ghost(protocol):Ghost<Seq<Protocol>>,Ghost(actions):Ghost<Seq<Action>>,
    Ghost(initial_stamps):Ghost<Map<usize,u64>>,Ghost(buckets):Ghost<Set<usize>>)
    -> (r:Result<Vec<usize>,Error>)
    requires transaction_valid(driver.image(),*old(tx)),
        coherent(initial,events,events.len() as int),history_maps_snapshot(driver.image(),*old(tx),initial,events),
        protocol_history(protocol,actions),protocol.last().observed.contains_key(old(tx).txid),
        old(tx).snapshot_active@.to_set()==protocol.last().observed[old(tx).txid],
        forall|i:int| 0<=i<events.len() ==> protocol.last().published.contains_key(events[i].creator)
            && protocol.last().published[events[i].creator]==events[i].stamp,
        guarded_stamp_history(initial_stamps,events,*predicate),
        forall|value:usize| predicate.matches(value) ==> buckets.contains(predicate.bucket(value)),
        forall|b:usize| buckets.contains(b) ==> stamp_at(initial_stamps,events,events.len() as int,*predicate,b)<old(tx).txid,
        raw_candidates(*predicate,posting_replay(postings(initial),events,events.len() as int)).subset_of(candidates@.to_set()),
    ensures transaction_view_same(*old(tx),*final(tx)),
        records_extend(old(tx).read_set@,final(tx).read_set@),
        records_have_provenance(driver.image(),*old(tx),old(tx).read_set@,final(tx).read_set@),
        r.is_ok() ==> forall|id:usize| r.unwrap()@.contains(id) <==> matches_row(driver.image(),*old(tx),*predicate,id),
        r.is_ok() ==> forall|i:int,j:int| 0<=i<j<r.unwrap().len() ==> r.unwrap()[i]<r.unwrap()[j],
{
    proof {
        assert forall|i:int| 0<=i<events.len() implies event_chronology(snapshot(*tx),events[i]) by {
            event_chronology_from_history(protocol,actions,snapshot(*tx),events[i]);
        }
        accepted_stamps_force_early_events(initial_stamps,events,*predicate,buckets,tx.txid);
        accepted_history_has_candidates(initial,events,snapshot(*tx),*predicate);
        assert forall|id:usize| matches_row(driver.image(),*tx,*predicate,id) implies
            candidates@.contains(id) || own_ids(tx.write_set@,tx.write_set.len() as int).contains(id) by {
            if !own_ids(tx.write_set@,tx.write_set.len() as int).contains(id) {
                matched_row_comes_from_snapshot_or_own(driver.image(),*tx,*predicate,id);
                assert(initial.contains_key(id));
                assert(key_matches(*predicate,snapshot_rows(initial,events,events.len() as int,snapshot(*tx))[id]));
                assert(raw_candidates(*predicate,posting_replay(postings(initial),events,events.len() as int)).contains(id));
                assert(candidates@.to_set().contains(id));
            }
        }
    }
    materialize::<D,C,P>(driver,tx,candidates,predicate)
}
pub struct EqualsKey {pub key:usize}
impl KeyPredicate for EqualsKey {
    open spec fn matches(&self,value:usize) -> bool {value==self.key}
    open spec fn bucket(&self,_value:usize) -> usize {0}
    fn evaluate(&self,value:&Option<usize>) -> (r:bool) {
        match value {Some(key)=>*key==self.key,None=>false}
    }
}
pub proof fn query_contract_has_success_witness() {
    let p0=Protocol{clock:1,active:Set::empty(),ended:Set::empty(),published:Map::empty(),observed:Map::empty()};
    let p1=Protocol{clock:2,active:Set::empty().insert(1),..p0};
    let p2=Protocol{active:Set::empty(),ended:Set::empty().insert(1),..p1};
    let p3=Protocol{clock:3,published:Map::empty().insert(1,2),..p2};
    let p4=Protocol{clock:4,active:Set::empty().insert(3),..p3};
    let p5=Protocol{observed:Map::empty().insert(3,Set::empty()),..p4};
    let states=seq![p0,p1,p2,p3,p4,p5];
    let actions=seq![Action::Start{txid:1},Action::End{txid:1},Action::Publish{writer:1,stamp:2},
        Action::Start{txid:3},Action::Snapshot{reader:3}];
    assert(p1.active.remove(1) =~= Set::<u64>::empty());
    assert(p4.active.remove(3) =~= Set::<u64>::empty());
    assert forall|i:int| 0<=i<5 implies protocol_step(states[i],states[i+1],actions[i]) by {
        if i==0 {} else if i==1 {} else if i==2 {} else if i==3 {} else {assert(i==4);}
    }
    assert(protocol_history(states,actions));
    reachable_protocol_valid(states,actions,5);
    let s=Snapshot{txid:3,xmin:3,xmax:4,active:Seq::empty()};
    assert(s.active.to_set() =~= Set::<u64>::empty());
    let e=Event{row:0,before:Some(10),after:Some(42),creator:1,stamp:2};
    let initial=Map::empty().insert(0,Some(10));
    let events=seq![e];
    let p=EqualsKey{key:42};
    event_chronology_from_history(states,actions,s,e);
    assert(coherent(initial,events,1));
    assert(guarded_stamp_history(Map::empty().insert(0,0),events,p));
    assert(stamp_at(Map::empty().insert(0,0),events,1,p,0)<s.txid);
    accepted_history_has_candidates(initial,events,s,p);
    assert(snapshot_rows(initial,events,1,s)[0]==Some(42));
    assert(raw_candidates(p,posting_replay(postings(initial),events,1)).contains(0));
}
}

}
pub mod postings {
// Generated from native posting preparation/rollback/removal. See generate.py.
// Conditional posting-set contracts for actual native preparation/rollback/
// source-removal algorithms. The primitive pointer operations remain unproved.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub type Posting = (usize, usize, usize);
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error { Index, Primitive }
#[derive(Clone, Copy)]
pub struct IndexChange { pub binding: usize, pub row_id: usize, pub before: Option<usize>, pub after: Option<usize> }
pub struct State {
    // Operation-local posting projection under the required publication guards.
    pub postings: Set<Posting>,
    pub binding_count: usize,
    pub held: Set<(usize, usize)>,
    pub poisoned: bool,
}
pub open spec fn posting(c: IndexChange, key: usize) -> Posting { (c.binding, key, c.row_id) }
pub open spec fn option_posting(c: IndexChange, key: Option<usize>) -> Set<Posting> {
    match key { Some(k) => Set::empty().insert(posting(c, k)), None => Set::empty() }
}
pub open spec fn destinations(changes: Seq<IndexChange>, n: int) -> Set<Posting>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { destinations(changes, n - 1).union(option_posting(changes[n - 1], changes[n - 1].after)) }
}
pub open spec fn sources(changes: Seq<IndexChange>, n: int) -> Set<Posting>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { sources(changes, n - 1).union(option_posting(changes[n - 1], changes[n - 1].before)) }
}
pub open spec fn recorded(changes: Seq<IndexChange>, indices: Seq<usize>) -> Set<Posting>
    decreases indices.len(),
{
    if indices.len() == 0 { Set::empty() }
    else { recorded(changes, indices.drop_last()).union(
        option_posting(changes[indices.last() as int], changes[indices.last() as int].after)) }
}
pub open spec fn change_valid(s: State, c: IndexChange) -> bool {
    c.binding < s.binding_count
    && (c.before.is_some() ==> s.held.contains((c.binding, c.before.unwrap())))
    && (c.after.is_some() ==> s.held.contains((c.binding, c.after.unwrap())))
}
pub open spec fn valid(s: State, changes: Seq<IndexChange>) -> bool {
    forall|i: int| 0 <= i < changes.len() ==> change_valid(s, changes[i])
}
pub open spec fn unique_destinations(changes: Seq<IndexChange>) -> bool {
    forall|i: int, j: int| 0 <= i < j < changes.len()
        && changes[i].after.is_some() && changes[j].after.is_some() ==>
            posting(changes[i], changes[i].after.unwrap()) != posting(changes[j], changes[j].after.unwrap())
}
pub open spec fn valid_indices(changes: Seq<IndexChange>, indices: Seq<usize>) -> bool {
    indices.no_duplicates()
    && forall|i: int| 0 <= i < indices.len() ==> indices[i] < changes.len()
        && changes[indices[i] as int].after.is_some()
}
pub open spec fn prefix_record(changes: Seq<IndexChange>, indices: Seq<usize>, n: int) -> bool {
    valid_indices(changes, indices)
    && (forall|i: int| 0 <= i < indices.len() ==> indices[i] < n)
    && (forall|j: int| 0 <= j < n ==> indices.contains(j as usize) == changes[j].after.is_some())
    && recorded(changes, indices) == destinations(changes, n)
}
pub open spec fn stable(a: State, b: State) -> bool {
    b.binding_count == a.binding_count && b.held == a.held
    && (a.poisoned ==> b.poisoned)
}
pub open spec fn removed_only(before: Set<Posting>, after: Set<Posting>, owned: Set<Posting>) -> bool {
    after.subset_of(before) && before.difference(owned).subset_of(after)
}

pub trait PostingPrimitives {
    spec fn state(&self) -> State;
    // Ownership is explicit: insertion requires a genuinely absent posting.
    // The primitive's failure frame is an assumption on native index insertion.
    fn transactional_insert(&mut self, binding: usize, key: usize, row: usize) -> (r: Result<(), Error>)
        requires binding < old(self).state().binding_count,
            old(self).state().held.contains((binding, key)),
            !old(self).state().postings.contains((binding, key, row)),
        ensures final(self).state() == (State {
            postings: if r.is_ok() { old(self).state().postings.insert((binding, key, row)) } else { old(self).state().postings },
            ..old(self).state()
        });
    fn transactional_remove(&mut self, binding: usize, key: &usize, row: &usize) -> (r: Result<(), Error>)
        requires binding < old(self).state().binding_count, old(self).state().held.contains((binding, *key)),
        ensures final(self).state().binding_count == old(self).state().binding_count,
            final(self).state().held == old(self).state().held,
            final(self).state().poisoned == old(self).state().poisoned,
            r.is_ok() ==> final(self).state().postings == old(self).state().postings.remove((binding, *key, *row)),
            r.is_err() ==> final(self).state().postings == old(self).state().postings
                || final(self).state().postings == old(self).state().postings.remove((binding, *key, *row));
    fn poison_indexes(&mut self)
        ensures final(self).state() == (State { poisoned: true, ..old(self).state() });
}

pub proof fn destination_member(changes: Seq<IndexChange>, i: int, n: int)
    requires 0 <= i < n <= changes.len(), changes[i].after.is_some(),
    ensures destinations(changes, n).contains(posting(changes[i], changes[i].after.unwrap())),
    decreases n,
{
    if i < n - 1 { destination_member(changes, i, n - 1); }
}

pub proof fn source_member(changes: Seq<IndexChange>, i: int, n: int)
    requires 0 <= i < n <= changes.len(), changes[i].before.is_some(),
    ensures sources(changes, n).contains(posting(changes[i], changes[i].before.unwrap())),
    decreases n,
{
    if i < n - 1 { source_member(changes, i, n - 1); }
}

pub proof fn destination_prefix_subset(changes: Seq<IndexChange>, n: int, m: int)
    requires 0 <= n <= m <= changes.len(),
    ensures destinations(changes, n).subset_of(destinations(changes, m)),
    decreases m - n,
{
    if n < m { destination_prefix_subset(changes, n, m - 1); }
}

pub proof fn restoration_frame(base: Set<Posting>, owned: Set<Posting>, after: Set<Posting>)
    requires base.disjoint(owned), removed_only(base.union(owned), after, owned),
    ensures base.subset_of(after), after.subset_of(base.union(owned)),
        after == base.union(owned).difference(owned) ==> after == base,
{
    assert(base.union(owned).difference(owned) =~= base);
}

pub proof fn destination_new(changes: Seq<IndexChange>, n: int)
    requires 0 <= n < changes.len(), unique_destinations(changes), changes[n].after.is_some(),
    ensures !destinations(changes, n).contains(posting(changes[n], changes[n].after.unwrap())),
    decreases n,
{
    if n > 0 {
        destination_not_prior(changes, n, n);
    }
}
pub proof fn destination_not_prior(changes: Seq<IndexChange>, i: int, n: int)
    requires 0 <= n <= i < changes.len(), unique_destinations(changes), changes[i].after.is_some(),
    ensures !destinations(changes, n).contains(posting(changes[i], changes[i].after.unwrap())),
    decreases n,
{
    if n > 0 { destination_not_prior(changes, i, n - 1); }
}

pub proof fn recorded_push(changes: Seq<IndexChange>, indices: Seq<usize>, idx: usize)
    ensures recorded(changes, indices.push(idx)) == recorded(changes, indices)
        .union(option_posting(changes[idx as int], changes[idx as int].after)),
{
    assert(indices.push(idx).drop_last() == indices);
}

pub proof fn prefix_record_push(changes: Seq<IndexChange>, indices: Seq<usize>, n: int)
    requires 0 <= n < changes.len(), n <= usize::MAX, prefix_record(changes, indices, n), changes[n].after.is_some(),
    ensures prefix_record(changes, indices.push(n as usize), n + 1),
{
    recorded_push(changes, indices, n as usize);
    reveal_with_fuel(destinations, 2);
    assert forall|i: int, j: int| 0 <= i < j < indices.push(n as usize).len()
        implies #[trigger] indices.push(n as usize)[i] != #[trigger] indices.push(n as usize)[j] by {
        if j < indices.len() { assert(indices[i] != indices[j]); }
    }
    assert(indices.push(n as usize).no_duplicates());
    assert forall|j: int| 0 <= j < n + 1 implies indices.push(n as usize).contains(j as usize) == changes[j].after.is_some() by {
        if j < n { assert(indices.contains(j as usize) == changes[j].after.is_some()); }
    }
}

pub proof fn prefix_record_skip(changes: Seq<IndexChange>, indices: Seq<usize>, n: int)
    requires 0 <= n < changes.len(), n <= usize::MAX, prefix_record(changes, indices, n), changes[n].after.is_none(),
    ensures prefix_record(changes, indices, n + 1),
{
    reveal_with_fuel(destinations, 2);
    assert(destinations(changes, n + 1) =~= destinations(changes, n));
    assert(!indices.contains(n as usize)) by {
        if indices.contains(n as usize) {
            let i = choose|i: int| 0 <= i < indices.len() && indices[i] == n as usize;
            assert(indices[i] < n);
        }
    }
}

pub proof fn recorded_member(changes: Seq<IndexChange>, indices: Seq<usize>, i: int)
    requires 0 <= i < indices.len(),
    ensures option_posting(changes[indices[i] as int], changes[indices[i] as int].after)
        .subset_of(recorded(changes, indices)),
    decreases indices.len(),
{
    if i < indices.len() - 1 { recorded_member(changes, indices.drop_last(), i); }
}

pub proof fn recorded_suffix(changes: Seq<IndexChange>, indices: Seq<usize>, n: int)
    requires 0 <= n < indices.len(),
    ensures recorded(changes, indices.subrange(n, indices.len() as int)) ==
        recorded(changes, indices.subrange(n + 1, indices.len() as int))
        .union(option_posting(changes[indices[n] as int], changes[indices[n] as int].after)),
    decreases indices.len() - n,
{
    if n + 1 < indices.len() {
        recorded_suffix(changes, indices.drop_last(), n);
        assert(indices.subrange(n, indices.len() as int).drop_last() == indices.drop_last().subrange(n, indices.len() as int - 1));
        assert(indices.subrange(n + 1, indices.len() as int).drop_last() == indices.drop_last().subrange(n + 1, indices.len() as int - 1));
    }
    reveal_with_fuel(recorded, 2);
    assert(recorded(changes, indices.subrange(n, indices.len() as int)) =~=
        recorded(changes, indices.subrange(n + 1, indices.len() as int))
        .union(option_posting(changes[indices[n] as int], changes[indices[n] as int].after)));
}

pub fn rollback_index_destinations<D: PostingPrimitives>(driver: &mut D, changes: &Vec<IndexChange>, inserted: &Vec<usize>)
    -> (result: Result<(), Error>)
    requires valid(old(driver).state(), changes@), valid_indices(changes@, inserted@),
    ensures stable(old(driver).state(), final(driver).state()),
        result.is_ok() ==> final(driver).state().poisoned == old(driver).state().poisoned,
        result.is_ok() ==> final(driver).state().postings == old(driver).state().postings.difference(recorded(changes@, inserted@)),
        result.is_err() ==> final(driver).state().poisoned,
        removed_only(old(driver).state().postings, final(driver).state().postings, recorded(changes@, inserted@)),
{
    let ghost initial = driver.state(); let mut rollback_error = None ;

        let mut ri = inserted.len();
        proof { assert(recorded(changes@, inserted@.subrange(ri as int, inserted.len() as int)) =~= Set::<Posting>::empty()); }
        while ri > 0
            invariant ri <= inserted.len(), initial == old(driver).state(),
                stable(initial, driver.state()), driver.state().poisoned == initial.poisoned,
                valid(initial, changes@), valid(driver.state(), changes@),
                valid_indices(changes@, inserted@),
                removed_only(initial.postings, driver.state().postings, recorded(changes@, inserted@)),
                rollback_error.is_none() ==> driver.state().postings == initial.postings.difference(
                    recorded(changes@, inserted@.subrange(ri as int, inserted.len() as int))),
            decreases ri,
        {
            ri = ri - 1;
            let idx = &inserted[ri];
            proof {
                recorded_suffix(changes@, inserted@, ri as int);
                recorded_member(changes@, inserted@, ri as int);
                assert(recorded(changes@, inserted@).contains(posting(changes@[*idx as int], changes@[*idx as int].after.unwrap())));
            }
     let previous = & changes [ * idx ] ;
if let Err ( undo ) = driver . transactional_remove ( previous . binding , previous . after . as_ref ( ) . expect ( "inserted destination" ) , & previous . row_id , ) {
    rollback_error = Some ( undo ) ;
}

        }
        proof { assert(inserted@.subrange(0, inserted.len() as int) == inserted@); }
     if let Some ( undo ) = rollback_error {
    driver . poison_indexes ( ) ;
    return Err ( Error :: Index ) ;
}
Ok ( ( ) )
}

pub fn prepare_index_destinations<D: PostingPrimitives>(driver: &mut D, changes: &Vec<IndexChange>)
    -> (result: Result<Vec<usize>, Error>)
    requires valid(old(driver).state(), changes@), unique_destinations(changes@),
        old(driver).state().postings.disjoint(destinations(changes@, changes.len() as int)),
    ensures stable(old(driver).state(), final(driver).state()),
        result.is_ok() ==> final(driver).state().poisoned == old(driver).state().poisoned,
        result.is_ok() ==> final(driver).state().postings == old(driver).state().postings.union(destinations(changes@, changes.len() as int)),
        result.is_ok() ==> prefix_record(changes@, result->Ok_0@, changes.len() as int),
        result.is_err() ==> final(driver).state().postings == old(driver).state().postings || final(driver).state().poisoned,
        old(driver).state().postings.subset_of(final(driver).state().postings),
        final(driver).state().postings.subset_of(old(driver).state().postings.union(destinations(changes@, changes.len() as int))),
{
    let ghost initial = driver.state(); let mut inserted = Vec :: new ( ) ;

        let mut change_idx: usize = 0;
        proof { assert(recorded(changes@, inserted@) =~= Set::<Posting>::empty()); }
        while change_idx < changes.len()
            invariant change_idx <= changes.len(), initial == old(driver).state(),
                stable(initial, driver.state()), driver.state().poisoned == initial.poisoned,
                valid(initial, changes@), valid(driver.state(), changes@),
                unique_destinations(changes@),
                initial.postings.disjoint(destinations(changes@, changes.len() as int)),
                driver.state().postings == initial.postings.union(destinations(changes@, change_idx as int)),
                prefix_record(changes@, inserted@, change_idx as int),
            decreases changes.len() - change_idx,
        {
            let change = &changes[change_idx];
            let ghost previous_indices = inserted@;
            proof {
                destination_prefix_subset(changes@, change_idx as int, changes.len() as int);
                assert(initial.postings.disjoint(destinations(changes@, change_idx as int)));
                if change.after.is_some() {
                    destination_member(changes@, change_idx as int, changes.len() as int);
                    destination_new(changes@, change_idx as int);
                }
            }
     if let Some ( after ) = & change . after {
    let index = change . binding ;
    if let Err ( err ) = driver . transactional_insert ( index , after . clone ( ) , change . row_id ) {
        proof { assert(driver.state().postings == initial.postings.union(recorded(changes@, inserted@))); } if let Err ( undo ) = rollback_index_destinations ( driver , changes , & inserted ) {
            proof { restoration_frame(initial.postings, recorded(changes@, inserted@), driver.state().postings); } return Err ( Error :: Index ) ;
        }
        proof { restoration_frame(initial.postings, recorded(changes@, inserted@), driver.state().postings); } return Err ( err ) ;
    }
    proof { recorded_push(changes@, previous_indices, change_idx); } inserted . push ( change_idx ) ;
}

            proof {
                if change.after.is_some() { prefix_record_push(changes@, previous_indices, change_idx as int); }
                else { prefix_record_skip(changes@, previous_indices, change_idx as int); }
                reveal_with_fuel(destinations, 2);
            }
            change_idx = change_idx + 1;
        }
     Ok ( inserted )
}

pub fn remove_index_sources<D: PostingPrimitives>(driver: &mut D, changes: &Vec<IndexChange>)
    -> (result: Result<(), Error>)
    requires valid(old(driver).state(), changes@),
    ensures stable(old(driver).state(), final(driver).state()),
        result.is_ok() ==> final(driver).state().poisoned == old(driver).state().poisoned,
        result.is_ok() ==> final(driver).state().postings == old(driver).state().postings.difference(sources(changes@, changes.len() as int)),
        result.is_err() ==> final(driver).state().poisoned,
        removed_only(old(driver).state().postings, final(driver).state().postings, sources(changes@, changes.len() as int)),
{
    let ghost initial = driver.state(); 
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                stable(initial, driver.state()), driver.state().poisoned == initial.poisoned,
                valid(initial, changes@), valid(driver.state(), changes@),
                driver.state().postings == initial.postings.difference(sources(changes@, ci as int)),
                removed_only(initial.postings, driver.state().postings, sources(changes@, changes.len() as int)),
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
            proof { if change.before.is_some() { source_member(changes@, ci as int, changes.len() as int); } }
     if let Some ( before ) = & change . before {
    if let Err ( err ) = driver . transactional_remove ( change . binding , before , & change . row_id ) {
        driver . poison_indexes ( ) ;
        return Err ( Error :: Index ) ;
    }
}

            proof { reveal_with_fuel(sources, 2); }
            ci = ci + 1;
        }
     Ok ( ( ) )
}
}

}
pub mod publication {
use crate::lookup;
use lookup::{Image, Row, Snapshot, Error};
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CommittedWrite { pub row_id:usize, pub base_offset:u32, pub new_offset:u32 }
pub struct CommitRecord { pub txid:u64, pub writes:Vec<CommittedWrite> }
pub open spec fn set_xmax(s:Image,p:u32,value:u64) -> Image {
    Image {rows:s.rows.insert(p,Row{xmax:value,..s.rows[p]}),..s}
}
pub open spec fn set_next(s:Image,p:u32,value:u32) -> Image {
    Image {rows:s.rows.insert(p,Row{next:value,..s.rows[p]}),..s}
}
pub open spec fn set_head(s:Image,id:usize,value:u32) -> Image {
    Image {heads:s.heads.insert(id,value),..s}
}
pub open spec fn publication_prefix(s:Image,w:CommittedWrite,writer:u64,phase:nat) -> Image {
    let deleted=if w.base_offset==0 {s} else {set_xmax(s,w.base_offset,writer)};
    let linked=set_next(deleted,w.new_offset,w.base_offset);
    if phase==0 {s} else if phase==1 {deleted} else if phase==2 {linked}
    else {set_head(linked,w.row_id,w.new_offset)}
}
pub open spec fn prepared(s:Image,w:CommittedWrite,writer:u64) -> bool {
    lookup::image_valid(s) && writer>0 && w.row_id<s.capacity
    && s.heads[w.row_id]==w.base_offset
    && w.new_offset!=0 && s.rows.contains_key(w.new_offset)
    && w.new_offset!=w.base_offset && s.rows[w.new_offset].xmin==writer
    && s.rows[w.new_offset].xmax==0
    && (w.base_offset==0 || (s.rows.contains_key(w.base_offset)
        && s.rows[w.base_offset].xmax==0 && s.rank[w.base_offset]<s.rank[w.new_offset]))
    && (forall|id:usize| id<s.capacity ==> s.heads[id]!=w.new_offset)
    && (forall|p:u32| #[trigger] s.rows.contains_key(p) && p!=w.new_offset ==> s.rows[p].next!=w.new_offset)
}
pub open spec fn allowed_prefix(before:Image,after:Image,w:CommittedWrite,writer:u64) -> bool {
    after==publication_prefix(before,w,writer,0) || after==publication_prefix(before,w,writer,1)
        || after==publication_prefix(before,w,writer,2) || after==publication_prefix(before,w,writer,3)
}

// The native row-partition guard excludes writers and vacuum on this projected
// chain; the prepared new allocation is exclusive. Each operation models one
// actual field access. These contracts do not supply a publication result.
pub trait PublicationStorage {
    spec fn image(&self)->Image;
    spec fn infallible(&self)->bool;
    // Exact row partition and exclusive prepared allocation, belonging to this
    // driver's physical arena. Native capability construction remains open.
    spec fn authorized(&self,row_id:usize,new_ptr:u32)->bool;
    fn slot_ref(&self,id:usize)->(r:Result<usize,Error>)
        requires id<self.image().capacity, exists|p:u32| self.authorized(id,p),
        ensures r.is_ok() ==> r.unwrap()==id, self.infallible() ==> r.is_ok();
    fn resolve(&self,p:u32)->(r:Result<u32,Error>)
        requires self.image().rows.contains_key(p),
        ensures r.is_ok() ==> r.unwrap()==p, self.infallible() ==> r.is_ok();
    fn compare_xmax(&mut self,p:u32,expected:u64,value:u64,Ghost(row_id):Ghost<usize>,Ghost(new_ptr):Ghost<u32>)->(r:Result<(),Error>)
        requires old(self).image().rows.contains_key(p), old(self).authorized(row_id,new_ptr),
            row_id<old(self).image().capacity, old(self).image().heads[row_id]==p,
        ensures final(self).infallible()==old(self).infallible(),
            forall|id:usize,q:u32| #[trigger] final(self).authorized(id,q)==old(self).authorized(id,q),
            r.is_ok() <==> old(self).image().rows[p].xmax==expected,
            final(self).image()==if r.is_ok() {set_xmax(old(self).image(),p,value)} else {old(self).image()};
    fn store_next(&mut self,p:u32,value:u32,Ghost(row_id):Ghost<usize>)
        requires old(self).image().rows.contains_key(p), old(self).authorized(row_id,p),
        ensures final(self).infallible()==old(self).infallible(),
            forall|id:usize,q:u32| #[trigger] final(self).authorized(id,q)==old(self).authorized(id,q),
            final(self).image()==set_next(old(self).image(),p,value);
    fn compare_head(&mut self,id:usize,expected:u32,value:u32)->(r:Result<(),Error>)
        requires id<old(self).image().capacity, old(self).authorized(id,value),
        ensures final(self).infallible()==old(self).infallible(),
            forall|row:usize,q:u32| #[trigger] final(self).authorized(row,q)==old(self).authorized(row,q),
            r.is_ok() <==> old(self).image().heads[id]==expected,
            final(self).image()==if r.is_ok() {set_head(old(self).image(),id,value)} else {old(self).image()};
}

pub fn publish_prepared_write_set<D:PublicationStorage>(driver:&mut D,record:&CommitRecord)
    ->(r:Result<(),Error>)
    requires record.writes.len()==1, prepared(old(driver).image(),record.writes[0],record.txid),
        old(driver).authorized(record.writes[0].row_id,record.writes[0].new_offset),
    ensures allowed_prefix(old(driver).image(),final(driver).image(),record.writes[0],record.txid),
        r.is_ok() ==> final(driver).image()==publication_prefix(old(driver).image(),record.writes[0],record.txid,3),
        r.is_err() ==> final(driver).image()==publication_prefix(old(driver).image(),record.writes[0],record.txid,0)
            || final(driver).image()==publication_prefix(old(driver).image(),record.writes[0],record.txid,1),
        old(driver).infallible() ==> r.is_ok(),
        final(driver).infallible()==old(driver).infallible(),
        final(driver).authorized(record.writes[0].row_id,record.writes[0].new_offset),
{
    
    let ghost initial=driver.image();
    let ghost initially_infallible=driver.infallible();
    let mut i=0;
    while i<record.writes.len()
        invariant initial==old(driver).image(), initially_infallible==old(driver).infallible(),
            record.writes.len()==1,i<=record.writes.len(),
            prepared(initial,record.writes[0],record.txid),
            driver.infallible()==initially_infallible,
            driver.authorized(record.writes[0].row_id,record.writes[0].new_offset),
            driver.image()==publication_prefix(initial,record.writes[0],record.txid,if i==0 {0} else {3}),
        decreases record.writes.len()-i,
    {
        let write=&record.writes[i];
     let slot = driver . slot_ref ( write . row_id ) ? ;
if write . base_offset != 0 {
    let base_row = driver . resolve ( write . base_offset ) ? ;
    if driver . compare_xmax ( base_row , 0 , record . txid , Ghost ( write . row_id ) , Ghost ( write . new_offset ) ) . is_err ( ) {
        return Err ( Error :: SerializationFailure ) ;
    }
}
let new_row = driver . resolve ( write . new_offset ) ? ;
driver . store_next ( new_row , write . base_offset , Ghost ( write . row_id ) ) ;
if driver . compare_head ( slot , write . base_offset , write . new_offset ) . is_err ( ) {
    return Err ( Error :: SerializationFailure ) ;
}

        proof {assert(driver.image()==publication_prefix(initial,record.writes[0],record.txid,3));}
        i+=1;
    }
     Ok ( ( ) )
}

pub proof fn image_head_present(s:Image,id:usize)
    requires lookup::image_valid(s),id<s.capacity,
    ensures s.heads.contains_key(id) && (s.heads[id]==0 || s.rows.contains_key(s.heads[id])),
{
    // Materialize the selected head to instantiate the native image's head
    // quantifier (whose trigger is the indexed head, not domain membership).
    let head=s.heads[id];
    assert(head==0 || head>0);
}
pub proof fn publication_prefix_valid(s:Image,w:CommittedWrite,writer:u64,phase:nat)
    requires prepared(s,w,writer), phase<=3,
    ensures lookup::image_valid(publication_prefix(s,w,writer,phase)),
        publication_prefix(s,w,writer,phase).capacity==s.capacity,
        publication_prefix(s,w,writer,phase).rows.dom()==s.rows.dom(),
        forall|p:u32| #[trigger] s.rows.contains_key(p) ==>
            publication_prefix(s,w,writer,phase).rows[p].value==s.rows[p].value,
{
    let after=publication_prefix(s,w,writer,phase);
    reveal(lookup::image_valid);
    assert(lookup::image_valid(s));
    assert(after.capacity==s.capacity);
    assert forall|id:usize| id<after.capacity implies after.heads.contains_key(id)
        && (#[trigger] after.heads[id]==0 || after.rows.contains_key(after.heads[id])) by {
        assert(id<s.capacity);
        image_head_present(s,id);
        assert(s.heads.contains_key(id) && (s.heads[id]==0 || s.rows.contains_key(s.heads[id])));
        if phase==3 && id==w.row_id {assert(after.heads[id]==w.new_offset);}
        else {assert(after.heads[id]==s.heads[id]);}
    }
    assert forall|p:u32| #[trigger] after.rows.contains_key(p) implies p!=0 && after.rank.contains_key(p)
        && (after.rows[p].next==0 || after.rows.contains_key(after.rows[p].next)
            && after.rank[after.rows[p].next]<after.rank[p]) by {
        assert(s.rows.contains_key(p));
        if phase>=2 && p==w.new_offset {assert(after.rows[p].next==w.base_offset);}
        else {assert(after.rows[p].next==s.rows[p].next);}
    }

}

pub proof fn retained_chain_frame(before:Image,after:Image,tx:Snapshot,ptr:u32,new_ptr:u32)
    requires lookup::image_valid(before),lookup::image_valid(after),ptr==0 || before.rows.contains_key(ptr),ptr!=new_ptr,
        forall|p:u32| #[trigger] before.rows.contains_key(p) && p!=new_ptr ==> after.rows.contains_key(p)
            && before.rows[p].next!=new_ptr && before.rows[p].next==after.rows[p].next
            && lookup::visible(before.rows[p],tx)==lookup::visible(after.rows[p],tx),
    ensures lookup::first_visible(before,ptr,tx)==lookup::first_visible(after,ptr,tx),
    decreases if ptr==0 {0nat} else {before.rank[ptr]+1},
{
    if ptr!=0 && !lookup::visible(before.rows[ptr],tx) {
        retained_chain_frame(before,after,tx,before.rows[ptr].next,new_ptr);
    }
}
pub proof fn publication_preserves_old_snapshot(before:Image,w:CommittedWrite,writer:u64,phase:nat,tx:Snapshot)
    requires prepared(before,w,writer),phase<=3,writer!=tx.txid,
        !lookup::creator_visible(writer,tx),writer>=tx.xmax || tx.active.contains(writer),
    ensures lookup::first_visible(before,before.heads[w.row_id],tx)
        ==lookup::first_visible(publication_prefix(before,w,writer,phase),publication_prefix(before,w,writer,phase).heads[w.row_id],tx),
        forall|p:u32| #[trigger] before.rows.contains_key(p) ==>
            publication_prefix(before,w,writer,phase).rows[p].value==before.rows[p].value,
{
    let after=publication_prefix(before,w,writer,phase);
    publication_prefix_valid(before,w,writer,phase);
    assert forall|p:u32| before.rows.contains_key(p) && p!=w.new_offset implies after.rows.contains_key(p)
        && before.rows[p].next!=w.new_offset && before.rows[p].next==after.rows[p].next
        && lookup::visible(before.rows[p],tx)==lookup::visible(after.rows[p],tx) by {
        if p==w.base_offset {assert(before.rows[p].xmax==0);}
    }
    retained_chain_frame(before,after,tx,before.heads[w.row_id],w.new_offset);
    if phase==3 {
        assert(!lookup::visible(after.rows[w.new_offset],tx));
        assert(after.rows[w.new_offset].next==w.base_offset);
    }
}
pub proof fn successful_publication_connects_lookup(before:Image,after:Image,w:CommittedWrite,writer:u64,tx:Snapshot)
    requires prepared(before,w,writer),after==publication_prefix(before,w,writer,3),writer!=tx.txid,
        !lookup::creator_visible(writer,tx),writer>=tx.xmax || tx.active.contains(writer),
    ensures lookup::image_valid(after),after.heads[w.row_id]==w.new_offset,
        after.rows[w.new_offset].next==w.base_offset,
        w.base_offset!=0 ==> after.rows[w.base_offset].xmax==writer,
        lookup::first_visible(before,before.heads[w.row_id],tx)==lookup::first_visible(after,after.heads[w.row_id],tx),
{
    publication_prefix_valid(before,w,writer,3);
    publication_preserves_old_snapshot(before,w,writer,3,tx);
}
pub proof fn publication_has_live_witness()
    ensures exists|s:Image,w:CommittedWrite,tx:Snapshot|
        prepared(s,w,9) && w.base_offset!=0 && !lookup::creator_visible(9,tx)
        && lookup::first_visible(s,s.heads[w.row_id],tx)==Some(w.base_offset)
        && lookup::first_visible(publication_prefix(s,w,9,3),w.new_offset,tx)==Some(w.base_offset),
{
    let base=Row{xmin:1,xmax:0,next:0,value:Some(42),locked:false,owner:0};
    let fresh=Row{xmin:9,xmax:0,next:1,value:Some(77),locked:false,owner:0};
    let s=Image{heads:Map::empty().insert(0,1),rows:Map::empty().insert(1,base).insert(2,fresh),
        rank:Map::empty().insert(1,0).insert(2,1),capacity:1};
    let w=CommittedWrite{row_id:0,base_offset:1,new_offset:2};
    let tx=Snapshot{txid:4,xmin:2,xmax:5,active:Seq::empty()};
    assert(lookup::image_valid(s));
    assert(prepared(s,w,9));
    publication_preserves_old_snapshot(s,w,9,3,tx);
    assert(lookup::first_visible(s,1,tx)==Some(1));
    assert(exists|image:Image,write:CommittedWrite,snap:Snapshot|
        prepared(image,write,9) && write.base_offset!=0 && !lookup::creator_visible(9,snap)
        && lookup::first_visible(image,image.heads[write.row_id],snap)==Some(write.base_offset)
        && lookup::first_visible(publication_prefix(image,write,9,3),write.new_offset,snap)==Some(write.base_offset));
}

// Executable consistency witness for all field contracts, using fixed arena
// offsets 1/2 and one held row partition. This is a model implementation, not
// a claim that the native unsafe allocator already refines these contracts.
pub struct Memory { pub head:u32, pub base:Row, pub fresh:Row }
impl PublicationStorage for Memory {
    open spec fn image(&self)->Image {
        Image{heads:Map::empty().insert(0,self.head),
            rows:Map::empty().insert(1,self.base).insert(2,self.fresh),
            rank:Map::empty().insert(1,0nat).insert(2,1nat),capacity:1}
    }
    open spec fn infallible(&self)->bool { true }
    open spec fn authorized(&self,row_id:usize,new_ptr:u32)->bool { row_id==0 && new_ptr==2 }
    fn slot_ref(&self,id:usize)->(r:Result<usize,Error>) { Ok(id) }
    fn resolve(&self,p:u32)->(r:Result<u32,Error>) { Ok(p) }
    fn compare_xmax(&mut self,p:u32,expected:u64,value:u64,Ghost(row_id):Ghost<usize>,Ghost(new_ptr):Ghost<u32>)->(r:Result<(),Error>) {
        let ghost before=self.image();
        let found=if p==1 {self.base.xmax} else {self.fresh.xmax};
        if found!=expected {return Err(Error::SerializationFailure);}
        if p==1 {self.base.xmax=value;} else {self.fresh.xmax=value;}
        proof {assert(self.image().rows =~= set_xmax(before,p,value).rows);}
        Ok(())
    }
    fn store_next(&mut self,p:u32,value:u32,Ghost(row_id):Ghost<usize>) {
        let ghost before=self.image();
        self.fresh.next=value;
        proof {assert(self.image().rows =~= set_next(before,p,value).rows);}
    }
    fn compare_head(&mut self,id:usize,expected:u32,value:u32)->(r:Result<(),Error>) {
        let ghost before=self.image();
        if self.head!=expected {return Err(Error::SerializationFailure);}
        self.head=value;
        proof {assert(self.image().heads =~= set_head(before,id,value).heads);}
        Ok(())
    }
}
pub fn native_publication_has_executable_witness()->(result:bool)
    ensures result,
{
    let mut driver=Memory{head:1,
        base:Row{xmin:1,xmax:0,next:0,value:Some(42),locked:false,owner:0},
        fresh:Row{xmin:9,xmax:0,next:1,value:Some(77),locked:false,owner:0}};
    let write=CommittedWrite{row_id:0,base_offset:1,new_offset:2};
    let mut writes=Vec::new();writes.push(write);
    let record=CommitRecord{txid:9,writes};
    let ghost before=driver.image();
    let ghost tx=Snapshot{txid:4,xmin:2,xmax:5,active:Seq::empty()};
    proof {assert(prepared(before,write,9));}
    let published=publish_prepared_write_set(&mut driver,&record);
    proof {
        successful_publication_connects_lookup(before,driver.image(),write,9,tx);
        assert(lookup::first_visible(before,1,tx)==Some(1));
        assert(lookup::first_visible(driver.image(),driver.head,tx)==Some(1));
    }
    published.is_ok() && driver.head==2 && driver.base.xmax==9 && driver.fresh.next==1
        && driver.base.value==Some(42) && driver.fresh.value==Some(77)
}
}

}
pub mod ordinary {
use crate::lookup;
use crate::publication;
use publication::PublicationStorage;
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
#[derive(Clone,Copy,PartialEq,Eq)]
pub struct Write {pub row_id:usize,pub base_offset:u32,pub new_offset:u32,pub dirty_columns_bitmask:u64}
pub struct Transaction {pub txid:u64,pub write_set:Vec<Write>}
#[derive(Clone,Copy,PartialEq,Eq)]
pub struct PublishedWrite {
    pub row_id:usize,pub base_offset:u32,pub new_offset:u32,
    pub base_value:Option<usize>,pub value:Option<usize>,pub dirty_columns_bitmask:u64,
}
pub open spec fn row_write(w:Write)->publication::CommittedWrite {
    publication::CommittedWrite{row_id:w.row_id,base_offset:w.base_offset,new_offset:w.new_offset}
}
pub open spec fn admitted(image:lookup::Image,tx:Transaction,indices:Seq<usize>)->bool {
    indices.len()==1 && indices[0]<tx.write_set.len()
    && publication::prepared(image,row_write(tx.write_set[indices[0] as int]),tx.txid)
}
pub open spec fn report(image:lookup::Image,w:Write)->PublishedWrite {
    PublishedWrite{row_id:w.row_id,base_offset:w.base_offset,new_offset:w.new_offset,
        base_value:if w.base_offset==0 {image.rows[w.new_offset].value} else {image.rows[w.base_offset].value},
        value:image.rows[w.new_offset].value,dirty_columns_bitmask:w.dirty_columns_bitmask}
}
// Immutable native value loads use the same projected key value as lookup.
// Full T payload/key-extractor correspondence is a declared native boundary.
pub trait OrdinaryStorage:PublicationStorage {
    fn load_value(&self,p:u32)->(value:Option<usize>)
        requires self.image().rows.contains_key(p),
        ensures value==self.image().rows[p].value;
}
pub fn publish_write_set<D:OrdinaryStorage>(driver:&mut D,tx:&Transaction,indices:&Vec<usize>)
    ->(result:Result<Vec<PublishedWrite>,lookup::Error>)
    requires admitted(old(driver).image(),*tx,indices@),
        old(driver).authorized(tx.write_set[indices[0] as int].row_id,tx.write_set[indices[0] as int].new_offset),
    ensures result.is_ok() ==> final(driver).image()==publication::publication_prefix(old(driver).image(),
            row_write(tx.write_set[indices[0] as int]),tx.txid,3),
        result.is_ok() ==> result->Ok_0.len()==1 && result->Ok_0[0]==report(old(driver).image(),tx.write_set[indices[0] as int]),
        result.is_err() ==> final(driver).image()==publication::publication_prefix(old(driver).image(),
                row_write(tx.write_set[indices[0] as int]),tx.txid,0)
            || final(driver).image()==publication::publication_prefix(old(driver).image(),
                row_write(tx.write_set[indices[0] as int]),tx.txid,1),
        old(driver).infallible() ==> result.is_ok(),
        final(driver).infallible()==old(driver).infallible(),
        final(driver).authorized(tx.write_set[indices[0] as int].row_id,tx.write_set[indices[0] as int].new_offset),
{
    let mut published = Vec :: with_capacity ( indices . len ( ) ) ;

        let ghost initial=driver.image();
        let ghost initially_infallible=driver.infallible();
        let mut i=0;
        while i<indices.len()
            invariant i<=indices.len(), admitted(initial,*tx,indices@),initial==old(driver).image(),
                driver.infallible()==initially_infallible,initially_infallible==old(driver).infallible(),
                driver.authorized(tx.write_set[indices[0] as int].row_id,tx.write_set[indices[0] as int].new_offset),
                driver.image()==publication::publication_prefix(initial,row_write(tx.write_set[indices[0] as int]),tx.txid,if i==0 {0} else {3}),
                published.len()==i,
                i==1 ==> published[0]==report(initial,tx.write_set[indices[0] as int]),
            decreases indices.len()-i,
        {
            let idx=&indices[i];
     let write = & tx . write_set [ * idx ] ;
let slot = driver . slot_ref ( write . row_id ) ? ;
let base_offset = write . base_offset ;
let new_offset = write . new_offset ;
let base_value = if base_offset != 0 {
    let base_row = driver . resolve ( write . base_offset ) ? ;
    if driver . compare_xmax ( base_row , 0 , tx . txid , Ghost ( write . row_id ) , Ghost ( write . new_offset ) ) . is_err ( ) {
        return Err ( lookup :: Error :: SerializationFailure ) ;
    }
    driver . load_value ( base_row )
}
else {
    let new_row = driver . resolve ( write . new_offset ) ? ;
    driver . load_value ( new_row )
}
;
let new_row = driver . resolve ( write . new_offset ) ? ;
driver . store_next ( new_row , base_offset , Ghost ( write . row_id ) ) ;
if driver . compare_head ( slot , base_offset , new_offset ) . is_err ( ) {
    return Err ( lookup :: Error :: SerializationFailure ) ;
}
published . push ( PublishedWrite {
    row_id : write . row_id , base_offset , new_offset , base_value , value : driver . load_value ( new_row ) , dirty_columns_bitmask : write . dirty_columns_bitmask ,
}
) ;

            proof {assert(published[0]==report(initial,tx.write_set[indices[0] as int]));}
            i+=1;
        }
     Ok ( published )
}
impl OrdinaryStorage for publication::Memory {
    fn load_value(&self,p:u32)->(value:Option<usize>) {
        if p==1 {self.base.value} else {self.fresh.value}
    }
}
}

}
use vstd::prelude::*;
use crate::postings::PostingPrimitives;
use crate::publication::PublicationStorage;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

// Disjoint posting and row views of one admitted native storage projection.
// Native arena/guard correspondence is an instantiation obligation; neither
// component may alias the other's logical field domain through its primitive.
pub struct Storage<P:PostingPrimitives,R:PublicationStorage> {
    pub postings:P,
    pub rows:R,
}
pub struct DataState { pub postings:postings::State, pub image:lookup::Image }
impl<P:PostingPrimitives,R:PublicationStorage> Storage<P,R> {
    pub open spec fn view(&self)->DataState { DataState{postings:self.postings.state(),image:self.rows.image()} }
}
pub struct Plan { pub changes:Vec<postings::IndexChange>, pub record:publication::CommitRecord }
pub enum DataError { Posting(postings::Error), Publication(lookup::Error) }

pub open spec fn head_key(image:lookup::Image,id:usize)->Option<usize> {
    if image.heads[id]==0 {None} else {image.rows[image.heads[id]].value}
}
pub open spec fn row_postings_match(s:DataState,binding:usize,row:usize)->bool {
    forall|key:usize| #[trigger] s.postings.postings.contains((binding,key,row))
        <==> head_key(s.image,row)==Some(key)
}
pub open spec fn planned(before:DataState,plan:Plan)->bool {
    plan.changes.len()==1 && plan.record.writes.len()==1
    && plan.changes[0].row_id==plan.record.writes[0].row_id
    && plan.changes[0].before!=plan.changes[0].after
    && plan.changes[0].before==head_key(before.image,plan.changes[0].row_id)
    && before.image.rows[plan.record.writes[0].new_offset].value==plan.changes[0].after
    && publication::prepared(before.image,plan.record.writes[0],plan.record.txid)
    && postings::valid(before.postings,plan.changes@)
    && !before.postings.poisoned
    && row_postings_match(before,plan.changes[0].binding,plan.changes[0].row_id)
}
pub open spec fn destinations(plan:Plan)->Set<postings::Posting> {
    postings::destinations(plan.changes@,plan.changes.len() as int)
}
pub open spec fn sources(plan:Plan)->Set<postings::Posting> {
    postings::sources(plan.changes@,plan.changes.len() as int)
}
pub open spec fn installed(before:DataState,after:DataState,plan:Plan)->bool {
    after.image==before.image
    && postings::stable(before.postings,after.postings)
    && after.postings.poisoned==before.postings.poisoned
    && after.postings.postings==before.postings.postings.union(destinations(plan))
}
pub open spec fn removed(before:DataState,after:DataState,plan:Plan)->bool {
    after.image==before.image
    && postings::stable(before.postings,after.postings)
    && after.postings.poisoned==before.postings.poisoned
    && after.postings.postings==before.postings.postings.union(destinations(plan)).difference(sources(plan))
}
pub open spec fn committed(before:DataState,after:DataState,plan:Plan)->bool {
    after.image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,3)
    && postings::stable(before.postings,after.postings)
    && after.postings.poisoned==before.postings.poisoned
    && after.postings.postings==before.postings.postings.union(destinations(plan)).difference(sources(plan))
    && row_postings_match(after,plan.changes[0].binding,plan.changes[0].row_id)
}
pub open spec fn unchanged_data(before:DataState,after:DataState)->bool {
    before.image==after.image && before.postings.postings==after.postings.postings
}

pub proof fn planned_derives_destination_ownership(before:DataState,plan:Plan)
    requires planned(before,plan),
    ensures postings::unique_destinations(plan.changes@),
        before.postings.postings.disjoint(destinations(plan)),
        sources(plan).disjoint(destinations(plan)),
{
    reveal_with_fuel(postings::destinations,2);
    reveal_with_fuel(postings::sources,2);
    let c=plan.changes[0];
    assert forall|p:postings::Posting| #[trigger] destinations(plan).contains(p)
        implies !before.postings.postings.contains(p) by {
        assert(c.after.is_some());
        assert(p==(c.binding,c.after.unwrap(),c.row_id));
        assert(head_key(before.image,c.row_id)!=Some(c.after.unwrap()));
    }
    assert(before.postings.postings.disjoint(destinations(plan)));
    assert(sources(plan).disjoint(destinations(plan)));
}

pub proof fn complete_postings_match_published_row(before:DataState,after:DataState,plan:Plan)
    requires planned(before,plan),
        after.image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,3),
        after.postings.postings==before.postings.postings.union(destinations(plan)).difference(sources(plan)),
    ensures row_postings_match(after,plan.changes[0].binding,plan.changes[0].row_id),
        head_key(after.image,plan.changes[0].row_id)==plan.changes[0].after,
        forall|binding:usize,key:usize,row:usize|
            binding!=plan.changes[0].binding || row!=plan.changes[0].row_id ==>
            after.postings.postings.contains((binding,key,row))==before.postings.postings.contains((binding,key,row)),
{
    reveal_with_fuel(postings::destinations,2);
    reveal_with_fuel(postings::sources,2);
    let c=plan.changes[0];
    assert(head_key(after.image,c.row_id)==c.after);
    assert forall|key:usize| #[trigger] after.postings.postings.contains((c.binding,key,c.row_id))
        <==> head_key(after.image,c.row_id)==Some(key) by {
        assert(before.postings.postings.contains((c.binding,key,c.row_id)) == (c.before==Some(key)));
    }
}

pub fn prepare_destinations<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan)
    ->(result:Result<Vec<usize>,postings::Error>)
    requires planned(old(storage).view(),*plan),
    ensures final(storage).rows==old(storage).rows,
        postings::stable(old(storage).view().postings,final(storage).view().postings),
        result.is_ok() ==> final(storage).view().postings.poisoned==old(storage).view().postings.poisoned,
        result.is_ok() ==> installed(old(storage).view(),final(storage).view(),*plan),
        result.is_ok() ==> postings::prefix_record(plan.changes@,result->Ok_0@,plan.changes.len() as int),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
{
    proof {planned_derives_destination_ownership(old(storage).view(),*plan);}
    postings::prepare_index_destinations(&mut storage.postings,&plan.changes)
}

pub fn rollback_destinations<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan,
    inserted:&Vec<usize>,Ghost(before):Ghost<DataState>)->(result:Result<(),postings::Error>)
    requires planned(before,*plan),installed(before,old(storage).view(),*plan),
        postings::prefix_record(plan.changes@,inserted@,plan.changes.len() as int),
    ensures final(storage).rows==old(storage).rows,
        postings::stable(old(storage).view().postings,final(storage).view().postings),
        result.is_ok() ==> final(storage).view().postings.poisoned==old(storage).view().postings.poisoned,
        result.is_ok() ==> unchanged_data(before,final(storage).view()),
        result.is_err() ==> final(storage).view().postings.poisoned,
{
    proof {planned_derives_destination_ownership(before,*plan);}
    let result=postings::rollback_index_destinations(&mut storage.postings,&plan.changes,inserted);
    proof {
        if result.is_ok() {
            assert(storage.view().postings.postings =~= before.postings.postings);
        }
    }
    result
}

pub fn remove_sources<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan,
    Ghost(before):Ghost<DataState>)->(result:Result<(),postings::Error>)
    requires planned(before,*plan),installed(before,old(storage).view(),*plan),
    ensures final(storage).rows==old(storage).rows,
        postings::stable(old(storage).view().postings,final(storage).view().postings),
        result.is_ok() ==> final(storage).view().postings.poisoned==old(storage).view().postings.poisoned,
        result.is_ok() ==> removed(before,final(storage).view(),*plan),
        result.is_err() ==> final(storage).view().postings.poisoned,
        postings::removed_only(old(storage).view().postings.postings,final(storage).view().postings.postings,sources(*plan)),
{
    postings::remove_index_sources(&mut storage.postings,&plan.changes)
}

pub fn publish_rows<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan,
    Ghost(before):Ghost<DataState>)->(result:Result<(),lookup::Error>)
    requires planned(before,*plan),removed(before,old(storage).view(),*plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures final(storage).postings==old(storage).postings,
        result.is_ok() ==> committed(before,final(storage).view(),*plan),
        result.is_err() ==> final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,0)
            || final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,1),
        old(storage).rows.infallible() ==> result.is_ok(),
{
    let result=publication::publish_prepared_write_set(&mut storage.rows,&plan.record);
    proof {if result.is_ok() {complete_postings_match_published_row(before,storage.view(),*plan);}}
    result
}

pub fn poison<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>)
    ensures final(storage).rows==old(storage).rows,
        final(storage).view().postings==(postings::State{poisoned:true,..old(storage).view().postings}),
{
    storage.postings.poison_indexes();
}

// Data-only composition of the commit interval. The native orchestration and
// its actual failure branches are composed separately by the caller campaign.
pub fn publish_indexed_write<P:PostingPrimitives,R:PublicationStorage>(storage:&mut Storage<P,R>,plan:&Plan)
    ->(result:Result<(),DataError>)
    requires planned(old(storage).view(),*plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures result.is_ok() ==> committed(old(storage).view(),final(storage).view(),*plan),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
{
    let ghost before=storage.view();
    let inserted=match prepare_destinations(storage,plan) {
        Ok(inserted)=>inserted,
        Err(err)=>return Err(DataError::Posting(err)),
    };
    match remove_sources(storage,plan,Ghost(before)) {
        Ok(())=>{},
        Err(err)=>return Err(DataError::Posting(err)),
    }
    match publish_rows(storage,plan,Ghost(before)) {
        Ok(())=>Ok(()),
        Err(err)=>{poison(storage);Err(DataError::Publication(err))},
    }
}

pub struct OrdinaryPlan {pub tx:ordinary::Transaction,pub indices:Vec<usize>}
pub open spec fn ordinary_matches(plan:Plan,ordinary_plan:OrdinaryPlan)->bool {
    ordinary_plan.indices.len()==1 && ordinary_plan.indices[0]<ordinary_plan.tx.write_set.len()
    && plan.record.writes.len()==1 && ordinary_plan.tx.txid==plan.record.txid
    && ordinary::row_write(ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int])==plan.record.writes[0]
}
pub fn publish_rows_ordinary<P:PostingPrimitives,R:ordinary::OrdinaryStorage>(storage:&mut Storage<P,R>,
    plan:&Plan,ordinary_plan:&OrdinaryPlan,Ghost(before):Ghost<DataState>)
    ->(result:Result<Vec<ordinary::PublishedWrite>,lookup::Error>)
    requires planned(before,*plan),removed(before,old(storage).view(),*plan),ordinary_matches(*plan,*ordinary_plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures final(storage).postings==old(storage).postings,
        result.is_ok() ==> committed(before,final(storage).view(),*plan),
        result.is_ok() ==> result->Ok_0.len()==1 && result->Ok_0[0]==ordinary::report(before.image,
            ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),
        result.is_err() ==> final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,0)
            || final(storage).view().image==publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,1),
        old(storage).rows.infallible() ==> result.is_ok(),
{
    let result=ordinary::publish_write_set(&mut storage.rows,&ordinary_plan.tx,&ordinary_plan.indices);
    proof {if result.is_ok() {complete_postings_match_published_row(before,storage.view(),*plan);}}
    result
}
pub fn native_ordinary_data_segment<P:PostingPrimitives,R:ordinary::OrdinaryStorage>(storage:&mut Storage<P,R>,
    plan:&Plan,ordinary_plan:&OrdinaryPlan)->(result:Result<Vec<ordinary::PublishedWrite>,DataError>)
    requires planned(old(storage).view(),*plan),ordinary_matches(*plan,*ordinary_plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
    ensures result.is_ok() ==> committed(old(storage).view(),final(storage).view(),*plan),
        result.is_ok() ==> result->Ok_0.len()==1 && result->Ok_0[0]==ordinary::report(old(storage).view().image,
            ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
{
    let ghost before=storage.view();
let inserted = match prepare_destinations ( storage , plan ) {
    Ok ( value ) => value , Err ( err ) => return Err ( DataError :: Posting ( err ) ) ,
}
;
match remove_sources ( storage , plan , Ghost ( before ) ) {
    Ok ( value ) => value , Err ( err ) => return Err ( DataError :: Posting ( err ) ) ,
}
;
let publication = publish_rows_ordinary ( storage , plan , ordinary_plan , Ghost ( before ) ) ;
let writes = match publication {
    Ok ( writes ) => writes , Err ( err ) => {
        poison ( storage ) ;
        return Err ( DataError :: Publication ( err ) ) ;
    }
}
;
Ok(writes)
}


// A consistency implementation of the posting primitives. Its finite logical
// image is ghost state; it is deliberately not offered as a native index.
pub struct PostingMemory {
    pub logical:Ghost<postings::State>,pub fail_insert:bool,pub fail_remove:bool,pub remove_then_fail:bool,
}
impl PostingPrimitives for PostingMemory {
    open spec fn state(&self)->postings::State {self.logical@}
    fn transactional_insert(&mut self,binding:usize,key:usize,row:usize)->(result:Result<(),postings::Error>) {
        if self.fail_insert {return Err(postings::Error::Primitive);}
        proof {self.logical=Ghost(postings::State{postings:self.logical@.postings.insert((binding,key,row)),..self.logical@});}
        Ok(())
    }
    fn transactional_remove(&mut self,binding:usize,key:&usize,row:&usize)->(result:Result<(),postings::Error>) {
        if !self.fail_remove || self.remove_then_fail {
            proof {self.logical=Ghost(postings::State{postings:self.logical@.postings.remove((binding,*key,*row)),..self.logical@});}
        }
        if self.fail_remove {Err(postings::Error::Primitive)} else {Ok(())}
    }
    fn poison_indexes(&mut self) {
        proof {self.logical=Ghost(postings::State{poisoned:true,..self.logical@});}
    }
}
pub fn admitted_input_witness(before_key:Option<usize>,after_key:Option<usize>)
    ->(result:(Storage<PostingMemory,publication::Memory>,Plan,OrdinaryPlan))
    requires before_key!=after_key,
    ensures planned(result.0.view(),result.1),ordinary_matches(result.1,result.2),
        result.0.rows.authorized(result.1.record.writes[0].row_id,result.1.record.writes[0].new_offset),
        result.1.changes[0].before==before_key,result.1.changes[0].after==after_key,
{
    let base=lookup::Row{xmin:1,xmax:0,next:0,value:before_key,locked:false,owner:0};
    let fresh=lookup::Row{xmin:9,xmax:0,next:0,value:after_key,locked:false,owner:0};
    let head=if before_key.is_some() {1u32} else {0u32};
    let rows=publication::Memory{head,base,fresh};
    let ghost before_held=if before_key.is_some() {Set::<(usize,usize)>::empty().insert((0,before_key.unwrap()))}
        else {Set::<(usize,usize)>::empty()};
    let ghost held=if after_key.is_some() {before_held.insert((0,after_key.unwrap()))} else {before_held};
    let ghost initial_posts=if before_key.is_some() {
        Set::<postings::Posting>::empty().insert((0,before_key.unwrap(),0))
    } else {Set::<postings::Posting>::empty()};
    let postings=PostingMemory{logical:Ghost(postings::State{postings:initial_posts,binding_count:1,held,poisoned:false}),
        fail_insert:false,fail_remove:false,remove_then_fail:false};
    let storage=Storage{postings,rows};
    let change=postings::IndexChange{binding:0,row_id:0,before:before_key,after:after_key};
    let mut changes=Vec::new();changes.push(change);
    let write=publication::CommittedWrite{row_id:0,base_offset:head,new_offset:2};
    let mut writes=Vec::new();writes.push(write);
    let plan=Plan{changes,record:publication::CommitRecord{txid:9,writes}};
    let ordinary_write=ordinary::Write{row_id:0,base_offset:head,new_offset:2,dirty_columns_bitmask:37};
    let mut write_set=Vec::new();write_set.push(ordinary_write);
    let tx=ordinary::Transaction{txid:9,write_set};
    let mut indices=Vec::new();indices.push(0usize);
    let ordinary_plan=OrdinaryPlan{tx,indices};
    proof {
        assert(lookup::image_valid(storage.view().image));
        assert(publication::prepared(storage.view().image,write,9));
        assert(row_postings_match(storage.view(),0,0));
        assert(planned(storage.view(),plan));
    }
    (storage,plan,ordinary_plan)
}
pub fn successful_relation_witness(before_key:Option<usize>,after_key:Option<usize>)
    requires before_key!=after_key,
    ensures exists|before:DataState,after:DataState,plan:Plan|
        planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==before_key && plan.changes[0].after==after_key,
{
    let (storage,plan,ordinary_plan)=admitted_input_witness(before_key,after_key);
    let ghost before=storage.view();
    let ghost after=DataState{
        image:publication::publication_prefix(before.image,plan.record.writes[0],plan.record.txid,3),
        postings:postings::State{postings:before.postings.postings.union(destinations(plan)).difference(sources(plan)),..before.postings},
    };
    proof {
        complete_postings_match_published_row(before,after,plan);
        assert(committed(before,after,plan));
        assert(exists|s0:DataState,s1:DataState,p:Plan| planned(s0,p) && committed(s0,s1,p)
            && p.changes[0].before==before_key && p.changes[0].after==after_key);
    }
}
pub fn create_move_delete_have_witnesses()
    ensures (exists|before:DataState,after:DataState,plan:Plan| planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==None && plan.changes[0].after==Some(42usize)),
        (exists|before:DataState,after:DataState,plan:Plan| planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==Some(42usize) && plan.changes[0].after==Some(77usize)),
        (exists|before:DataState,after:DataState,plan:Plan| planned(before,plan) && committed(before,after,plan)
        && plan.changes[0].before==Some(42usize) && plan.changes[0].after==None),
{
    successful_relation_witness(None,Some(42));
    successful_relation_witness(Some(42),Some(77));
    successful_relation_witness(Some(42),None);
}

}

pub mod scenario {
// Generated shared lifecycle/clock scenario; see generate.py.
// Checked composition of the exact generated native operations. This harness
// is not linked into AeroStore. Raw index/guard/atomic correspondence is explicit.
pub mod lifecycle { // Generated from actual ProcArray lifecycle operations; see generate.py.
// Native ProcArray algorithms, conditional on mutex exclusion, acquisition
// framing, slot atomic projection, shared-clock linearization, allocation, and
// scoped guard release. Acquisition framing is a separate unproved assumption:
// a real blocking mutex does not preserve metadata from native API entry.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
pub const PROCARRAY_SLOTS: usize = 256;
pub const EMPTY_SLOT: u64 = 0;
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProcArrayRegistration { pub slot_idx: u16, pub txid: u64 }
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProcArrayError {
    NoFreeSlot { txid: u64 }, InvalidSlot { slot_idx: u16 },
    SlotOwnershipMismatch { slot_idx: u16, expected_txid: u64, observed_txid: u64 },
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Slot { pub txid: u64, pub snapshot_xmin: u64 }
pub struct ProcSnapshot { pub xmin: u64, pub xmax: u64, pub in_flight: Vec<u64>, pub in_flight_len: u16 }
// Operation-local input metadata is the view at lifecycle-lock acquisition,
// not an arbitrary real API-entry view. The current driver gives acquisition
// an exact frame. Composing released calls therefore excludes unmodeled slot
// transitions before/during acquisition; shared-clock advances can still be
// represented at the explicit observation/reservation events below. A native
// interference/ownership refinement relating API entry to acquired state is
// not established by this model or by mutex exclusion alone.
pub struct State {
    pub slots: Seq<Slot>,
    // Latest observation of the shared atomic clock; external allocations may
    // advance it at the next modeled observation/reservation. It is not frozen
    // physically while the ProcArray mutex is held.
    pub clock: u64,
    // The last atomic load may lag the current counter, including a Relaxed
    // snapshot load. Native xmax adjustment must cover all active txids.
    pub sampled_clock: u64,
    // Logical history of reservations made through this shared-clock interface.
    // Interfering allocations may skip values; no synthetic consecutive IDs.
    pub reservations: Seq<u64>,
    pub lifecycle_held: bool,
}
pub open spec fn ordered(ids: Seq<u64>) -> bool {
    forall|i: int, j: int| 0 <= i < j < ids.len() ==> ids[i] < ids[j]
}
pub open spec fn well_formed(s: State) -> bool {
    s.slots.len() == PROCARRAY_SLOTS && 0 < s.sampled_clock <= s.clock
    && ordered(s.reservations)
    && (forall|i: int| 0 <= i < s.reservations.len() ==> 0 < #[trigger] s.reservations[i] < s.clock)
    && (forall|i: int| 0 <= i < s.slots.len() ==>
        if #[trigger] s.slots[i].txid == 0 { s.slots[i].snapshot_xmin == 0 }
        else { 0 < s.slots[i].snapshot_xmin <= s.slots[i].txid < s.clock
            && s.reservations.contains(s.slots[i].txid) })
    && (forall|i: int, j: int| 0 <= i < j < s.slots.len() && s.slots[i].txid != 0 ==>
        s.slots[i].txid != s.slots[j].txid)
}
pub open spec fn active(slots: Seq<Slot>, n: int) -> Seq<u64>
    decreases n,
{
    if n <= 0 { Seq::empty() }
    else if slots[n - 1].txid == 0 { active(slots, n - 1) }
    else { active(slots, n - 1).push(slots[n - 1].txid) }
}
pub open spec fn minimum_active(slots: Seq<Slot>, n: int, ceiling: u64) -> u64
    decreases n,
{
    if n <= 0 { ceiling }
    else if slots[n - 1].txid == 0 { minimum_active(slots, n - 1, ceiling) }
    else { if slots[n - 1].txid < minimum_active(slots, n - 1, ceiling) {
        slots[n - 1].txid
    } else { minimum_active(slots, n - 1, ceiling) } }
}
pub open spec fn maximum_active(slots: Seq<Slot>, n: int) -> u64
    decreases n,
{
    if n <= 0 { 0 }
    else { if slots[n - 1].txid > maximum_active(slots, n - 1) {
        slots[n - 1].txid
    } else { maximum_active(slots, n - 1) } }
}
pub open spec fn minimum_retention(slots: Seq<Slot>, n: int, ceiling: u64) -> u64
    decreases n,
{
    if n <= 0 { ceiling }
    else if slots[n - 1].txid == 0 { minimum_retention(slots, n - 1, ceiling) }
    else { if slots[n - 1].snapshot_xmin < minimum_retention(slots, n - 1, ceiling) {
        slots[n - 1].snapshot_xmin
    } else { minimum_retention(slots, n - 1, ceiling) } }
}
pub open spec fn coherent_snapshot(s: State, snapshot: ProcSnapshot) -> bool {
    snapshot.in_flight@ == active(s.slots, s.slots.len() as int)
    && snapshot.in_flight_len == snapshot.in_flight.len()
    && snapshot.xmax == if s.sampled_clock > maximum_active(s.slots, s.slots.len() as int) + 1 {
        s.sampled_clock as int
    } else { maximum_active(s.slots, s.slots.len() as int) + 1 }
    && snapshot.xmin == minimum_active(s.slots, s.slots.len() as int, s.sampled_clock)
}
pub open spec fn observation(before: State, after: State) -> bool {
    after.slots == before.slots && after.reservations == before.reservations
    && after.lifecycle_held == before.lifecycle_held && after.clock >= before.clock
    && 0 < after.sampled_clock <= after.clock
}
pub open spec fn reservation(before: State, after: State, id: u64) -> bool {
    id >= before.clock && id < u64::MAX && after.clock == id + 1
    && after.reservations == before.reservations.push(id)
    && after.slots == before.slots && after.lifecycle_held == before.lifecycle_held
    && after.sampled_clock == before.sampled_clock
}

pub trait LifecyclePrimitives {
    spec fn state(&self) -> State;
    // EXTRA ASSUMPTION: this preserves the operation-local acquired-input view.
    // Native ShmMutex::lock may block while other threads change slots/history;
    // that interference is not represented here. The algorithm's old(state)
    // postconditions must not be read as frames from arbitrary native API entry.
    fn lock_lifecycle(&mut self)
        requires !old(self).state().lifecycle_held,
        ensures final(self).state() == (State { lifecycle_held: true, ..old(self).state() });
    fn release_lifecycle(&mut self)
        requires old(self).state().lifecycle_held,
        ensures final(self).state() == (State { lifecycle_held: false, ..old(self).state() });
    // The lock precondition is an obligation of the registration protocol,
    // not a property assumed of all AtomicU64::fetch_add calls.
    fn reserve_registration(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held, 0 < old(self).state().clock < u64::MAX,
        ensures reservation(old(self).state(), final(self).state(), id);
    fn reserve_publication(&mut self) -> (id: u64)
        requires 0 < old(self).state().clock < u64::MAX,
        ensures reservation(old(self).state(), final(self).state(), id);
    fn load_clock(&mut self) -> (id: u64)
        requires old(self).state().lifecycle_held,
        ensures observation(old(self).state(), final(self).state()), id == final(self).state().sampled_clock;
    fn slot_txid(&self, slot: usize) -> (id: u64)
        requires self.state().lifecycle_held, slot < self.state().slots.len(),
        ensures id == self.state().slots[slot as int].txid;
    fn slot_xmin(&self, slot: usize) -> (id: u64)
        requires self.state().lifecycle_held, slot < self.state().slots.len(),
        ensures id == self.state().slots[slot as int].snapshot_xmin;
    fn compare_empty_register(&mut self, slot: usize, txid: u64) -> (ok: bool)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures ok == (old(self).state().slots[slot as int].txid == EMPTY_SLOT),
            final(self).state() == (State { slots: if ok {
                old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] })
            } else { old(self).state().slots }, ..old(self).state() });
    fn store_xmin(&mut self, slot: usize, xmin: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { snapshot_xmin: xmin, ..old(self).state().slots[slot as int] }), ..old(self).state() });
    fn store_txid(&mut self, slot: usize, txid: u64)
        requires old(self).state().lifecycle_held, slot < old(self).state().slots.len(),
        ensures final(self).state() == (State { slots:
            old(self).state().slots.update(slot as int, Slot { txid, ..old(self).state().slots[slot as int] }), ..old(self).state() });
}

pub fn checked_slot(slot: u16) -> (result: Result<usize, ProcArrayError>)
    ensures result.is_ok() ==> result.unwrap() == slot as usize && slot < PROCARRAY_SLOTS,
        result.is_err() ==> slot >= PROCARRAY_SLOTS,
{
    if (slot as usize) < PROCARRAY_SLOTS { Ok(slot as usize) }
    else { Err(ProcArrayError::InvalidSlot { slot_idx: slot }) }
}

pub proof fn reservation_preserves_clock_history(before: State, after: State, id: u64)
    requires well_formed(before), reservation(before, after, id),
    ensures well_formed(after), after.reservations.contains(id),
        forall|i: int| 0 <= i < before.reservations.len() ==> before.reservations[i] < id,
{}
pub proof fn observation_preserves_state(before: State, after: State)
    requires well_formed(before), observation(before, after),
    ensures well_formed(after),
{}

pub proof fn snapshot_scan_bounds(s: State, n: int)
    requires well_formed(s), 0 <= n <= s.slots.len(),
    ensures active(s.slots, n).len() <= n,
        0 < minimum_active(s.slots, n, s.sampled_clock) <= s.sampled_clock,
        maximum_active(s.slots, n) < s.clock,
        active(s.slots, n).len() == 0 ==> minimum_active(s.slots, n, s.sampled_clock) == s.sampled_clock,
        active(s.slots, n).len() == 0 ==> maximum_active(s.slots, n) == 0,
    decreases n,
{
    if n > 0 { snapshot_scan_bounds(s, n - 1); }
}

pub proof fn snapshot_minimum_covers_slot(s: State, i: int, n: int)
    requires well_formed(s), 0 <= i < n <= s.slots.len(), s.slots[i].txid != 0,
    ensures minimum_active(s.slots, n, s.sampled_clock) <= s.slots[i].txid,
    decreases n,
{
    if i < n - 1 { snapshot_minimum_covers_slot(s, i, n - 1); }
}

pub proof fn snapshot_fields_unchanged(before: Seq<Slot>, after: Seq<Slot>, n: int, ceiling: u64)
    requires before.len() == after.len(), 0 <= n <= before.len(),
        forall|i: int| 0 <= i < before.len() ==> before[i].txid == after[i].txid,
    ensures active(before, n) == active(after, n),
        minimum_active(before, n, ceiling) == minimum_active(after, n, ceiling),
        maximum_active(before, n) == maximum_active(after, n),
    decreases n,
{
    if n > 0 { snapshot_fields_unchanged(before, after, n - 1, ceiling); }
}

pub proof fn reservation_after_reader(before: State, after: State, stamp: u64, reader: u64)
    requires well_formed(before), reservation(before, after, stamp), before.reservations.contains(reader),
    ensures stamp > reader,
{
    let i = choose|i: int| 0 <= i < before.reservations.len() && before.reservations[i] == reader;
    assert(reader < before.clock);
}

pub proof fn active_slot_membership(s: State, i: int, n: int)
    requires well_formed(s), 0 <= i < n <= s.slots.len(), s.slots[i].txid != 0,
    ensures active(s.slots, n).contains(s.slots[i].txid), maximum_active(s.slots, n) >= s.slots[i].txid,
    decreases n,
{
    if i < n - 1 { active_slot_membership(s, i, n - 1); }
}

pub proof fn snapshot_covers_active_writer(s: State, snapshot: ProcSnapshot, writer_slot: int)
    requires well_formed(s), coherent_snapshot(s, snapshot),
        0 <= writer_slot < s.slots.len(), s.slots[writer_slot].txid != 0,
    ensures snapshot.in_flight@.contains(s.slots[writer_slot].txid),
        0 < snapshot.xmin <= s.slots[writer_slot].txid < snapshot.xmax <= s.clock,
{
    snapshot_scan_bounds(s, s.slots.len() as int);
    snapshot_minimum_covers_slot(s, writer_slot, s.slots.len() as int);
    active_slot_membership(s, writer_slot, s.slots.len() as int);
}

pub proof fn retention_covers_active_snapshot(s: State, slot: int, n: int)
    requires well_formed(s), 0 <= slot < n <= s.slots.len(), s.slots[slot].txid != 0,
    ensures minimum_retention(s.slots, n, s.sampled_clock) <= s.slots[slot].snapshot_xmin,
    decreases n,
{
    if slot < n - 1 { retention_covers_active_snapshot(s, slot, n - 1); }
}

pub proof fn reservation_history_strict_order(s: State, earlier: int, later: int)
    requires well_formed(s), 0 <= earlier < later < s.reservations.len(),
    ensures s.reservations[earlier] < s.reservations[later],
{}

// Non-vacuity witnesses include an older retained writer and a stale sampled
// clock below both active IDs. The native xmax compensation is necessary for
// this permitted observation; a latest-value load is not assumed.
pub proof fn lifecycle_contract_has_live_and_stale_witnesses() {
    let empty = Seq::new(PROCARRAY_SLOTS as nat, |i: int| Slot { txid: 0, snapshot_xmin: 0 });
    let initial = State { slots: empty, clock: 1, sampled_clock: 1,
        reservations: Seq::empty(), lifecycle_held: false };
    assert(well_formed(initial));
    let live = State { slots: empty.update(0, Slot { txid: 5, snapshot_xmin: 2 })
        .update(1, Slot { txid: 10, snapshot_xmin: 5 }), clock: 11, sampled_clock: 1,
        reservations: seq![2u64, 5u64, 10u64], lifecycle_held: true };
    assert(well_formed(live));
    assert(live.sampled_clock < live.slots[0].txid < live.slots[1].txid);
    let later = State { clock: 12, reservations: live.reservations.push(11), ..live };
    assert(reservation(live, later, 11));
    reservation_after_reader(live, later, 11, 10);
    assert(11u64 > 10u64);
}

pub fn begin_transaction<D: LifecyclePrimitives>(driver: &mut D) -> (result: Result<ProcArrayRegistration, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        old(driver).state().clock < u64::MAX,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().clock > old(driver).state().clock,
        final(driver).state().reservations.len() == old(driver).state().reservations.len() + 1,
        result.is_ok() ==> {
            let r = result.unwrap();
            r.slot_idx < PROCARRAY_SLOTS && r.txid >= old(driver).state().clock
            && final(driver).state().reservations == old(driver).state().reservations.push(r.txid)
            && old(driver).state().slots[r.slot_idx as int].txid == 0
            && final(driver).state().slots == old(driver).state().slots.update(r.slot_idx as int,
                Slot { txid: r.txid, snapshot_xmin: r.txid })
        },
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots
            && (forall|i: int| 0 <= i < old(driver).state().slots.len() ==> old(driver).state().slots[i].txid != 0),
        match result {
            Err(ProcArrayError::NoFreeSlot { txid }) => txid >= old(driver).state().clock
                && final(driver).state().reservations == old(driver).state().reservations.push(txid),
            Err(_) => false,
            Ok(_) => true,
        },
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let ghost before_reservation = driver.state(); let txid = driver . reserve_registration ( ) ;
proof { reservation_preserves_clock_history(before_reservation, driver.state(), txid); } let ghost reserved = driver.state(); 
        let mut slot_idx: usize = 0;
        while slot_idx < PROCARRAY_SLOTS
            invariant slot_idx <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(initial), well_formed(reserved), driver.state() == reserved,
                reserved.slots == initial.slots, reserved.lifecycle_held,
                reserved.clock > initial.clock, txid >= initial.clock, txid < reserved.clock,
                reserved.reservations == initial.reservations.push(txid),
                forall|i: int| 0 <= i < slot_idx ==> initial.slots[i].txid != 0,
            decreases PROCARRAY_SLOTS - slot_idx,
        {
     if driver . compare_empty_register ( slot_idx , txid ) {
    driver . store_xmin ( slot_idx , txid ) ;
    return Ok ( ProcArrayRegistration {
        slot_idx : slot_idx as u16 , txid ,
    }
    ) ;
}

            slot_idx += 1;
        }
     Err ( ProcArrayError :: NoFreeSlot {
    txid
}
)
}

pub fn end_transaction<D: LifecyclePrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<(), ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        registration.txid > 0,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().clock == old(driver).state().clock,
        final(driver).state().sampled_clock == old(driver).state().sampled_clock,
        final(driver).state().reservations == old(driver).state().reservations,
        result.is_ok() ==> registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid
            && final(driver).state().slots == old(driver).state().slots.update(registration.slot_idx as int,
                Slot { txid: 0, snapshot_xmin: 0 }),
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots,
        result == if registration.slot_idx >= PROCARRAY_SLOTS {
            Err(ProcArrayError::InvalidSlot { slot_idx: registration.slot_idx })
        } else if old(driver).state().slots[registration.slot_idx as int].txid != registration.txid {
            Err(ProcArrayError::SlotOwnershipMismatch { slot_idx: registration.slot_idx,
                expected_txid: registration.txid,
                observed_txid: old(driver).state().slots[registration.slot_idx as int].txid })
        } else { Ok(()) },
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let slot_idx = registration . slot_idx as usize ;
if slot_idx >= PROCARRAY_SLOTS {
    return Err ( ProcArrayError :: InvalidSlot {
        slot_idx : registration . slot_idx ,
    }
    ) ;
}
let slot = slot_idx ;
let observed = driver . slot_txid ( slot ) ;
if observed != registration . txid {
    return Err ( ProcArrayError :: SlotOwnershipMismatch {
        slot_idx : registration . slot_idx , expected_txid : registration . txid , observed_txid : observed ,
    }
    ) ;
}
driver . store_xmin ( slot , EMPTY_SLOT ) ;
driver . store_txid ( slot , EMPTY_SLOT ) ;
Ok ( ( ) )
}

pub fn snapshot_locked<D: LifecyclePrimitives>(driver: &mut D) -> (result: ProcSnapshot)
    requires well_formed(old(driver).state()), old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), observation(old(driver).state(), final(driver).state()),
        coherent_snapshot(final(driver).state(), result),
{
    let ghost initial = driver.state(); let mut xmax = driver . load_clock ( ) ;
let mut xmin = xmax ;
let mut max_in_flight = 0_u64 ;
let mut in_flight = Vec :: new ( ) ;
let mut in_flight_len = 0_u16 ;

        let ghost loaded = driver.state();
        proof { observation_preserves_state(initial, loaded); }
        let mut si: usize = 0;
        while si < PROCARRAY_SLOTS
            invariant si <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(loaded), driver.state() == loaded, observation(initial, loaded),
                loaded.lifecycle_held, xmax == loaded.sampled_clock,
                in_flight@ == active(loaded.slots, si as int),
                in_flight_len == in_flight.len(), in_flight_len <= si,
                xmin == minimum_active(loaded.slots, si as int, loaded.sampled_clock),
                max_in_flight == maximum_active(loaded.slots, si as int),
            decreases PROCARRAY_SLOTS - si,
        {
            let slot = si;
            si += 1;
            proof {
                reveal_with_fuel(active, 2); reveal_with_fuel(minimum_active, 2); reveal_with_fuel(maximum_active, 2);
            }
     let txid = driver . slot_txid ( slot ) ;
if txid == EMPTY_SLOT {
    continue ;
}
proof { assert((in_flight_len as usize) < PROCARRAY_SLOTS); } in_flight . push ( txid ) ;
in_flight_len += 1 ;
xmin = xmin . min ( txid ) ;
max_in_flight = max_in_flight . max ( txid ) ;

        }
        proof { snapshot_scan_bounds(loaded, PROCARRAY_SLOTS as int); }
     if in_flight_len == 0 {
    xmin = xmax ;
}
else {
    xmax = xmax . max ( max_in_flight . saturating_add ( 1 ) ) ;
}
ProcSnapshot {
    xmin , xmax , in_flight , in_flight_len ,
}
}

pub fn create_snapshot<D: LifecyclePrimitives>(driver: &mut D) -> (result: ProcSnapshot)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().slots == old(driver).state().slots,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        coherent_snapshot(final(driver).state(), result),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
snapshot_locked ( driver )
}

pub fn create_transaction_snapshot<D: LifecyclePrimitives>(driver: &mut D, registration: ProcArrayRegistration)
    -> (result: Result<ProcSnapshot, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held, registration.txid > 0,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        result.is_ok() ==> {
            let snap = result->Ok_0;
            registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid
            && coherent_snapshot(final(driver).state(), snap)
            && final(driver).state().slots == old(driver).state().slots.update(registration.slot_idx as int,
                Slot { snapshot_xmin: snap.xmin, ..old(driver).state().slots[registration.slot_idx as int] })
        },
        result.is_err() ==> final(driver).state().slots == old(driver).state().slots,
        (registration.slot_idx < PROCARRAY_SLOTS
            && old(driver).state().slots[registration.slot_idx as int].txid == registration.txid) == result.is_ok(),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let slot = checked_slot ( registration . slot_idx ) ? ;
let observed = driver . slot_txid ( slot ) ;
if observed != registration . txid {
    return Err ( ProcArrayError :: SlotOwnershipMismatch {
        slot_idx : registration . slot_idx , expected_txid : registration . txid , observed_txid : observed ,
    }
    ) ;
}
let snapshot = snapshot_locked ( driver ) ;
let ghost snapshot_state = driver.state(); proof { snapshot_scan_bounds(snapshot_state, PROCARRAY_SLOTS as int); snapshot_minimum_covers_slot(snapshot_state, slot as int, PROCARRAY_SLOTS as int); } driver . store_xmin ( slot , snapshot . xmin ) ;
proof { snapshot_fields_unchanged(snapshot_state.slots, driver.state().slots, PROCARRAY_SLOTS as int, snapshot_state.sampled_clock); } Ok ( snapshot )
}

pub fn oldest_snapshot_xmin<D: LifecyclePrimitives>(driver: &mut D) -> (result: u64)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
    ensures well_formed(final(driver).state()), final(driver).state().lifecycle_held,
        final(driver).state().slots == old(driver).state().slots,
        final(driver).state().reservations == old(driver).state().reservations,
        final(driver).state().clock >= old(driver).state().clock,
        result == minimum_retention(final(driver).state().slots, PROCARRAY_SLOTS as int, final(driver).state().sampled_clock),
{
    let ghost initial = driver.state(); driver . lock_lifecycle ( ) ;
let mut xmin = driver . load_clock ( ) ;

        let ghost loaded = driver.state();
        proof { observation_preserves_state(State { lifecycle_held: true, ..initial }, loaded); }
        let mut si: usize = 0;
        while si < PROCARRAY_SLOTS
            invariant si <= PROCARRAY_SLOTS, initial == old(driver).state(),
                well_formed(loaded), driver.state() == loaded, loaded.lifecycle_held,
                loaded.slots == initial.slots, loaded.reservations == initial.reservations,
                loaded.clock >= initial.clock,
                xmin == minimum_retention(loaded.slots, si as int, loaded.sampled_clock),
            decreases PROCARRAY_SLOTS - si,
        {
            let slot = si;
            si += 1;
            proof { reveal_with_fuel(minimum_retention, 2); }
     if driver . slot_txid ( slot ) != EMPTY_SLOT {
    xmin = xmin . min ( driver . slot_xmin ( slot ) ) ;
}

        }
     xmin
}

// This expression is extracted from publish_index_stamps, where the existing
// commit driver invokes it after finish_transaction. Its atomic operation is
// the same shared allocator used by native begin_transaction.
pub fn reserve_publication_clock<D: LifecyclePrimitives>(driver: &mut D) -> (stamp: u64)
    requires well_formed(old(driver).state()), old(driver).state().clock < u64::MAX,
    ensures well_formed(final(driver).state()), reservation(old(driver).state(), final(driver).state(), stamp),
{
    let ghost before = driver.state();
    let stamp = driver . reserve_publication ( );
    proof { reservation_preserves_clock_history(before, driver.state(), stamp); }
    stamp
}

// Proof harness: the actual end operation, its explicit scoped-guard boundary,
// and the actual publication-reservation expression. No extra reservation is
// inserted into AeroStore; composition must identify this event with the same
// fetch_add used by publish_index_stamps, never perform a second reservation.
pub fn deregister_then_reserve<D: LifecyclePrimitives>(driver: &mut D,
    registration: ProcArrayRegistration, reader: u64) -> (result: Result<u64, ProcArrayError>)
    requires well_formed(old(driver).state()), !old(driver).state().lifecycle_held,
        registration.txid > 0, old(driver).state().clock < u64::MAX,
        old(driver).state().reservations.contains(reader),
    ensures well_formed(final(driver).state()), !final(driver).state().lifecycle_held,
        result.is_ok() ==> result.unwrap() > reader,
        result.is_ok() ==> final(driver).state().reservations == old(driver).state().reservations.push(result.unwrap()),
        result.is_ok() ==> (forall|i: int| 0 <= i < final(driver).state().slots.len() ==>
            final(driver).state().slots[i].txid != registration.txid),
        result.is_err() ==> final(driver).state().reservations == old(driver).state().reservations,
{
    let result = end_transaction(driver, registration);
    driver.release_lifecycle();
    match result {
        Err(error) => Err(error),
        Ok(()) => {
            let ghost before = driver.state();
            let stamp = reserve_publication_clock(driver);
            proof { reservation_after_reader(before, driver.state(), stamp, reader); }
            Ok(stamp)
        }
    }
}
}
 }
pub mod predicate { // Generated from native indexed transaction operations; see generate.py.
// The traits below are explicitly unproved primitive boundaries. The three
// native algorithms are inserted from production source by generate.py.
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub type Pair = (usize, usize);
#[derive(PartialEq, Eq, Debug)]
pub enum Error { IndexBindingsIncomplete, Other }
#[derive(Clone, Copy)]
pub struct IndexRead { pub index_offset: usize, pub bucket: usize, pub stamp: u64 }
#[derive(Clone, Copy)]
pub struct IndexChange { pub binding: usize, pub before: Option<usize>, pub after: Option<usize> }
pub struct OccTransaction { pub txid: u64, pub index_conflict: bool, pub index_reads: Vec<IndexRead> }
pub struct State {
    // Physical arena identity, not a process-local virtual base address.
    pub arena: usize,
    pub bindings: Map<usize, usize>,
    pub binding_count: usize,
    pub key_buckets: Map<Pair, usize>,
    // The operation's guarded stamp projection, not a snapshot claiming that
    // every bucket in the concurrently changing database is frozen.
    pub stamps: Map<Pair, u64>,
    pub held: Set<Pair>,
    // Last represented clock observation. Interfering reservations may advance
    // the actual allocator before this operation's fetch_add linearizes.
    pub clock: u64,
    pub reserved_stamp: u64,
    pub reservations: Seq<u64>,
    pub deregistered: bool,
}
// An arbitrary fixed key-to-bucket function; no hash injectivity is assumed.
pub open spec fn key_bucket(s: State, binding: usize, key: usize) -> usize { s.key_buckets[(binding, key)] }
pub open spec fn pair_less(a: Pair, b: Pair) -> bool {
    a.0 < b.0 || (a.0 == b.0 && a.1 < b.1)
}
pub open spec fn canonical(v: Seq<Pair>, members: Set<Pair>) -> bool {
    v.to_set() == members
    && forall|i: int, j: int| 0 <= i < j < v.len() ==> pair_less(v[i], v[j])
}
pub open spec fn read_pair(s: State, r: IndexRead) -> Pair { (s.bindings[r.index_offset], r.bucket) }
pub open spec fn read_keys(s: State, reads: Seq<IndexRead>, n: int) -> Set<Pair>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { read_keys(s, reads, n - 1).insert(read_pair(s, reads[n - 1])) }
}
pub open spec fn option_keys(s: State, binding: usize, key: Option<usize>) -> Set<Pair> {
    match key { Some(k) => Set::empty().insert((binding, key_bucket(s, binding, k))), None => Set::empty() }
}
pub open spec fn change_keys_one(s: State, c: IndexChange) -> Set<Pair> {
    option_keys(s, c.binding, c.before).union(option_keys(s, c.binding, c.after))
}
pub open spec fn change_keys(s: State, changes: Seq<IndexChange>, n: int) -> Set<Pair>
    decreases n,
{
    if n <= 0 { Set::empty() }
    else { change_keys(s, changes, n - 1).union(change_keys_one(s, changes[n - 1])) }
}
pub open spec fn all_reads_bound(s: State, reads: Seq<IndexRead>, n: int) -> bool {
    forall|i: int| 0 <= i < n ==> s.bindings.contains_key(reads[i].index_offset)
        && s.bindings[reads[i].index_offset] < s.binding_count
}
pub open spec fn changes_valid(s: State, changes: Seq<IndexChange>) -> bool {
    forall|i: int| 0 <= i < changes.len() ==> changes[i].binding < s.binding_count
}
pub open spec fn read_good(s: State, r: IndexRead, txid: u64) -> bool {
    s.bindings.contains_key(r.index_offset)
    && s.stamps.contains_key(read_pair(s, r))
    && s.stamps[read_pair(s, r)] == r.stamp
    && s.stamps[read_pair(s, r)] < txid
}
pub open spec fn reads_good(s: State, reads: Seq<IndexRead>, txid: u64, n: int) -> bool {
    forall|i: int| 0 <= i < n ==> read_good(s, reads[i], txid)
}
pub open spec fn stamp_relation(before: Map<Pair, u64>, after: Map<Pair, u64>, done: Set<Pair>, stamp: u64) -> bool {
    after.dom() == before.dom().union(done)
    && forall|p: Pair| after.contains_key(p) ==> after[p] == if done.contains(p) { stamp } else { before[p] }
}

// BTreeSet's ordinary extensional and canonical-iteration contracts. Its
// implementation and Rust allocation/panic behavior are not proved here.
pub trait PairSet {
    spec fn contents(&self) -> Set<Pair>;
    fn new() -> (out: Self) where Self: Sized
        ensures out.contents() == Set::<Pair>::empty();
    fn insert(&mut self, pair: Pair)
        ensures final(self).contents() == old(self).contents().insert(pair);
    fn into_vec(self) -> (out: Vec<Pair>) where Self: Sized
        ensures canonical(out@, self.contents());
}

// Immutable registry lookup, key encoding/hash, guarded Acquire loads, and
// Release stores remain assumptions. A mutable proof receiver models one
// operation's state; it does not assert exclusive access to the real database.
pub trait Primitives {
    spec fn state(&self) -> State;
    fn find_binding(&self, offset: usize) -> (r: Result<usize, Error>)
        ensures r.is_ok() ==> self.state().bindings.contains_key(offset)
            && r.unwrap() == self.state().bindings[offset]
            && r.unwrap() < self.state().binding_count;
    fn transactional_key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, Error>)
        requires binding < self.state().binding_count,
        ensures r.is_ok() ==> r.unwrap() == key_bucket(self.state(), binding, *key);
    fn transactional_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, Error>)
        requires self.state().held.contains((binding, bucket)), binding < self.state().binding_count,
        ensures r.is_ok() ==> self.state().stamps.contains_key((binding, bucket))
            && r.unwrap() == self.state().stamps[(binding, bucket)];
    fn reserve_stamp(&mut self) -> (stamp: u64)
        requires old(self).state().deregistered, old(self).state().clock < u64::MAX,
        ensures old(self).state().clock <= stamp < u64::MAX,
            final(self).state() == (State { clock: (stamp + 1) as u64, reserved_stamp: stamp,
                reservations: old(self).state().reservations.push(stamp), ..old(self).state() });
    fn transactional_publish_stamp(&mut self, binding: usize, bucket: usize, stamp: u64) -> (r: Result<(), Error>)
        requires old(self).state().held.contains((binding, bucket)), old(self).state().deregistered,
            binding < old(self).state().binding_count,
        ensures final(self).state() == (State {
            stamps: if r.is_ok() { old(self).state().stamps.insert((binding, bucket), stamp) } else { old(self).state().stamps },
            ..old(self).state()
        });
}

pub proof fn stamp_unchanged(before: Map<Pair, u64>, stamp: u64)
    ensures stamp_relation(before, before, Set::empty(), stamp),
{
    assert(before.dom() =~= before.dom().union(Set::empty()));
}

pub proof fn stamp_update(before: Map<Pair, u64>, after: Map<Pair, u64>, done: Set<Pair>, p: Pair, stamp: u64)
    requires stamp_relation(before, after, done, stamp),
    ensures stamp_relation(before, after.insert(p, stamp), done.insert(p), stamp),
{
    assert(after.insert(p, stamp).dom() =~= before.dom().union(done.insert(p)));
}

pub proof fn read_key_member(s: State, reads: Seq<IndexRead>, i: int, n: int)
    requires 0 <= i < n <= reads.len(),
    ensures read_keys(s, reads, n).contains(read_pair(s, reads[i])),
    decreases n,
{
    if i < n - 1 { read_key_member(s, reads, i, n - 1); }
}

pub proof fn changed_binding_in_range(s: State, changes: Seq<IndexChange>, n: int, pair: Pair)
    requires 0 <= n <= changes.len(), changes_valid(s, changes), change_keys(s, changes, n).contains(pair),
    ensures pair.0 < s.binding_count,
    decreases n,
{
    if n > 0 && change_keys(s, changes, n - 1).contains(pair) {
        changed_binding_in_range(s, changes, n - 1, pair);
    }
}

// Composition uses the stamp actually written by the native publication
// theorem. Establishing stamp >= reader.txid from the shared allocator and
// snapshot/ProcArray execution is a separate temporal refinement obligation.
// In particular, the publishing writer's own (possibly older) txid is absent.
pub proof fn publication_invalidates_dependency(before: State, after: State,
    reads: Seq<IndexRead>, txid: u64, touched: Set<Pair>, stamp: u64, i: int)
    requires 0 <= i < reads.len(), before.bindings == after.bindings,
        touched.contains(read_pair(before, reads[i])), stamp >= txid,
        stamp_relation(before.stamps, after.stamps, touched, stamp),
    ensures !reads_good(after, reads, txid, reads.len() as int),
{
    assert(after.stamps.contains_key(read_pair(after, reads[i])));
    assert(after.stamps[read_pair(after, reads[i])] == stamp);
    assert(!read_good(after, reads[i], txid));
}

// A changed captured stamp also invalidates the read regardless of txid.
pub proof fn changed_stamp_invalidates_dependency(after: State,
    reads: Seq<IndexRead>, txid: u64, i: int, stamp: u64)
    requires 0 <= i < reads.len(),
        after.stamps.contains_key(read_pair(after, reads[i])),
        after.stamps[read_pair(after, reads[i])] == stamp, stamp != reads[i].stamp,
    ensures !reads_good(after, reads, txid, reads.len() as int),
{
    assert(!read_good(after, reads[i], txid));
}

pub fn index_lock_keys<D: Primitives, C: PairSet>(driver: &D, tx: &OccTransaction, changes: &Vec<IndexChange>)
    -> (result: Result<Vec<Pair>, Error>)
    requires changes_valid(driver.state(), changes@),
    ensures result.is_ok() ==> canonical(result->Ok_0@,
        read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)
            .union(change_keys(driver.state(), changes@, changes.len() as int))),
        result.is_ok() ==> all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
{
    let mut keys = C :: new ( ) ;

        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, ri as int),
                all_reads_bound(driver.state(), tx.index_reads@, ri as int),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
     let binding = driver . find_binding ( read . index_offset ) ? ;
keys . insert ( ( binding , read . bucket ) ) ;

            proof { reveal_with_fuel(read_keys, 2); }
            ri = ri + 1;
        }
     
        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), changes_valid(driver.state(), changes@),
                keys.contents() == read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int)
                    .union(change_keys(driver.state(), changes@, ci as int)),
                all_reads_bound(driver.state(), tx.index_reads@, tx.index_reads.len() as int),
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
     let index = change . binding ;
if let Some ( key ) = & change . before {
    keys . insert ( ( change . binding , driver . transactional_key_bucket ( index , key ) ? ) ) ;
}
if let Some ( key ) = & change . after {
    keys . insert ( ( change . binding , driver . transactional_key_bucket ( index , key ) ? ) ) ;
}

            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
     Ok ( keys . into_vec ( ) )
}

pub fn index_read_conflict<D: Primitives>(driver: &D, tx: &OccTransaction)
    -> (result: Result<bool, Error>)
    requires read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
    ensures result.is_ok() ==> result.unwrap() ==
        (tx.index_conflict || !reads_good(driver.state(), tx.index_reads@, tx.txid, tx.index_reads.len() as int)),
{
    if tx . index_conflict {
    return Ok ( true ) ;
}

        let mut ri: usize = 0;
        while ri < tx.index_reads.len()
            invariant ri <= tx.index_reads.len(), !tx.index_conflict,
                reads_good(driver.state(), tx.index_reads@, tx.txid, ri as int),
                read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
            decreases tx.index_reads.len() - ri,
        {
            let read = &tx.index_reads[ri];
            proof { read_key_member(driver.state(), tx.index_reads@, ri as int, tx.index_reads.len() as int); }
     let bound = driver . find_binding ( read . index_offset ) ? ;
let stamp = driver . transactional_stamp ( bound , read . bucket ) ? ;
if stamp != read . stamp || ! stamp_precedes_snapshot ( stamp , tx . txid ) {
    return Ok ( true ) ;
}

            ri = ri + 1;
        }
     Ok ( false )
}

pub fn publish_index_stamps<D: Primitives, C: PairSet>(driver: &mut D, changes: &Vec<IndexChange>)
    -> (result: Result<(), Error>)
    requires old(driver).state().deregistered, old(driver).state().clock < u64::MAX,
        changes_valid(old(driver).state(), changes@),
        change_keys(old(driver).state(), changes@, changes.len() as int).subset_of(old(driver).state().held),
    ensures final(driver).state().bindings == old(driver).state().bindings,
        final(driver).state().arena == old(driver).state().arena,
        final(driver).state().binding_count == old(driver).state().binding_count,
        final(driver).state().key_buckets == old(driver).state().key_buckets,
        final(driver).state().held == old(driver).state().held,
        final(driver).state().deregistered == old(driver).state().deregistered,
        changes.len() > 0 ==> old(driver).state().clock <= final(driver).state().reserved_stamp < u64::MAX,
        changes.len() > 0 ==> final(driver).state().clock == final(driver).state().reserved_stamp + 1,
        changes.len() > 0 ==> final(driver).state().reservations == old(driver).state().reservations.push(final(driver).state().reserved_stamp),
        changes.len() == 0 ==> final(driver).state() == old(driver).state(),
        result.is_ok() ==> stamp_relation(old(driver).state().stamps, final(driver).state().stamps,
            change_keys(old(driver).state(), changes@, changes.len() as int), final(driver).state().reserved_stamp),
        // Failures may have published a prefix; every changed stamp has the
        // reserved value, and unrelated buckets are never modified.
        forall|p: Pair| !change_keys(old(driver).state(), changes@, changes.len() as int).contains(p) ==>
            final(driver).state().stamps.contains_key(p) == old(driver).state().stamps.contains_key(p)
            && (old(driver).state().stamps.contains_key(p) ==> final(driver).state().stamps[p] == old(driver).state().stamps[p]),
        forall|p: Pair| final(driver).state().stamps.contains_key(p)
            && (!old(driver).state().stamps.contains_key(p) || final(driver).state().stamps[p] != old(driver).state().stamps[p]) ==>
                change_keys(old(driver).state(), changes@, changes.len() as int).contains(p)
                && final(driver).state().stamps[p] == final(driver).state().reserved_stamp,
{
    let ghost initial = driver.state(); proof { stamp_unchanged(initial.stamps, initial.reserved_stamp); } if changes . is_empty ( ) {
    return Ok ( ( ) ) ;
}
let stamp = driver . reserve_stamp ( ) ;
let mut touched = C :: new ( ) ;

        let mut ci: usize = 0;
        while ci < changes.len()
            invariant ci <= changes.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state() == (State { clock: (stamp + 1) as u64, reserved_stamp: stamp,
                    reservations: initial.reservations.push(stamp), ..initial }),
                initial.deregistered, initial.clock < u64::MAX, initial.clock <= stamp < u64::MAX,
                touched.contents() == change_keys(initial, changes@, ci as int),
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                changes.len() > 0,
            decreases changes.len() - ci,
        {
            let change = &changes[ci];
     if let Some ( key ) = & change . before {
    touched . insert ( ( change . binding , driver . transactional_key_bucket ( change . binding , key ) ? , ) ) ;
}
if let Some ( key ) = & change . after {
    touched . insert ( ( change . binding , driver . transactional_key_bucket ( change . binding , key ) ? , ) ) ;
}

            proof { reveal_with_fuel(change_keys, 2); }
            ci = ci + 1;
        }
     
        proof { stamp_unchanged(initial.stamps, stamp); }
        let ordered = touched.into_vec();
        let mut pi: usize = 0;
        proof { assert(ordered@.take(0).to_set() =~= Set::<Pair>::empty()); }
        while pi < ordered.len()
            invariant pi <= ordered.len(), initial == old(driver).state(),
                changes_valid(initial, changes@),
                driver.state().binding_count == initial.binding_count,
                driver.state().arena == initial.arena,
                canonical(ordered@, change_keys(initial, changes@, changes.len() as int)),
                driver.state().bindings == initial.bindings, driver.state().held == initial.held,
                driver.state().key_buckets == initial.key_buckets,
                driver.state().deregistered == initial.deregistered,
                driver.state().clock == stamp + 1, driver.state().reserved_stamp == stamp,
                driver.state().reservations == initial.reservations.push(stamp),
                initial.deregistered, initial.clock < u64::MAX, initial.clock <= stamp < u64::MAX,
                changes.len() > 0,
                change_keys(initial, changes@, changes.len() as int).subset_of(initial.held),
                stamp_relation(initial.stamps, driver.state().stamps, ordered@.take(pi as int).to_set(), stamp),
                forall|p: Pair| !change_keys(initial, changes@, changes.len() as int).contains(p) ==>
                    driver.state().stamps.contains_key(p) == initial.stamps.contains_key(p)
                    && (initial.stamps.contains_key(p) ==> driver.state().stamps[p] == initial.stamps[p]),
            decreases ordered.len() - pi,
        {
            let (binding, bucket) = ordered[pi];
            let ghost previous_stamps = driver.state().stamps;
            proof {
                assert(ordered@.contains((binding, bucket)));
                changed_binding_in_range(initial, changes@, changes.len() as int, (binding, bucket));
            }
     driver . transactional_publish_stamp ( binding , bucket , stamp ) ? ;

            proof {
                stamp_update(initial.stamps, previous_stamps, ordered@.take(pi as int).to_set(), (binding, bucket), stamp);
                assert(ordered@.take(pi as int + 1).to_set() =~= ordered@.take(pi as int).to_set().insert((binding, bucket)));
            }
            pi = pi + 1;
        }
     Ok ( ( ) )
}

// A proof harness composing the mathematical dependency lemma with the
// extracted native validator. This harness is never compiled into AeroStore.
pub fn validate_after_late_publication<D: Primitives>(driver: &D, tx: &OccTransaction,
    Ghost(before): Ghost<State>, Ghost(touched): Ghost<Set<Pair>>,
    publication_stamp: u64, read_index: usize) -> (result: Result<bool, Error>)
    requires read_index < tx.index_reads.len(), before.bindings == driver.state().bindings,
        touched.contains(read_pair(before, tx.index_reads@[read_index as int])),
        publication_stamp >= tx.txid,
        stamp_relation(before.stamps, driver.state().stamps, touched, publication_stamp),
        read_keys(driver.state(), tx.index_reads@, tx.index_reads.len() as int).subset_of(driver.state().held),
    ensures result.is_ok() ==> result.unwrap(),
{
    proof {
        publication_invalidates_dependency(before, driver.state(), tx.index_reads@,
            tx.txid, touched, publication_stamp, read_index as int);
    }
    index_read_conflict(driver, tx)
}
}

verus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (result: bool)
ensures result == (stamp < transaction_id),
{
stamp < transaction_id
} }
 }
pub mod capture { // Generated from native index_lookup; see generate.py for the boundary.
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
        result.is_ok() ==> final(tx).index_conflict == old(tx).index_conflict,
        unique(final(tx).index_reads@), extends(old(tx).index_reads@, final(tx).index_reads@),
        permitted_additions(old(tx).index_reads@, final(tx).index_reads@, index.offset(), buckets@, index.stamps()),
        final(tx).index_reads.len() <= old(tx).index_reads.len() + buckets.len(),
        result == Err(Error::SerializationFailure) ==> final(tx).index_conflict,
        result.is_ok() ==> forall|i: int| 0 <= i < buckets.len() ==>
            captured(final(tx).index_reads@, index.offset(), buckets[i], index.stamps()[buckets[i]])
                && index.stamps()[buckets[i]] < final(tx).txid,
{
    let ghost initial_reads = tx.index_reads@;
    let mut bucket_pos = 0;
    while bucket_pos < buckets.len()
        invariant
            bucket_pos <= buckets.len(), unique(tx.index_reads@),
            initial_reads == old(tx).index_reads@, extends(initial_reads, tx.index_reads@),
            permitted_additions(initial_reads, tx.index_reads@, index.offset(), buckets@, index.stamps()),
            tx.index_reads.len() <= initial_reads.len() + bucket_pos,
            tx.txid == old(tx).txid,
            old(tx).index_conflict ==> tx.index_conflict,
            tx.index_conflict == old(tx).index_conflict,
            forall|j: int| 0 <= j < buckets.len() ==>
                index.held().contains(buckets[j]) && index.stamps().contains_key(buckets[j]),
            forall|j: int| 0 <= j < bucket_pos ==>
                captured(tx.index_reads@, index.offset(), buckets[j], index.stamps()[buckets[j]])
                    && index.stamps()[buckets[j]] < tx.txid,
        decreases buckets.len() - bucket_pos,
    {
        let bucket = &buckets[bucket_pos];
let stamp = index . transactional_stamp ( * bucket ) ? ;
if ! stamp_precedes_snapshot ( stamp , tx . txid ) {
    tx . index_conflict = true ;
    return Err ( Error :: SerializationFailure ) ;
}
if let Some ( previous ) = find_read ( & tx . index_reads , index . header_offset ( ) , * bucket ) {
    if previous . stamp != stamp {
        tx . index_conflict = true ;
        return Err ( Error :: SerializationFailure ) ;
    }
}
else {
    proof {
        appended_read_preserves_unique(tx.index_reads@, IndexRead {
            index_offset: index.offset(), bucket: *bucket, stamp });
    }
    tx . index_reads . push ( IndexRead {
        index_offset : index . header_offset ( ) , bucket : * bucket , stamp ,
    }
    ) ;
}
        bucket_pos += 1;
    }
    Ok(())
}
}
verus! { pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> (r: bool)
ensures r == (stamp < transaction_id),
{ stamp < transaction_id } }
 }
use vstd::prelude::*;
use lifecycle::LifecyclePrimitives;
use predicate::Primitives;
use capture::CaptureIndex;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub struct IndexProjection {
    pub arena: usize,
    pub bindings: Map<usize, usize>,
    pub binding_count: usize,
    pub key_buckets: Map<predicate::Pair, usize>,
    pub stamps: Map<predicate::Pair, u64>,
    pub held: Set<predicate::Pair>,
}
// The operation-local, guarded index projection. This does not claim that raw
// native index atomics or held guard lifetimes have been refined here.
pub trait IndexPrimitives {
    spec fn projection(&self) -> IndexProjection;
    fn find_binding(&self, offset: usize) -> (r: Result<usize, predicate::Error>)
        ensures r.is_ok() ==> self.projection().bindings.contains_key(offset)
            && r.unwrap() == self.projection().bindings[offset]
            && r.unwrap() < self.projection().binding_count;
    fn key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, predicate::Error>)
        requires binding < self.projection().binding_count,
        ensures r.is_ok() ==> r.unwrap() == self.projection().key_buckets[(binding, *key)];
    fn load_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, predicate::Error>)
        requires self.projection().held.contains((binding, bucket)), binding < self.projection().binding_count,
        ensures r.is_ok() ==> self.projection().stamps.contains_key((binding, bucket))
            && r.unwrap() == self.projection().stamps[(binding, bucket)];
    fn store_stamp(&mut self, binding: usize, bucket: usize, stamp: u64) -> (r: Result<(), predicate::Error>)
        requires old(self).projection().held.contains((binding, bucket)), binding < old(self).projection().binding_count,
        ensures final(self).projection() == (IndexProjection {
            stamps: if r.is_ok() { old(self).projection().stamps.insert((binding, bucket), stamp) }
                else { old(self).projection().stamps }, ..old(self).projection()
        });
}
// Borrowed view of the same index object used by publication and validation.
pub struct CaptureView<'a, I: IndexPrimitives> { pub index: &'a I, pub offset: usize, pub binding: usize, pub bucket: usize }
impl<'a, I: IndexPrimitives> capture::CaptureIndex for CaptureView<'a, I> {
    open spec fn offset(&self) -> usize { self.offset }
    open spec fn stamps(&self) -> Map<usize, u64> {
        Map::empty().insert(self.bucket, self.index.projection().stamps[(self.binding, self.bucket)])
    }
    open spec fn held(&self) -> Set<usize> {
        if self.binding < self.index.projection().binding_count
            && self.index.projection().held.contains((self.binding, self.bucket))
            && self.index.projection().stamps.contains_key((self.binding, self.bucket)) {
            Set::empty().insert(self.bucket)
        } else { Set::empty() }
    }
    fn header_offset(&self) -> (r: usize) { self.offset }
    fn transactional_stamp(&self, bucket: usize) -> (r: Result<u64, capture::Error>) {
        match self.index.load_stamp(self.binding, bucket) {
            Ok(stamp) => Ok(stamp),
            Err(_) => Err(capture::Error::Index),
        }
    }
}

pub fn capture_one_dependency<I: IndexPrimitives>(index: &I, offset: usize, bucket: usize, txid: u64)
    -> (r: Result<predicate::IndexRead, capture::Error>)
    requires index.projection().bindings.contains_key(offset),
        index.projection().bindings[offset] < index.projection().binding_count,
        index.projection().held.contains((index.projection().bindings[offset], bucket)),
        index.projection().stamps.contains_key((index.projection().bindings[offset], bucket)),
    ensures r.is_ok() ==> r.unwrap().index_offset == offset && r.unwrap().bucket == bucket
        && r.unwrap().stamp == index.projection().stamps[(index.projection().bindings[offset], bucket)]
        && r.unwrap().stamp < txid,
{
    let binding = match index.find_binding(offset) {
        Ok(binding) => binding,
        Err(_) => return Err(capture::Error::Index),
    };
    let view = CaptureView { index, offset, binding, bucket };
    let mut captured = capture::Transaction { txid, index_conflict: false, index_reads: Vec::new() };
    let mut buckets = Vec::new();
    buckets.push(bucket);
    match capture::capture_dependencies(&view, &mut captured, &buckets) {
        Err(error) => return Err(error),
        Ok(()) => {},
    }
    proof {
        assert(capture::captured(captured.index_reads@, view.offset(), buckets[0], view.stamps()[buckets[0]]));
        assert(capture::captured(captured.index_reads@, offset, bucket,
            index.projection().stamps[(binding, bucket)]));
        let member = choose|i: int| 0 <= i < captured.index_reads.len()
            && captured.index_reads[i] == capture::IndexRead { index_offset: offset, bucket,
                stamp: index.projection().stamps[(binding, bucket)] };
        assert(captured.index_reads.len() == 1);
        assert(captured.index_reads[0] == capture::IndexRead { index_offset: offset, bucket,
            stamp: index.projection().stamps[(binding, bucket)] });
    }
    let read = &captured.index_reads[0];
    Ok(predicate::IndexRead { index_offset: read.index_offset, bucket: read.bucket, stamp: read.stamp })
}

pub trait ScenarioClock: LifecyclePrimitives {
    // Harness case selection only: native AeroStore does not implement this
    // overflow check. External reservations may have exhausted the finite clock
    // during snapshot acquisition even if the initial clock had room.
    fn clock_has_capacity(&self) -> (r: bool)
        ensures r == (self.state().clock < u64::MAX);
}

// One lifecycle object is used for reader registration, snapshot creation,
// writer deregistration AND the publisher's exact reservation expression.
// Arena/clock correspondence is the caller's native projection obligation.
pub struct Bridge<L: ScenarioClock, I: IndexPrimitives> {
    pub lifecycle: L,
    pub index: I,
    pub writer_txid: u64,
    pub reserved_stamp: u64,
}

impl<L: ScenarioClock, I: IndexPrimitives> predicate::Primitives for Bridge<L, I> {
    open spec fn state(&self) -> predicate::State {
        predicate::State {
            arena: self.index.projection().arena,
            bindings: self.index.projection().bindings,
            binding_count: self.index.projection().binding_count,
            key_buckets: self.index.projection().key_buckets,
            stamps: self.index.projection().stamps,
            held: self.index.projection().held,
            clock: self.lifecycle.state().clock,
            reserved_stamp: self.reserved_stamp,
            reservations: self.lifecycle.state().reservations,
            deregistered: lifecycle::well_formed(self.lifecycle.state())
                && !self.lifecycle.state().lifecycle_held
                && (forall|i: int| 0 <= i < self.lifecycle.state().slots.len() ==>
                    self.lifecycle.state().slots[i].txid != self.writer_txid),
        }
    }
    fn find_binding(&self, offset: usize) -> (r: Result<usize, predicate::Error>) {
        self.index.find_binding(offset)
    }
    fn transactional_key_bucket(&self, binding: usize, key: &usize) -> (r: Result<usize, predicate::Error>) {
        self.index.key_bucket(binding, key)
    }
    fn transactional_stamp(&self, binding: usize, bucket: usize) -> (r: Result<u64, predicate::Error>) {
        self.index.load_stamp(binding, bucket)
    }
    fn reserve_stamp(&mut self) -> (stamp: u64) {
        let stamp = lifecycle::reserve_publication_clock(&mut self.lifecycle);
        self.reserved_stamp = stamp;
        stamp
    }
    fn transactional_publish_stamp(&mut self, binding: usize, bucket: usize, stamp: u64)
        -> (r: Result<(), predicate::Error>)
    {
        self.index.store_stamp(binding, bucket, stamp)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum ScenarioError { Registration, Snapshot, Capture, Deregistration, ClockExhausted, Publication, Validation }
pub struct Outcome { pub reader_txid: u64, pub publication_stamp: u64, pub conflict: bool,
    pub reader_snapshot: lifecycle::ProcSnapshot }

pub proof fn change_keys_ignore_clock(before: predicate::State, after: predicate::State,
    changes: Seq<predicate::IndexChange>, n: int)
    requires before.key_buckets == after.key_buckets, 0 <= n <= changes.len(),
    ensures predicate::change_keys(before, changes, n) == predicate::change_keys(after, changes, n),
    decreases n,
{
    if n > 0 { change_keys_ignore_clock(before, after, changes, n - 1); }
}

pub proof fn read_keys_ignore_clock(before: predicate::State, after: predicate::State,
    reads: Seq<predicate::IndexRead>, n: int)
    requires before.bindings == after.bindings, 0 <= n <= reads.len(),
    ensures predicate::read_keys(before, reads, n) == predicate::read_keys(after, reads, n),
    decreases n,
{
    if n > 0 { read_keys_ignore_clock(before, after, reads, n - 1); }
}

pub proof fn cleared_writer_is_absent(before: lifecycle::State, after: lifecycle::State,
    registration: lifecycle::ProcArrayRegistration)
    requires lifecycle::well_formed(before), registration.txid > 0,
        registration.slot_idx < before.slots.len(),
        before.slots[registration.slot_idx as int].txid == registration.txid,
        after.slots == before.slots.update(registration.slot_idx as int,
            lifecycle::Slot { txid: 0, snapshot_xmin: 0 }),
    ensures forall|i: int| 0 <= i < after.slots.len() ==> after.slots[i].txid != registration.txid,
{
    assert forall|i: int| 0 <= i < after.slots.len() implies after.slots[i].txid != registration.txid by {
        if i < registration.slot_idx { assert(before.slots[i].txid != registration.txid); }
        if i > registration.slot_idx { assert(before.slots[i].txid != registration.txid); }
    }
}

pub open spec fn scenario_entry(life: lifecycle::State, p: predicate::State,
    writer: lifecycle::ProcArrayRegistration, offset: usize, bucket: usize,
    changes: Seq<predicate::IndexChange>) -> bool {
    lifecycle::well_formed(life)
        && !life.lifecycle_held
        && life.clock < u64::MAX
        && writer.txid > 0
        && writer.slot_idx < lifecycle::PROCARRAY_SLOTS
        && life.slots[writer.slot_idx as int].txid == writer.txid
        && p.bindings.contains_key(offset)
        && p.bindings[offset] < p.binding_count
        && p.stamps.contains_key((p.bindings[offset], bucket))
        && p.held.contains((p.bindings[offset], bucket))
        && predicate::changes_valid(p, changes)
        && predicate::change_keys(p, changes, changes.len() as int)
            .contains((p.bindings[offset], bucket))
        && predicate::change_keys(p, changes, changes.len() as int).subset_of(p.held)
}

// A fixed schedule with an older active writer and a newly registered reader.
// The requested index/bucket and guard permissions are supplied; the reader ID,
// snapshot, dependency, deregistration, history and publication stamp are
// produced by actual generated native functions below. Slot transitions are
// restricted to this schedule, not arbitrary interference while acquiring locks.
pub fn registered_reader_then_writer_publication<L: ScenarioClock, I: IndexPrimitives, C: predicate::PairSet>(
    driver: &mut Bridge<L, I>, writer: lifecycle::ProcArrayRegistration,
    offset: usize, bucket: usize, changes: &Vec<predicate::IndexChange>)
    -> (result: Result<Outcome, ScenarioError>)
    requires scenario_entry(old(driver).lifecycle.state(), old(driver).state(), writer, offset, bucket, changes@),
        writer.txid == old(driver).writer_txid,
    ensures result.is_ok() ==> result.unwrap().conflict
        && result.unwrap().publication_stamp > result.unwrap().reader_txid,
        result.is_ok() ==> result.unwrap().reader_snapshot.in_flight@.contains(writer.txid),
        result.is_ok() ==> final(driver).state().reservations.contains(result.unwrap().reader_txid)
            && final(driver).state().reservations.contains(result.unwrap().publication_stamp),
{
    let ghost initial = driver.lifecycle.state();
    let ghost initial_predicate = driver.state();
    let reader_result = lifecycle::begin_transaction(&mut driver.lifecycle);
    driver.lifecycle.release_lifecycle();
    let reader = match reader_result {
        Ok(reader) => reader,
        Err(_) => return Err(ScenarioError::Registration),
    };
    proof {
        assert(reader.slot_idx != writer.slot_idx);
        assert(driver.lifecycle.state().slots[writer.slot_idx as int].txid == writer.txid);
    }
    let snapshot_result = lifecycle::create_transaction_snapshot(&mut driver.lifecycle, reader);
    driver.lifecycle.release_lifecycle();
    let snapshot = match snapshot_result {
        Ok(snapshot) => snapshot,
        Err(_) => return Err(ScenarioError::Snapshot),
    };
    proof {
        lifecycle::snapshot_covers_active_writer(driver.lifecycle.state(), snapshot, writer.slot_idx as int);
        assert(snapshot.in_flight@.contains(writer.txid));
        assert(driver.lifecycle.state().reservations.contains(reader.txid));
    }
    let read = match capture_one_dependency(&driver.index, offset, bucket, reader.txid) {
        Ok(read) => read,
        Err(_) => return Err(ScenarioError::Capture),
    };
    let mut reads = Vec::new();
    reads.push(read);
    let ghost before_end = driver.lifecycle.state();
    let finish = lifecycle::end_transaction(&mut driver.lifecycle, writer);
    driver.lifecycle.release_lifecycle();
    match finish {
        Err(_) => return Err(ScenarioError::Deregistration),
        Ok(()) => {},
    }
    proof {
        cleared_writer_is_absent(before_end, driver.lifecycle.state(), writer);
        assert(driver.state().deregistered);
    }
    if !driver.lifecycle.clock_has_capacity() {
        return Err(ScenarioError::ClockExhausted);
    }
    let ghost before_publication = driver.state();
    let ghost clock_before_publication = driver.lifecycle.state();
    proof {
        change_keys_ignore_clock(initial_predicate, before_publication, changes@, changes.len() as int);
        read_keys_ignore_clock(initial_predicate, before_publication, reads@, reads.len() as int);
        reveal_with_fuel(predicate::read_keys, 2);
        assert(predicate::read_keys(before_publication, reads@, 1)
            == Set::empty().insert((before_publication.bindings[offset], bucket)));
    }
    let tx = predicate::OccTransaction { txid: reader.txid, index_conflict: false, index_reads: reads };
    let published = predicate::publish_index_stamps::<Bridge<L, I>, C>(driver, changes);
    match published {
        Err(_) => return Err(ScenarioError::Publication),
        Ok(()) => {},
    }
    let publication_stamp = driver.reserved_stamp;
    proof {
        assert(changes.len() > 0);
        read_keys_ignore_clock(before_publication, driver.state(), tx.index_reads@, tx.index_reads.len() as int);
        let member = choose|i: int| 0 <= i < clock_before_publication.reservations.len()
            && clock_before_publication.reservations[i] == reader.txid;
        assert(reader.txid < clock_before_publication.clock);
        assert(publication_stamp > reader.txid);
        predicate::publication_invalidates_dependency(before_publication, driver.state(), tx.index_reads@,
            reader.txid, predicate::change_keys(before_publication, changes@, changes.len() as int),
            publication_stamp, 0);
    }
    let conflict = match predicate::index_read_conflict(driver, &tx) {
        Ok(conflict) => conflict,
        Err(_) => return Err(ScenarioError::Validation),
    };
    Ok(Outcome { reader_txid: reader.txid, publication_stamp, conflict, reader_snapshot: snapshot })
}

// Finite, live entry states for both empty creation and key movement, together
// with a safe old read and a later invalidating publication. This witnesses
// consistency of the scenario premises; it is not a native heap construction.
pub proof fn creation_and_move_have_live_witnesses() {
    let empty = Seq::new(lifecycle::PROCARRAY_SLOTS as nat,
        |i: int| lifecycle::Slot { txid: 0, snapshot_xmin: 0 });
    let slots = empty.update(0, lifecycle::Slot { txid: 2, snapshot_xmin: 2 });
    let life = lifecycle::State { slots, clock: 3, sampled_clock: 3,
        reservations: seq![2u64], lifecycle_held: false };
    let p = predicate::State { arena: 1, bindings: Map::empty().insert(7usize, 0usize),
        binding_count: 1, key_buckets: Map::empty().insert((0usize, 99usize), 1usize)
            .insert((0usize, 100usize), 2usize),
        stamps: Map::empty().insert((0usize, 1usize), 1u64).insert((0usize, 2usize), 1u64),
        held: Set::empty().insert((0usize, 1usize)).insert((0usize, 2usize)),
        clock: 3, reserved_stamp: 0, reservations: seq![2u64], deregistered: false };
    let writer = lifecycle::ProcArrayRegistration { slot_idx: 0, txid: 2 };
    let creation = seq![predicate::IndexChange { binding: 0, before: None, after: Some(100) }];
    let movement = seq![predicate::IndexChange { binding: 0, before: Some(99), after: Some(100) }];
    reveal_with_fuel(predicate::change_keys, 2);
    assert(scenario_entry(life, p, writer, 7, 2, creation));
    assert(scenario_entry(life, p, writer, 7, 2, movement));
    let read = predicate::IndexRead { index_offset: 7, bucket: 2, stamp: 1 };
    assert(predicate::read_good(p, read, 3));
    let allocated = lifecycle::State { clock: 4, reservations: life.reservations.push(3), ..life };
    assert(lifecycle::reservation(life, allocated, 3));
    assert(life.slots[1].txid == 0);
    let registered = lifecycle::State { slots: slots.update(1,
        lifecycle::Slot { txid: 3, snapshot_xmin: 3 }), clock: 4, sampled_clock: 4,
        reservations: seq![2u64, 3u64], lifecycle_held: false };
    assert(registered.slots == allocated.slots.update(1,
        lifecycle::Slot { txid: 3, snapshot_xmin: 3 }));
    let snapshotted = lifecycle::State { slots: registered.slots.update(1,
        lifecycle::Slot { txid: 3, snapshot_xmin: 2 }), ..registered };
    assert(lifecycle::well_formed(snapshotted));
    lifecycle::active_slot_membership(snapshotted, 0, snapshotted.slots.len() as int);
    assert(lifecycle::active(snapshotted.slots, snapshotted.slots.len() as int).contains(2));
    let finished = lifecycle::State { slots: snapshotted.slots.update(0,
        lifecycle::Slot { txid: 0, snapshot_xmin: 0 }), ..snapshotted };
    assert(lifecycle::well_formed(registered));
    assert(lifecycle::well_formed(finished));
    let reserved = lifecycle::State { clock: 5, reservations: finished.reservations.push(4), ..finished };
    assert(lifecycle::reservation(finished, reserved, 4));
    lifecycle::reservation_after_reader(finished, reserved, 4, 3);
    let published = predicate::State { stamps: p.stamps.insert((0usize, 2usize), 4u64),
        clock: 5, reserved_stamp: 4, reservations: reserved.reservations, deregistered: true, ..p };
    assert(!predicate::read_good(published, read, 3));
    assert(4u64 > 3u64);
}
}

}
use scenario::{lifecycle, predicate};
use lifecycle::LifecyclePrimitives;
use predicate::Primitives;
verus! {

// The registration field is extracted from the same native transaction that
// supplies the write plan. Its ownership before this cut remains explicit.
pub struct FinishToken {pub registration:Option<lifecycle::ProcArrayRegistration>}

pub fn finish_transaction<L:LifecyclePrimitives>(driver:&mut L,tx:&mut FinishToken)
    ->(result:Result<(),lifecycle::ProcArrayError>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
        old(tx).registration.is_some() ==> old(tx).registration.unwrap().txid>0,
    ensures lifecycle::well_formed(final(driver).state()),!final(driver).state().lifecycle_held,
        final(tx).registration.is_none(),
        final(driver).state().clock==old(driver).state().clock,
        final(driver).state().reservations==old(driver).state().reservations,
        old(tx).registration.is_none() ==> final(driver).state()==old(driver).state() && result.is_ok(),
        old(tx).registration.is_some() && old(tx).registration.unwrap().slot_idx<lifecycle::PROCARRAY_SLOTS
            && old(driver).state().slots[old(tx).registration.unwrap().slot_idx as int].txid==old(tx).registration.unwrap().txid
            ==> result.is_ok(),
        result.is_ok() && old(tx).registration.is_some() ==>
            old(tx).registration.unwrap().slot_idx<lifecycle::PROCARRAY_SLOTS
            && old(driver).state().slots[old(tx).registration.unwrap().slot_idx as int].txid==old(tx).registration.unwrap().txid
            && final(driver).state().slots==old(driver).state().slots.update(old(tx).registration.unwrap().slot_idx as int,
                lifecycle::Slot{txid:0,snapshot_xmin:0}),
        result.is_err() ==> final(driver).state().slots==old(driver).state().slots,
{
    let Some ( registration ) = tx . registration . take ( ) else {
    return Ok ( ( ) ) ;
}
;
let ended = lifecycle :: end_transaction ( driver , registration ) ;
driver . release_lifecycle ( ) ;
ended ? ;
Ok ( ( ) )
}

pub open spec fn translated_change(c:postings::IndexChange)->predicate::IndexChange {
    predicate::IndexChange{binding:c.binding,before:c.before,after:c.after}
}
pub fn publication_changes(plan:&Plan)->(changes:Vec<predicate::IndexChange>)
    requires plan.changes.len()==1,
    ensures changes.len()==1 && changes[0]==translated_change(plan.changes[0]),
{
    let c=&plan.changes[0];
    let mut changes=Vec::new();
    changes.push(predicate::IndexChange{binding:c.binding,before:c.before,after:c.after});
    changes
}
pub open spec fn affected(s:predicate::State,plan:Plan)->Set<predicate::Pair> {
    predicate::change_keys(s,seq![translated_change(plan.changes[0])],1)
}
pub open spec fn completion_entry(life:lifecycle::State,index:predicate::State,writer:lifecycle::ProcArrayRegistration,
    data:DataState,plan:Plan)->bool {
    lifecycle::well_formed(life) && !life.lifecycle_held && life.clock<u64::MAX
    && writer.txid>0 && writer.txid==plan.record.txid
    && plan.changes.len()==1 && plan.changes[0].binding<index.binding_count
    && data.postings.binding_count==index.binding_count
    && affected(index,plan).subset_of(index.held)
}
#[derive(PartialEq,Eq)]
pub enum CompletionError {Data,Deregistration,Stamp}
pub struct Completion {pub writes:Vec<ordinary::PublishedWrite>,pub stamp:u64}

// This is a cutpoint composition: the incoming plan has already been validated
// under row/index guards. Index stamp and row/posting views denote the same
// registered native index; physical aliasing and guarded framing are explicit
// primitive obligations. Intervening local-set/recycler cleanup is assumed to
// preserve these published projections; that cleanup is not proved here.
pub fn publish_then_complete<P:postings::PostingPrimitives,R:ordinary::OrdinaryStorage,
    L:scenario::ScenarioClock,I:scenario::IndexPrimitives,C:predicate::PairSet>(
    storage:&mut Storage<P,R>,publisher:&mut scenario::Bridge<L,I>,token:&mut FinishToken,
    plan:&Plan,ordinary_plan:&OrdinaryPlan)->(result:Result<Completion,CompletionError>)
    requires planned(old(storage).view(),*plan),ordinary_matches(*plan,*ordinary_plan),
        old(storage).rows.authorized(plan.record.writes[0].row_id,plan.record.writes[0].new_offset),
        old(token).registration.is_some(),
        old(publisher).writer_txid==old(token).registration.unwrap().txid,
        completion_entry(old(publisher).lifecycle.state(),old(publisher).state(),old(token).registration.unwrap(),
            old(storage).view(),*plan),
    ensures result.is_ok() ==> committed(old(storage).view(),final(storage).view(),*plan),
        result.is_ok() ==> final(token).registration.is_none() && final(publisher).state().deregistered,
        result.is_ok() ==> result->Ok_0.writes.len()==1 && result->Ok_0.writes[0]==ordinary::report(old(storage).view().image,
            ordinary_plan.tx.write_set[ordinary_plan.indices[0] as int]),
        result.is_ok() ==> result->Ok_0.stamp>=old(publisher).state().clock && result->Ok_0.stamp< u64::MAX,
        result.is_ok() ==> result->Ok_0.stamp>plan.record.txid,
        result.is_ok() ==> predicate::stamp_relation(old(publisher).state().stamps,final(publisher).state().stamps,
            affected(old(publisher).state(),*plan),result->Ok_0.stamp),
        result.is_err() ==> unchanged_data(old(storage).view(),final(storage).view()) || final(storage).view().postings.poisoned,
        result==Err(CompletionError::Deregistration) || result==Err(CompletionError::Stamp) ==>
            final(storage).view().postings.poisoned && final(token).registration.is_none(),
{
    let ghost initial_data=storage.view();
    let ghost initial_index=publisher.state();
    let changes=publication_changes(plan);
    let writes=match native_ordinary_data_segment(storage,plan,ordinary_plan) {
        Ok(writes)=>writes,
        Err(_)=>return Err(CompletionError::Data),
    };
    let ghost before_end=publisher.lifecycle.state();
    let writer=token.registration.unwrap();
    proof {assert(committed(initial_data,storage.view(),*plan));}
    match finish_transaction(&mut publisher.lifecycle,token) {
        Err(_)=>{poison(storage);return Err(CompletionError::Deregistration);},
        Ok(())=>{},
    }
    proof {
        scenario::cleared_writer_is_absent(before_end,publisher.lifecycle.state(),writer);
        assert(publisher.state().deregistered);
        scenario::change_keys_ignore_clock(initial_index,publisher.state(),changes@,1);
        assert(changes@==seq![translated_change(plan.changes[0])]);
    }
    match Ok::<(),predicate::Error>(()) {
        Err(_)=>{poison(storage);return Err(CompletionError::Stamp);},
        Ok(())=>{},
    }
    Ok(Completion{writes,stamp:publisher.reserved_stamp})
}

pub fn owned_registration_finishes<L:LifecyclePrimitives>(driver:&mut L,token:&mut FinishToken)
    ->(result:Result<(),lifecycle::ProcArrayError>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
        old(token).registration.is_some(),old(token).registration.unwrap().txid>0,
        old(token).registration.unwrap().slot_idx<lifecycle::PROCARRAY_SLOTS,
        old(driver).state().slots[old(token).registration.unwrap().slot_idx as int].txid==old(token).registration.unwrap().txid,
    ensures result.is_ok(),final(token).registration.is_none(),
        forall|i:int| 0<=i<final(driver).state().slots.len() ==>
            final(driver).state().slots[i].txid!=old(token).registration.unwrap().txid,
{
    let ghost before=driver.state();
    let registration=token.registration.unwrap();
    let result=finish_transaction(driver,token);
    proof {scenario::cleared_writer_is_absent(before,driver.state(),registration);}
    result
}

pub proof fn completion_entry_has_live_witness(data:DataState,plan:Plan)
    requires planned(data,plan),plan.record.txid<u64::MAX-1,
    ensures exists|life:lifecycle::State,index:predicate::State,writer:lifecycle::ProcArrayRegistration|
        completion_entry(life,index,writer,data,plan)
        && writer.slot_idx<lifecycle::PROCARRAY_SLOTS && life.slots[writer.slot_idx as int].txid==writer.txid,
{
    let id=plan.record.txid;
    let empty=Seq::new(lifecycle::PROCARRAY_SLOTS as nat,|i:int|lifecycle::Slot{txid:0,snapshot_xmin:0});
    let life=lifecycle::State{slots:empty.update(0,lifecycle::Slot{txid:id,snapshot_xmin:id}),
        clock:(id+1) as u64,sampled_clock:(id+1) as u64,reservations:seq![id],lifecycle_held:false};
    let c=plan.changes[0];
    let buckets=Map::empty();
    let buckets=if c.before.is_some() {buckets.insert((c.binding,c.before.unwrap()),0usize)} else {buckets};
    let buckets=if c.after.is_some() {buckets.insert((c.binding,c.after.unwrap()),0usize)} else {buckets};
    let index=predicate::State{arena:1,bindings:Map::empty().insert(7usize,c.binding),
        binding_count:data.postings.binding_count,key_buckets:buckets,
        stamps:Map::empty().insert((c.binding,0usize),0u64),held:Set::empty().insert((c.binding,0usize)),
        clock:(id+1) as u64,reserved_stamp:0,reservations:seq![id],deregistered:false};
    let writer=lifecycle::ProcArrayRegistration{slot_idx:0,txid:id};
    reveal_with_fuel(predicate::change_keys,2);
    reveal(postings::valid);
    assert(postings::change_valid(data.postings,c));
    assert(lifecycle::well_formed(life));
    assert(completion_entry(life,index,writer,data,plan));
}

// A successfully returned writer invalidates a previously captured dependency
// in every affected bucket. The stamp is the actual reservation returned above,
// not a caller-supplied conflict conclusion or synthetic second reservation.
pub proof fn completed_write_invalidates_capture(before:predicate::State,after:predicate::State,
    plan:Plan,read:predicate::IndexRead,reader:u64,stamp:u64)
    requires before.bindings==after.bindings,before.bindings.contains_key(read.index_offset),
        affected(before,plan).contains((before.bindings[read.index_offset],read.bucket)),
        predicate::stamp_relation(before.stamps,after.stamps,affected(before,plan),stamp),
        reader<=before.clock,before.clock<=stamp,
    ensures !predicate::read_good(after,read,reader),
{}
}
