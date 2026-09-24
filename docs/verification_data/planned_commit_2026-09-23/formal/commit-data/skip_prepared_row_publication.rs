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
    let result=Ok::<(),lookup::Error>(());
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
