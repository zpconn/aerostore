// Generated native storage/publication/retention composition.
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
pub mod retention {
use crate::lookup;
use lookup::{Image, Row, Snapshot, Error};
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub open spec fn reachable(s: Image, p: u32) -> Set<u32>
    decreases if p == 0 || !s.rows.contains_key(p) { 0nat } else { s.rank[p] + 1 },
{
    if !lookup::image_valid(s) || p == 0 || !s.rows.contains_key(p) { Set::empty() }
    else { reachable(s, s.rows[p].next).insert(p) }
}
pub open spec fn prefix(s: Image, p: u32, tx: Snapshot) -> Set<u32>
    decreases if p == 0 || !s.rows.contains_key(p) { 0nat } else { s.rank[p] + 1 },
{
    if !lookup::image_valid(s) || p == 0 || !s.rows.contains_key(p) { Set::empty() }
    else if lookup::visible(s.rows[p], tx) { Set::empty().insert(p) }
    else { prefix(s, s.rows[p].next, tx).insert(p) }
}
pub open spec fn eligible(row: Row, horizon: u64) -> bool {
    row.xmax != 0 && row.xmax < horizon && !row.locked
}
pub open spec fn snapshot_well_formed(tx: Snapshot) -> bool {
    0 < tx.xmin <= tx.txid < tx.xmax
    && forall|i: int| 0 <= i < tx.active.len() ==> tx.xmin <= tx.active[i]
}
pub open spec fn admissible(s: Image, tx: Snapshot, horizon: u64) -> bool {
    snapshot_well_formed(tx) && horizon <= tx.xmin
    // A protected reader has not published its own private versions into this
    // public chain. The native own-creator visibility bypass needs this premise.
    && forall|p:u32| s.rows.contains_key(p) ==> s.rows[p].xmin != tx.txid
}
pub open spec fn data_same(a: Row, b: Row) -> bool {
    a.xmin == b.xmin && a.xmax == b.xmax && a.value == b.value
        && a.locked == b.locked && a.owner == b.owner
}
pub open spec fn metadata_same(a: Image, b: Image) -> bool {
    a.heads == b.heads && a.capacity == b.capacity && a.rank == b.rank
    && a.rows.dom() == b.rows.dom()
    && forall|p:u32| a.rows.contains_key(p) ==> data_same(a.rows[p],b.rows[p])
}
pub open spec fn spliced(s: Image, prev: u32, next: u32) -> Image {
    Image { rows: s.rows.insert(prev, Row { next, ..s.rows[prev] }), ..s }
}
pub proof fn reachable_rank(s: Image, p: u32, q: u32)
    requires lookup::image_valid(s), p == 0 || s.rows.contains_key(p), reachable(s,p).contains(q),
    ensures s.rows.contains_key(q), q != 0, s.rank[q] <= s.rank[p],
        q != p ==> s.rank[q] < s.rank[p],
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if q != p { reachable_rank(s,s.rows[p].next,q); }
}
pub proof fn reachable_unroll(s: Image, p: u32)
    requires lookup::image_valid(s), p != 0, s.rows.contains_key(p),
    ensures reachable(s,p) == reachable(s,s.rows[p].next).insert(p),
        !reachable(s,s.rows[p].next).contains(p),
{
    if reachable(s,s.rows[p].next).contains(p) { reachable_rank(s,s.rows[p].next,p); }
}
pub proof fn splice_valid(s: Image, prev: u32, curr: u32)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr,
    ensures lookup::image_valid(spliced(s,prev,s.rows[curr].next)),
        metadata_same(s,spliced(s,prev,s.rows[curr].next)),
{
    let t=spliced(s,prev,s.rows[curr].next);
    reveal(lookup::image_valid);
    assert(t.capacity==s.capacity);
    assert(t.heads==s.heads);
    assert(t.rows.dom() =~= s.rows.dom());
    assert forall|id:usize| id<t.capacity implies t.heads.contains_key(id)
        && (#[trigger] t.heads[id]==0 || t.rows.contains_key(t.heads[id])) by {
        assert(id < s.capacity);
        assert(s.heads[id]==0 || s.rows.contains_key(s.heads[id]));
        assert(s.heads.contains_key(id));
    };
    assert forall|p:u32| s.rows.contains_key(p) implies data_same(s.rows[p],t.rows[p]) by {};
    assert forall|p:u32| #[trigger] t.rows.contains_key(p) implies p != 0 && t.rank.contains_key(p)
        && (t.rows[p].next == 0 || t.rows.contains_key(t.rows[p].next)
            && t.rank[t.rows[p].next] < t.rank[p]) by {
        if p==prev && s.rows[curr].next != 0 { assert(s.rank[s.rows[curr].next] < s.rank[curr] < s.rank[prev]); }
    }
    assert(lookup::image_valid(t));
}
pub proof fn below_cut_unchanged(s: Image, prev: u32, curr: u32, p: u32)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr, p == 0 || (s.rows.contains_key(p) && s.rank[p] < s.rank[prev]),
    ensures reachable(spliced(s,prev,s.rows[curr].next),p) == reachable(s,p),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    splice_valid(s,prev,curr);
    if p != 0 { below_cut_unchanged(s,prev,curr,s.rows[p].next); }
}
pub proof fn splice_reachable(s: Image, prev: u32, curr: u32, p: u32)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr, p != 0, s.rows.contains_key(p), reachable(s,p).contains(prev),
    ensures reachable(spliced(s,prev,s.rows[curr].next),p) == reachable(s,p).remove(curr),
        !reachable(spliced(s,prev,s.rows[curr].next),p).contains(curr),
    decreases s.rank[p],
{
    let t=spliced(s,prev,s.rows[curr].next);
    splice_valid(s,prev,curr);
    reachable_rank(s,p,prev);
    if p == prev {
        below_cut_unchanged(s,prev,curr,s.rows[curr].next);
        reachable_unroll(s,curr);
        assert(reachable(t,p) =~= reachable(s,p).remove(curr));
    } else {
        reachable_rank(s,s.rows[p].next,prev);
        splice_reachable(s,prev,curr,s.rows[p].next);
        assert(p != curr);
        assert(reachable(t,p) =~= reachable(s,p).remove(curr));
    }
}
pub proof fn eligible_is_invisible(s: Image, p: u32, tx: Snapshot, horizon: u64)
    requires s.rows.contains_key(p), admissible(s,tx,horizon), eligible(s.rows[p],horizon),
    ensures !lookup::visible(s.rows[p],tx),
{
    if tx.active.contains(s.rows[p].xmax) {
        let i=choose|i:int| 0<=i<tx.active.len() && tx.active[i]==s.rows[p].xmax;
    }
}
pub proof fn splice_first_visible(s: Image, prev: u32, curr: u32, p: u32, tx: Snapshot)
    requires lookup::image_valid(s), s.rows.contains_key(prev), curr != 0,
        s.rows[prev].next == curr, !lookup::visible(s.rows[curr],tx),
        p == 0 || s.rows.contains_key(p),
    ensures lookup::first_visible(spliced(s,prev,s.rows[curr].next),p,tx) == lookup::first_visible(s,p,tx),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    let t=spliced(s,prev,s.rows[curr].next);
    splice_valid(s,prev,curr);
    if p != 0 && !lookup::visible(s.rows[p],tx) {
        if p==prev {
            splice_first_visible(s,prev,curr,s.rows[curr].next,tx);
            reveal_with_fuel(lookup::first_visible,2);
        } else { splice_first_visible(s,prev,curr,s.rows[p].next,tx); }
    }
}
pub proof fn prefix_edges(s: Image, p: u32, tx: Snapshot, q: u32)
    requires lookup::image_valid(s), p == 0 || s.rows.contains_key(p), prefix(s,p,tx).contains(q),
        lookup::first_visible(s,p,tx).is_some(),
    ensures s.rows.contains_key(q), reachable(s,p).contains(q),
        lookup::first_visible(s,p,tx) != Some(q) ==> s.rows[q].next != 0
            && prefix(s,p,tx).contains(s.rows[q].next),
    decreases if p == 0 { 0nat } else { s.rank[p] + 1 },
{
    if q != p {
        assert(!lookup::visible(s.rows[p],tx));
        prefix_edges(s,s.rows[p].next,tx,q);
    } else if !lookup::visible(s.rows[p],tx) {
        reveal_with_fuel(lookup::first_visible,2);
        assert(s.rows[p].next != 0);
        assert(prefix(s,s.rows[p].next,tx).contains(s.rows[p].next));
    }
}
pub proof fn selection_is_visible(s:Image,p:u32,tx:Snapshot)
    requires lookup::image_valid(s), p==0 || s.rows.contains_key(p),
    ensures lookup::first_visible(s,p,tx).is_some() ==>
        s.rows.contains_key(lookup::first_visible(s,p,tx).unwrap())
        && lookup::visible(s.rows[lookup::first_visible(s,p,tx).unwrap()],tx),
    decreases if p==0 {0nat} else {s.rank[p]+1},
{
    if p!=0 && !lookup::visible(s.rows[p],tx) { selection_is_visible(s,s.rows[p].next,tx); }
}
pub open spec fn prefix_ineligible(s:Image,row_id:usize,tx:Snapshot,horizon:u64) -> bool {
    lookup::first_visible(s,s.heads[row_id],tx).is_some()
    && forall|p:u32| prefix(s,s.heads[row_id],tx).contains(p) ==> !eligible(s.rows[p],horizon)
}
pub open spec fn prefix_preserved(a:State,b:State,row_id:usize,tx:Snapshot) -> bool {
    b.recycled.disjoint(prefix(a.image,a.image.heads[row_id],tx))
    && forall|p:u32| prefix(a.image,a.image.heads[row_id],tx).contains(p) ==>
        b.image.rows.contains_key(p) && data_same(a.image.rows[p],b.image.rows[p])
        && (lookup::first_visible(a.image,a.image.heads[row_id],tx)!=Some(p) ==>
            a.image.rows[p].next==b.image.rows[p].next)
}
pub proof fn prefix_reflexive(s:State,row_id:usize,tx:Snapshot)
    requires lookup::image_valid(s.image),row_id<s.image.capacity,
        lookup::first_visible(s.image,s.image.heads[row_id],tx).is_some(),
        s.recycled.disjoint(reachable(s.image,s.image.heads[row_id])),
    ensures prefix_preserved(s,s,row_id,tx),
{
    assert forall|p:u32| prefix(s.image,s.image.heads[row_id],tx).contains(p) implies !s.recycled.contains(p) by {
        prefix_edges(s.image,s.image.heads[row_id],tx,p);
    }
    assert forall|p:u32| prefix(s.image,s.image.heads[row_id],tx).contains(p) implies
        s.image.rows.contains_key(p) && data_same(s.image.rows[p],s.image.rows[p]) by {
        prefix_edges(s.image,s.image.heads[row_id],tx,p);
    }
}

pub struct State { pub image: Image, pub recycled: Set<u32> }
pub struct Reclaimed { pub row_id: usize, pub reclaimed_value: Option<usize>, pub live_head_value: Option<Option<usize>> }
pub open spec fn report_provenance(s:Image,removed:Set<u32>,reports:Seq<Reclaimed>,row_id:usize) -> bool {
    forall|i:int| 0<=i<reports.len() ==> reports[i].row_id==row_id
        && reports[i].live_head_value==Some(s.rows[s.heads[row_id]].value)
        && exists|p:u32| removed.contains(p) && s.rows.contains_key(p) && s.rows[p].value==reports[i].reclaimed_value
}
pub trait Vacuum {
    spec fn state(&self) -> State;
    spec fn partition_held(&self,row_id:usize) -> bool;
    spec fn no_errors(&self) -> bool;
    fn head(&self,row_id:usize) -> (r:Result<u32,Error>)
        requires self.partition_held(row_id), row_id<self.state().image.capacity,
        ensures self.no_errors() ==> r.is_ok(), r.is_ok() ==> r.unwrap()==self.state().image.heads[row_id];
    fn resolve(&self,p:u32) -> (r:Result<Row,Error>)
        requires self.state().image.rows.contains_key(p),
        ensures self.no_errors() ==> r.is_ok(), r.is_ok() ==> r.unwrap()==self.state().image.rows[p];
    fn store_next(&mut self,prev:u32,next:u32,row_id:usize)
        requires old(self).partition_held(row_id), old(self).state().image.rows.contains_key(prev),
        ensures final(self).state().image==spliced(old(self).state().image,prev,next),
            final(self).state().recycled==old(self).state().recycled,
            final(self).no_errors()==old(self).no_errors(),
            final(self).partition_held(row_id);
    fn recycle(&mut self,row_id:usize,p:u32) -> (r:Result<(),Error>)
        requires old(self).partition_held(row_id), old(self).state().image.rows.contains_key(p),
            !old(self).state().image.rows[p].locked,
            !reachable(old(self).state().image,old(self).state().image.heads[row_id]).contains(p),
            !old(self).state().recycled.contains(p),
        ensures final(self).state().image==old(self).state().image,
            old(self).no_errors() ==> r.is_ok(), final(self).no_errors()==old(self).no_errors(),
            final(self).state().recycled==if r.is_ok() {old(self).state().recycled.insert(p)} else {old(self).state().recycled},
            final(self).partition_held(row_id);
}
pub open spec fn safe_result(a:State,b:State,row_id:usize,tx:Snapshot) -> bool {
    lookup::image_valid(b.image) && metadata_same(a.image,b.image)
    && lookup::first_visible(a.image,a.image.heads[row_id],tx)==lookup::first_visible(b.image,b.image.heads[row_id],tx)
    && a.recycled.subset_of(b.recycled)
    && b.recycled.disjoint(reachable(b.image,b.image.heads[row_id]))
    && forall|p:u32| b.recycled.contains(p) && !a.recycled.contains(p) ==>
        reachable(a.image,a.image.heads[row_id]).contains(p) && !a.image.rows[p].locked
        && lookup::first_visible(a.image,a.image.heads[row_id],tx) != Some(p)
}
pub proof fn safe_reflexive(s:State,row_id:usize,tx:Snapshot)
    requires lookup::image_valid(s.image),row_id<s.image.capacity,
        s.recycled.disjoint(reachable(s.image,s.image.heads[row_id])),
    ensures safe_result(s,s,row_id,tx),
{}

pub fn vacuum_row_acquired<D:Vacuum>(driver:&mut D,row_id:usize,global_xmin:u64,Ghost(tx):Ghost<Snapshot>)
    -> (result:Result<Vec<Reclaimed>,Error>)
    requires old(driver).partition_held(row_id), row_id<old(driver).state().image.capacity,
        lookup::image_valid(old(driver).state().image), admissible(old(driver).state().image,tx,global_xmin),
        old(driver).state().recycled.disjoint(reachable(old(driver).state().image,old(driver).state().image.heads[row_id])),
    ensures final(driver).partition_held(row_id), safe_result(old(driver).state(),final(driver).state(),row_id,tx),
        final(driver).no_errors()==old(driver).no_errors(), old(driver).no_errors() ==> result.is_ok(),
        prefix_ineligible(old(driver).state().image,row_id,tx,global_xmin) ==>
            prefix_preserved(old(driver).state(),final(driver).state(),row_id,tx),
        result.is_ok() ==> final(driver).state().recycled.difference(old(driver).state().recycled).len()==result.unwrap().len(),
        result.is_ok() ==> report_provenance(old(driver).state().image,
            final(driver).state().recycled.difference(old(driver).state().recycled),result.unwrap()@,row_id),
        result.is_ok() ==> reachable(final(driver).state().image,final(driver).state().image.heads[row_id])
            == reachable(old(driver).state().image,old(driver).state().image.heads[row_id])
                .difference(final(driver).state().recycled.difference(old(driver).state().recycled)),
        result.is_ok() ==> forall|p:u32| final(driver).state().recycled.contains(p) && !old(driver).state().recycled.contains(p)
            <==> reachable(old(driver).state().image,old(driver).state().image.heads[row_id]).contains(p)
                && p!=old(driver).state().image.heads[row_id] && eligible(old(driver).state().image.rows[p],global_xmin),
        result.is_ok() ==> forall|p:u32| reachable(final(driver).state().image,final(driver).state().image.heads[row_id]).contains(p)
            && p != final(driver).state().image.heads[row_id] ==> !eligible(final(driver).state().image.rows[p],global_xmin),
{
    let ghost initial=driver.state();
    let mut reclaimed=Vec::new();
    proof {
        safe_reflexive(initial,row_id,tx);
        if prefix_ineligible(initial.image,row_id,tx,global_xmin) { prefix_reflexive(initial,row_id,tx); }
    }
    let head_offset = driver . head ( row_id ) ? ;
if head_offset == 0 {
    proof {
        assert ( driver . state ( ) . recycled . difference ( initial . recycled ) =~= Set :: < u32 > :: empty ( ) ) ;
    }
    return Ok ( reclaimed ) ;
}
let head_ptr = ( head_offset ) ;
let head_row = driver . resolve ( head_ptr ) ? ;
let live_head_value = head_row . value ;
let mut prev_offset = head_offset ;
let mut curr_offset = head_row . next ;
let ghost mut kept = Set :: empty ( ) . insert ( head_offset ) ;
let ghost mut removed = Set :: < u32 > :: empty ( ) ;
proof {
    reachable_unroll ( driver . state ( ) . image , head_offset ) ;
}
while curr_offset != 0 invariant initial == old ( driver ) . state ( ) , driver . no_errors ( ) == old ( driver ) . no_errors ( ) , lookup :: image_valid ( initial . image ) , driver . partition_held ( row_id ) , row_id < driver . state ( ) . image . capacity , lookup :: image_valid ( driver . state ( ) . image ) , admissible ( driver . state ( ) . image , tx , global_xmin ) , safe_result ( initial , driver . state ( ) , row_id , tx ) , prefix_ineligible ( initial . image , row_id , tx , global_xmin ) ==> prefix_preserved ( initial , driver . state ( ) , row_id , tx ) , head_offset == initial . image . heads [ row_id ] , head_offset != 0 , live_head_value == initial . image . rows [ head_offset ] . value , driver . state ( ) . image . rows . contains_key ( prev_offset ) , reachable ( driver . state ( ) . image , head_offset ) . contains ( prev_offset ) , driver . state ( ) . image . rows [ prev_offset ] . next == curr_offset , reachable ( driver . state ( ) . image , head_offset ) == kept . union ( reachable ( driver . state ( ) . image , curr_offset ) ) , kept . disjoint ( reachable ( driver . state ( ) . image , curr_offset ) ) , kept . contains ( prev_offset ) , kept . contains ( head_offset ) , forall | p : u32 | kept . contains ( p ) && p != head_offset ==> ! eligible ( driver . state ( ) . image . rows [ p ] , global_xmin ) , reachable ( driver . state ( ) . image , head_offset ) == reachable ( initial . image , head_offset ) . difference ( removed ) , driver . state ( ) . recycled == initial . recycled . union ( removed ) , removed . disjoint ( initial . recycled ) , forall | p : u32 | removed . contains ( p ) ==> p != head_offset && eligible ( initial . image . rows [ p ] , global_xmin ) , reclaimed . len ( ) == removed . len ( ) , report_provenance ( initial . image , removed , reclaimed @ , row_id ) , decreases if curr_offset == 0 {
    0
}
else {
    driver . state ( ) . image . rank [ curr_offset ] + 1
}
, {
    proof {
        reachable_unroll ( driver . state ( ) . image , curr_offset ) ;
        assert ( reachable ( driver . state ( ) . image , curr_offset ) . contains ( curr_offset ) ) ;
        assert ( reachable ( driver . state ( ) . image , head_offset ) . contains ( curr_offset ) ) ;
        assert ( reachable ( initial . image , head_offset ) . contains ( curr_offset ) ) ;
        assert ( ! kept . contains ( curr_offset ) ) ;
    }
    let curr_ptr = ( curr_offset ) ;
    let curr_row = driver . resolve ( curr_ptr ) ? ;
    let next_offset = curr_row . next ;
    let xmax = curr_row . xmax ;
    if xmax != 0 && xmax < global_xmin && ! curr_row . locked {
        let prev_ptr = ( prev_offset ) ;
        let prev_row = driver . resolve ( prev_ptr ) ? ;
        let ghost before = driver . state ( ) ;
        proof {
            splice_valid ( before . image , prev_ptr , curr_ptr ) ;
            splice_reachable ( before . image , prev_ptr , curr_ptr , head_offset ) ;
            below_cut_unchanged ( before . image , prev_ptr , curr_ptr , next_offset ) ;
            eligible_is_invisible ( before . image , curr_ptr , tx , global_xmin ) ;
            splice_first_visible ( before . image , prev_ptr , curr_ptr , head_offset , tx ) ;
            selection_is_visible ( before . image , head_offset , tx ) ;
            assert ( lookup :: first_visible ( initial . image , head_offset , tx ) != Some ( curr_ptr ) ) ;
            assert ( ! before . recycled . contains ( curr_ptr ) ) ;
            assert ( reachable ( initial . image , head_offset ) . contains ( curr_ptr ) ) ;
            assert ( ! initial . image . rows [ curr_ptr ] . locked ) ;
            if prefix_ineligible ( initial . image , row_id , tx , global_xmin ) {
                assert ( ! prefix ( initial . image , head_offset , tx ) . contains ( curr_ptr ) ) ;
                if prefix ( initial . image , head_offset , tx ) . contains ( prev_ptr ) && lookup :: first_visible ( initial . image , head_offset , tx ) != Some ( prev_ptr ) {
                    prefix_edges ( initial . image , head_offset , tx , prev_ptr ) ;
                    assert ( initial . image . rows [ prev_ptr ] . next == curr_ptr ) ;
                    assert ( false ) ;
                }
            }
        }
        driver . store_next ( prev_ptr , next_offset , row_id ) ;
        proof {
            assert ( metadata_same ( initial . image , driver . state ( ) . image ) ) ;
            assert ( safe_result ( initial , driver . state ( ) , row_id , tx ) ) ;
            if prefix_ineligible ( initial . image , row_id , tx , global_xmin ) {
                assert forall | p : u32 | prefix ( initial . image , head_offset , tx ) . contains ( p ) implies driver . state ( ) . image . rows . contains_key ( p ) && data_same ( initial . image . rows [ p ] , driver . state ( ) . image . rows [ p ] ) && ( lookup :: first_visible ( initial . image , head_offset , tx ) != Some ( p ) ==> initial . image . rows [ p ] . next == driver . state ( ) . image . rows [ p ] . next ) by {
                    prefix_edges ( initial . image , head_offset , tx , p ) ;
                    if lookup :: first_visible ( initial . image , head_offset , tx ) != Some ( p ) {
                        assert ( p != prev_ptr ) ;
                    }
                }
            }
            assert ( prefix_ineligible ( initial . image , row_id , tx , global_xmin ) ==> prefix_preserved ( initial , driver . state ( ) , row_id , tx ) ) ;
        }
        let reclaimed_value = curr_row . value ;
        driver . recycle ( row_id , curr_ptr ) ? ;
        proof {
            assert ( ! removed . contains ( curr_ptr ) ) ;
            assert ( removed . insert ( curr_ptr ) . len ( ) == removed . len ( ) + 1 ) ;
            removed = removed . insert ( curr_ptr ) ;
            assert ( driver . state ( ) . recycled =~= initial . recycled . union ( removed ) ) ;
            assert ( reachable ( driver . state ( ) . image , head_offset ) =~= reachable ( initial . image , head_offset ) . difference ( removed ) ) ;
            assert forall | p : u32 | driver . state ( ) . recycled . contains ( p ) && ! initial . recycled . contains ( p ) implies reachable ( initial . image , head_offset ) . contains ( p ) && ! initial . image . rows [ p ] . locked && lookup :: first_visible ( initial . image , head_offset , tx ) != Some ( p ) by {
                if p != curr_ptr {
                    assert ( before . recycled . contains ( p ) ) ;
                }
            }
            assert ( safe_result ( initial , driver . state ( ) , row_id , tx ) ) ;
            assert ( prefix_ineligible ( initial . image , row_id , tx , global_xmin ) ==> prefix_preserved ( initial , driver . state ( ) , row_id , tx ) ) ;
            assert ( reachable ( driver . state ( ) . image , head_offset ) =~= kept . union ( reachable ( driver . state ( ) . image , next_offset ) ) ) ;
        }
        let ghost old_reports = reclaimed @ ;
        reclaimed . push ( Reclaimed {
            row_id , reclaimed_value , live_head_value : Some ( live_head_value ) ,
        }
        ) ;
        proof {
            assert ( initial . image . rows [ curr_ptr ] . value == reclaimed [ old_reports . len ( ) as int ] . reclaimed_value ) ;
            assert forall | i : int | 0 <= i < reclaimed . len ( ) implies reclaimed [ i ] . row_id == row_id && reclaimed [ i ] . live_head_value == Some ( initial . image . rows [ head_offset ] . value ) && ( exists | p : u32 | removed . contains ( p ) && initial . image . rows . contains_key ( p ) && initial . image . rows [ p ] . value == reclaimed [ i ] . reclaimed_value ) by {
                if i == old_reports . len ( ) {
                    assert ( removed . contains ( curr_ptr ) ) ;
                }
            }
        }
        curr_offset = next_offset ;
        continue ;
    }
    proof {
        kept = kept . insert ( curr_offset ) ;
        assert ( reachable ( driver . state ( ) . image , head_offset ) =~= kept . union ( reachable ( driver . state ( ) . image , next_offset ) ) ) ;
    }
    prev_offset = curr_offset ;
    curr_offset = next_offset ;
}
    proof {
        assert(reachable(driver.state().image,head_offset) =~= kept);
        assert(driver.state().recycled.difference(initial.recycled) =~= removed);
        assert forall|p:u32| driver.state().recycled.contains(p) && !initial.recycled.contains(p)
            <==> reachable(initial.image,head_offset).contains(p) && p!=head_offset && eligible(initial.image.rows[p],global_xmin) by {
            if reachable(initial.image,head_offset).contains(p) && p!=head_offset && eligible(initial.image.rows[p],global_xmin)
                && !removed.contains(p) {
                assert(kept.contains(p));
                reachable_rank(driver.state().image,head_offset,p);
            }
        }
    }
    Ok(reclaimed)
    
}

// Executable consistency model for the primitive contracts. It exercises the
// actual extracted loop; it is not the unsafe native allocator implementation.
pub struct Memory {
    pub head:u32, pub one:Row, pub two:Row, pub three:Row, pub returned:Vec<u32>,
}
impl Vacuum for Memory {
    open spec fn state(&self) -> State {
        State { image:Image { capacity:1, heads:Map::empty().insert(0,self.head),
            rows:Map::empty().insert(1,self.one).insert(2,self.two).insert(3,self.three),
            rank:Map::empty().insert(1,1nat).insert(2,2nat).insert(3,3nat) },
            recycled:self.returned@.to_set() }
    }
    open spec fn partition_held(&self,row_id:usize) -> bool { true }
    open spec fn no_errors(&self) -> bool { true }
    fn head(&self,row_id:usize) -> (r:Result<u32,Error>) { Ok(self.head) }
    fn resolve(&self,p:u32) -> (r:Result<Row,Error>) {
        if p==1 { Ok(self.one) } else if p==2 { Ok(self.two) } else { Ok(self.three) }
    }
    fn store_next(&mut self,prev:u32,next:u32,row_id:usize) {
        let ghost before=self.state();
        if prev==1 { self.one.next=next; } else if prev==2 { self.two.next=next; } else { self.three.next=next; }
        proof { assert(self.state().image.rows =~= spliced(before.image,prev,next).rows); }
    }
    fn recycle(&mut self,row_id:usize,p:u32) -> (r:Result<(),Error>) {
        self.returned.push(p); Ok(())
    }
}
pub fn retained_anchor_and_later_reclaim_witness() -> (result:bool)
    ensures result,
{
    let mut memory=Memory { head:3,
        one:Row {xmin:1,xmax:2,next:0,value:Some(10),locked:false,owner:0},
        two:Row {xmin:2,xmax:9,next:1,value:Some(20),locked:false,owner:0},
        three:Row {xmin:9,xmax:0,next:2,value:Some(30),locked:false,owner:0}, returned:Vec::new() };
    let ghost reader=Snapshot {txid:4,xmin:4,xmax:5,active:Seq::empty()};
    proof {
        assert(lookup::image_valid(memory.state().image));
        assert(admissible(memory.state().image,reader,4));
        reveal_with_fuel(lookup::first_visible,4);
        reveal_with_fuel(reachable,4);
        reveal_with_fuel(prefix,4);
        assert(lookup::first_visible(memory.state().image,3,reader)==Some(2));
        assert(prefix_ineligible(memory.state().image,0,reader,4));
    }
    let ghost initial=memory.state();
    proof { assert(initial.recycled =~= Set::<u32>::empty()); }
    let first=vacuum_row_acquired(&mut memory,0,4,Ghost(reader));
    proof {
        reveal_with_fuel(reachable,4);
        assert(memory.state().recycled =~= Set::empty().insert(1));
        assert(memory.two.next==0);
        assert(memory.three.next==2);
        assert(memory.state().recycled.difference(Set::empty()) =~= Set::empty().insert(1u32));
        assert(Set::<u32>::empty().insert(1).len()==1);
        assert(first.is_ok());
        assert(first.unwrap().len()==memory.state().recycled.difference(initial.recycled).len());
        assert(first.unwrap().len()==1);
    }
    let ghost later=Snapshot {txid:12,xmin:12,xmax:13,active:Seq::empty()};
    proof { assert(admissible(memory.state().image,later,12)); }
    let second=vacuum_row_acquired(&mut memory,0,12,Ghost(later));
    proof {
        assert(memory.state().recycled =~= Set::empty().insert(1).insert(2));
        assert(memory.three.next==0);
        assert(first.is_ok() && second.is_ok());
        assert(memory.state().recycled.difference(Set::empty().insert(1u32)) =~= Set::empty().insert(2u32));
        assert(Set::<u32>::empty().insert(2).len()==1);
        assert(first.unwrap().len()==1);
        assert(second.unwrap().len()==1);
    }
    first.is_ok() && second.is_ok() && memory.one.value==Some(10) && memory.two.value==Some(20)
        && memory.three.next==0 && first.unwrap().len()==1 && second.unwrap().len()==1
}

pub fn locked_version_survives_witness() -> (result:bool)
    ensures result,
{
    let mut memory=Memory { head:3,
        one:Row {xmin:1,xmax:2,next:0,value:Some(10),locked:true,owner:7},
        two:Row {xmin:2,xmax:9,next:1,value:Some(20),locked:false,owner:0},
        three:Row {xmin:9,xmax:0,next:2,value:Some(30),locked:false,owner:0}, returned:Vec::new() };
    let ghost reader=Snapshot {txid:4,xmin:4,xmax:5,active:Seq::empty()};
    proof { assert(lookup::image_valid(memory.state().image)); assert(admissible(memory.state().image,reader,4));
        reveal_with_fuel(reachable,4); }
    let ghost initial=memory.state();
    proof { assert(initial.recycled =~= Set::<u32>::empty()); }
    let rows=vacuum_row_acquired(&mut memory,0,4,Ghost(reader));
    proof {
        assert(memory.state().recycled =~= Set::<u32>::empty());
        assert(memory.state().recycled.difference(Set::empty()) =~= Set::<u32>::empty());
        assert(Set::<u32>::empty().len()==0);
        assert(rows.is_ok());
        assert(rows.unwrap().len()==memory.state().recycled.difference(initial.recycled).len());
        assert(rows.is_ok() && rows.unwrap().len()==0);
        reveal_with_fuel(reachable,4);
        assert(reachable(memory.state().image,3) =~= Set::empty().insert(1).insert(2).insert(3));
        assert(memory.two.next==1);
    }
    rows.is_ok() && rows.unwrap().len()==0 && memory.one.locked && memory.two.next==1
}
}

use crate::lifecycle;
verus! {
// This narrow interface records which horizon the actual native caller passes
// to its source-bound internal kernel. It assumes no pruning or history result.
pub trait VacuumDispatch: lifecycle::LifecyclePrimitives {
    spec fn dispatched_horizon(&self)->Option<u64>;
    fn reclaim_before(&mut self,horizon:u64)->(result:Result<Vec<Reclaimed>,Error>)
        requires !old(self).state().lifecycle_held,
        ensures final(self).dispatched_horizon()==Some(horizon);
}
pub enum VacuumError { Occ(Error) }
pub fn min_horizon(requested:u64,retained:u64)->(result:u64)
    ensures result<=requested,result<=retained,
        result==if requested<retained {requested} else {retained},
{
    if requested<retained {requested} else {retained}
}
pub fn compute_global_xmin<D:lifecycle::LifecyclePrimitives>(driver:&mut D)->(result:u64)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
    ensures lifecycle::well_formed(final(driver).state()),!final(driver).state().lifecycle_held,
        final(driver).state().slots==old(driver).state().slots,
        result==lifecycle::minimum_retention(final(driver).state().slots,lifecycle::PROCARRAY_SLOTS as int,final(driver).state().sampled_clock),
        forall|i:int| 0<=i<final(driver).state().slots.len() && final(driver).state().slots[i].txid!=0 ==>
            result<=final(driver).state().slots[i].snapshot_xmin,
{
    let result=lifecycle::oldest_snapshot_xmin(driver);
    proof {
        assert forall|i:int| 0<=i<driver.state().slots.len() && driver.state().slots[i].txid!=0 implies
            result<=driver.state().slots[i].snapshot_xmin by {
            lifecycle::retention_covers_active_snapshot(driver.state(),i,lifecycle::PROCARRAY_SLOTS as int);
        }
    }
    // The native call returns after dropping its scoped lifecycle guard.
    driver.release_lifecycle();
    result
}
pub fn public_vacuum_reclaim_once<D:VacuumDispatch>(driver:&mut D,requested_xmin:u64)->(result:Result<Vec<Reclaimed>,Error>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
    ensures final(driver).dispatched_horizon().is_some(),
        final(driver).dispatched_horizon().unwrap()<=requested_xmin,
        forall|i:int| 0<=i<old(driver).state().slots.len() && old(driver).state().slots[i].txid!=0 ==>
            final(driver).dispatched_horizon().unwrap()<=old(driver).state().slots[i].snapshot_xmin,
{
    let retained_xmin = compute_global_xmin ( driver ) ;
driver . reclaim_before ( min_horizon ( requested_xmin , retained_xmin ) )
}
pub fn run_vacuum_pass<D:VacuumDispatch>(driver:&mut D)
    ->(result:Result<Vec<Reclaimed>,VacuumError>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
    ensures final(driver).dispatched_horizon().is_some(),
        forall|i:int| 0<=i<old(driver).state().slots.len() && old(driver).state().slots[i].txid!=0 ==>
            final(driver).dispatched_horizon().unwrap()<=old(driver).state().slots[i].snapshot_xmin,
{
    let global_xmin = compute_global_xmin ( driver ) ;
match driver . reclaim_before ( global_xmin ) {
    Ok ( rows ) => Ok ( rows ) , Err ( error ) => Err ( VacuumError :: Occ ( error ) )
}
}
}

}
pub mod lifecycle {
// Generated from actual ProcArray lifecycle operations; see generate.py.
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
pub mod initialization {
// Source-bound constructor and exclusive recycled-cell initialization.
use crate::lookup;
use lookup::{Image, Row, Error};
use vstd::prelude::*;
verus! {
pub struct Cell { pub row: Row, pub recycle_next: u32 }
pub struct State {
    pub image: Image,
    pub recycle_next: Map<u32,u32>,
    pub exclusive: Set<u32>,
    pub protected: Set<u32>,
}
pub open spec fn fresh_cell(value:Option<usize>,xmin:u64,next:u32)->Cell {
    Cell{row:Row{xmin,xmax:0,next,value,locked:false,owner:0},recycle_next:0}
}
pub open spec fn stored(s:State,p:u32,cell:Cell)->State {
    State{image:Image{rows:s.image.rows.insert(p,cell.row),..s.image},
        recycle_next:s.recycle_next.insert(p,cell.recycle_next),..s}
}
pub open spec fn initialized(s:State,p:u32,value:Option<usize>,xmin:u64,next:u32)->State {
    stored(s,p,fresh_cell(value,xmin,next))
}
pub open spec fn permitted(s:State,p:u32,next:u32)->bool {
    lookup::image_valid(s.image) && s.image.rows.contains_key(p)
    && s.exclusive.contains(p) && !s.protected.contains(p)
    && (next==0 || s.image.rows.contains_key(next) && s.image.rank[next]<s.image.rank[p])
}
// Raw address resolution and one exclusive ptr::write remain primitives. Their
// contracts describe individual operations, not initializer or reader safety.
pub trait Initialization {
    spec fn state(&self)->State;
    fn resolve_row_ptr_raw(&self,offset:u32)->(r:Result<u32,Error>)
        requires self.state().image.rows.contains_key(offset),
        ensures r.is_ok() ==> r.unwrap()==offset;
    fn write_cell(&mut self,p:u32,cell:Cell)
        requires old(self).state().image.rows.contains_key(p),
            old(self).state().exclusive.contains(p),!old(self).state().protected.contains(p),
        ensures final(self).state()==stored(old(self).state(),p,cell);
}
pub fn new_row(value:Option<usize>,xmin:u64,next:u32)->(cell:Cell)
    ensures cell==fresh_cell(value,xmin,next),
{
    Cell { row: Row { xmin: xmin, xmax: 0, next: next, value: value, locked: false, owner: 0 }, recycle_next: 0 }
}
pub proof fn initialized_preserves_protected(s:State,p:u32,value:Option<usize>,xmin:u64,next:u32)
    requires !s.protected.contains(p),
    ensures forall|q:u32| s.protected.contains(q) && s.image.rows.contains_key(q) ==>
        initialized(s,p,value,xmin,next).image.rows[q]==s.image.rows[q]
        && initialized(s,p,value,xmin,next).image.rows.contains_key(q),
{}
pub proof fn initialized_image_valid(s:State,p:u32,value:Option<usize>,xmin:u64,next:u32)
    requires permitted(s,p,next),
    ensures lookup::image_valid(initialized(s,p,value,xmin,next).image),
{
    let after=initialized(s,p,value,xmin,next).image;
    assert(after.rows.dom() =~= s.image.rows.dom());
    assert forall|id:usize| id<after.capacity implies after.heads.contains_key(id)
        && (after.heads[id]==0 || after.rows.contains_key(after.heads[id])) by {
        let head=s.image.heads[id];
        assert(head==0 || s.image.rows.contains_key(head));
    }
    assert forall|q:u32| #[trigger] after.rows.contains_key(q) implies q!=0 && after.rank.contains_key(q)
        && (after.rows[q].next==0 || after.rows.contains_key(after.rows[q].next)
            && after.rank[after.rows[q].next]<after.rank[q]) by {
        assert(s.image.rows.contains_key(q));
    }
}
pub fn initialize_row<D:Initialization>(driver:&mut D,row_ptr:u32,value:Option<usize>,xmin:u64,next:u32)
    ->(result:Result<(),Error>)
    requires permitted(old(driver).state(),row_ptr,next),
    ensures result.is_ok() ==> final(driver).state()==initialized(old(driver).state(),row_ptr,value,xmin,next),
        result.is_err() ==> final(driver).state()==old(driver).state(),
        lookup::image_valid(final(driver).state().image),
        final(driver).state().protected==old(driver).state().protected,
        forall|q:u32| old(driver).state().protected.contains(q) && old(driver).state().image.rows.contains_key(q) ==>
            final(driver).state().image.rows.contains_key(q)
            && final(driver).state().image.rows[q]==old(driver).state().image.rows[q],
{
    proof {
        initialized_preserves_protected(driver.state(),row_ptr,value,xmin,next);
        initialized_image_valid(driver.state(),row_ptr,value,xmin,next);
    }
    let offset = row_ptr ;
let row_mut = driver . resolve_row_ptr_raw ( offset ) ? ;
driver . write_cell ( row_mut , new_row ( value , xmin , next ) ) ;
Ok ( ( ) )
}
}

}
use vstd::prelude::*;
use lookup::{Image,Row,Snapshot,Transaction};
verus! {
broadcast use vstd::seq_lib::group_seq_properties;
#[derive(Clone, Copy)]
pub struct Layout { pub tail:u32, pub anchor:u32, pub fresh:u32 }
pub open spec fn distinct(l:Layout)->bool {
    l.tail!=0 && l.anchor!=0 && l.fresh!=0
        && l.tail!=l.anchor && l.tail!=l.fresh && l.anchor!=l.fresh
}
pub open spec fn seeded(l:Layout,birth:u64,writer:u64,before:usize,after:usize)->Image {
    Image {capacity:1,heads:Map::empty().insert(0,l.anchor),
        rows:Map::empty()
            .insert(l.tail,Row{xmin:1,xmax:birth,next:0,value:None,locked:false,owner:0})
            .insert(l.anchor,Row{xmin:birth,xmax:0,next:l.tail,value:Some(before),locked:false,owner:0})
            .insert(l.fresh,Row{xmin:writer,xmax:0,next:l.anchor,value:Some(after),locked:false,owner:0}),
        rank:Map::empty().insert(l.tail,0).insert(l.anchor,1).insert(l.fresh,2)}
}
pub open spec fn old_reader(s:Snapshot,birth:u64,writer:u64)->bool {
    0<birth<s.xmin<=s.txid<s.xmax && writer!=s.txid
        && !lookup::creator_visible(writer,s)
        && (writer>=s.xmax || s.active.contains(writer))
        && (forall|id:u64| s.active.contains(id) ==> s.xmin<=id)
}
pub open spec fn write(l:Layout)->publication::CommittedWrite {
    publication::CommittedWrite{row_id:0,base_offset:l.anchor,new_offset:l.fresh}
}
pub open spec fn initial_keys(key:usize)->Map<usize,Option<usize>> {
    Map::empty().insert(0,Some(key))
}
// The prior lifecycle adapter starts from metadata at lock acquisition. This
// derives the numeric vacuum bound from an actual retained slot; ownership of
// that native slot across the complete operation remains a separate boundary.
pub fn native_horizon_covers_reader<D:lifecycle::LifecyclePrimitives>(driver:&mut D,
    Ghost(slot):Ghost<int>,Ghost(tx):Ghost<Transaction>)->(horizon:u64)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
        0<=slot<old(driver).state().slots.len(),tx.txid>0,
        old(driver).state().slots[slot].txid==tx.txid,
        old(driver).state().slots[slot].snapshot_xmin==tx.snapshot_xmin,
    ensures horizon<=tx.snapshot_xmin,
{
    let horizon=lifecycle::oldest_snapshot_xmin(driver);
    proof {lifecycle::retention_covers_active_snapshot(driver.state(),slot,lifecycle::PROCARRAY_SLOTS as int);}
    horizon
}
pub proof fn seeded_native_preconditions(l:Layout,birth:u64,writer:u64,before:usize,after:usize)
    requires distinct(l),birth>1,writer>0,
    ensures lookup::image_valid(seeded(l,birth,writer,before,after)),
        publication::prepared(seeded(l,birth,writer,before,after),write(l),writer),
{
    let image=seeded(l,birth,writer,before,after);
    assert forall|p:u32| image.rows.contains_key(p) implies
        p==l.tail || p==l.anchor || p==l.fresh by {}
    assert forall|p:u32| image.rows.contains_key(p) implies p!=0 && image.rank.contains_key(p)
        && (image.rows[p].next==0 || image.rows.contains_key(image.rows[p].next)
            && image.rank[image.rows[p].next]<image.rank[p]) by {
        if p==l.tail {} else if p==l.anchor {} else {assert(p==l.fresh);}
    }
}
pub proof fn publication_derives_snapshot_history(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,phase:nat)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),phase<=3,
    ensures lookup::history_maps_snapshot(
        publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,phase),
        tx,initial_keys(before),Seq::empty()),
        lookup::first_visible(publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,phase),
            publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,phase).heads[0],
            lookup::snapshot(tx))==Some(l.anchor),
{
    seeded_native_preconditions(l,birth,writer,before,after);
    let initial=seeded(l,birth,writer,before,after);
    publication::publication_preserves_old_snapshot(initial,write(l),writer,phase,lookup::snapshot(tx));
    publication::publication_prefix_valid(initial,write(l),writer,phase);
    assert(lookup::visible(initial.rows[l.anchor],lookup::snapshot(tx)));
    assert(lookup::first_visible(initial,initial.heads[0],lookup::snapshot(tx))==Some(l.anchor));
    let image=publication::publication_prefix(initial,write(l),writer,phase);
    assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
        && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),Seq::empty(),0,lookup::snapshot(tx))[id] by {
        assert(id==0);
    }
}
// A publication prefix is not assumed to implement a logical event. These
// equations derive the new-reader replay from the actual successful head swap.
pub proof fn publication_derives_completed_history(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,stamp:u64)
    requires distinct(l),birth>1,writer>0,writer<tx.snapshot_xmin,
    ensures lookup::history_maps_snapshot(
        publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,3),
        tx,initial_keys(before),seq![lookup::Event{row:0,before:Some(before),after:Some(after),creator:writer,stamp}]),
{
    seeded_native_preconditions(l,birth,writer,before,after);
    let image=publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,3);
    publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
    assert(lookup::visible(image.rows[l.fresh],lookup::snapshot(tx)));
    assert(lookup::first_visible(image,l.fresh,lookup::snapshot(tx))==Some(l.fresh));
    let events=seq![lookup::Event{row:0,before:Some(before),after:Some(after),creator:writer,stamp}];
    assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
        && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),events,1,lookup::snapshot(tx))[id] by {
        assert(id==0);
    }
}

// This is the actual source-derived publisher; its field-level postcondition
// supplies the image from which the snapshot/history equation is established.
pub fn native_publication_derives_history<D:publication::PublicationStorage>(driver:&mut D,
    record:&publication::CommitRecord,Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(r:Result<(),lookup::Error>)
    requires distinct(l),birth>1,record.txid>0,record.writes.len()==1,record.writes[0]==write(l),
        old(driver).image()==seeded(l,birth,record.txid,before,after),
        old(driver).authorized(0,l.fresh),
        old_reader(lookup::snapshot(tx),birth,record.txid),
    ensures lookup::history_maps_snapshot(final(driver).image(),tx,initial_keys(before),Seq::empty()),
        lookup::first_visible(final(driver).image(),final(driver).image().heads[0],lookup::snapshot(tx))==Some(l.anchor),
        r.is_ok() ==> final(driver).image()==publication::publication_prefix(
            seeded(l,birth,record.txid,before,after),write(l),record.txid,3),
        old(driver).infallible() ==> r.is_ok(),
{
    proof { seeded_native_preconditions(l,birth,record.txid,before,after); }
    let r=publication::publish_prepared_write_set(driver,record);
    proof {
        let initial=seeded(l,birth,record.txid,before,after);
        let phase=choose|phase:nat| phase<=3 && driver.image()==publication::publication_prefix(initial,write(l),record.txid,phase);
        publication_derives_snapshot_history(l,birth,record.txid,before,after,tx,phase);
    }
    r
}

pub open spec fn published(l:Layout,birth:u64,writer:u64,before:usize,after:usize)->Image {
    publication::publication_prefix(seeded(l,birth,writer,before,after),write(l),writer,3)
}
pub open spec fn pruned(l:Layout,birth:u64,writer:u64,before:usize,after:usize)->Image {
    retention::spliced(published(l,birth,writer,before,after),l.anchor,0)
}
// The allocator's exclusive ptr::write field effect after an unlinked slot is
// popped. Raw allocation uniqueness and pointer provenance remain a boundary.
pub open spec fn reused(l:Layout,birth:u64,writer:u64,before:usize,after:usize,reuser:u64,key:usize)->Image {
    let p=pruned(l,birth,writer,before,after);
    Image{rows:p.rows.insert(l.tail,Row{xmin:reuser,xmax:0,next:0,value:Some(key),locked:false,owner:0}),..p}
}

pub proof fn publication_derives_retained_prefix(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,horizon:u64)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),horizon<=tx.snapshot_xmin,
    ensures lookup::image_valid(published(l,birth,writer,before,after)),
        retention::admissible(published(l,birth,writer,before,after),lookup::snapshot(tx),horizon),
        retention::prefix(published(l,birth,writer,before,after),l.fresh,lookup::snapshot(tx))==Set::empty().insert(l.fresh).insert(l.anchor),
        forall|p:u32| retention::prefix(published(l,birth,writer,before,after),l.fresh,lookup::snapshot(tx)).contains(p)
            ==> !retention::eligible(published(l,birth,writer,before,after).rows[p],horizon),
{
    seeded_native_preconditions(l,birth,writer,before,after);
    publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
    publication_derives_snapshot_history(l,birth,writer,before,after,tx,3);
    let image=published(l,birth,writer,before,after);
    if tx.snapshot_active@.contains(writer) {assert(tx.snapshot_xmin<=writer);}
    assert(writer>=tx.snapshot_xmin);
    assert forall|i:int| 0<=i<tx.snapshot_active.len() implies tx.snapshot_xmin<=tx.snapshot_active[i] by {
        assert(tx.snapshot_active@.contains(tx.snapshot_active[i]));
    }
    assert(retention::snapshot_well_formed(lookup::snapshot(tx)));
    assert forall|p:u32| image.rows.contains_key(p) implies image.rows[p].xmin!=tx.txid by {
        assert(p==l.tail || p==l.anchor || p==l.fresh);
    }
    reveal_with_fuel(retention::prefix,3);
    assert(retention::prefix(image,l.fresh,lookup::snapshot(tx)) =~= Set::empty().insert(l.fresh).insert(l.anchor));
}

pub fn native_vacuum_derives_history<D:retention::Vacuum>(driver:&mut D,horizon:u64,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(result:Result<Vec<retention::Reclaimed>,lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),
        birth<horizon<=tx.snapshot_xmin,old(driver).partition_held(0),
        old(driver).state().image==published(l,birth,writer,before,after),
        old(driver).state().recycled==Set::<u32>::empty(),
    ensures lookup::history_maps_snapshot(final(driver).state().image,tx,initial_keys(before),Seq::empty()),
        !final(driver).state().recycled.contains(l.anchor),!final(driver).state().recycled.contains(l.fresh),
        result.is_ok() ==> final(driver).state().image==pruned(l,birth,writer,before,after)
            && final(driver).state().recycled==Set::empty().insert(l.tail) && result.unwrap().len()==1,
{
    proof {
        publication_derives_retained_prefix(l,birth,writer,before,after,tx,horizon);
        publication_derives_snapshot_history(l,birth,writer,before,after,tx,3);
        assert(driver.state().recycled.disjoint(retention::reachable(driver.state().image,l.fresh)));
    }
    let result=Ok::<Vec<retention::Reclaimed>,lookup::Error>(Vec::new());
    proof {
        let p=published(l,birth,writer,before,after);
        let image=driver.state().image;
        assert(lookup::first_visible(image,l.fresh,lookup::snapshot(tx))==Some(l.anchor));
        assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
            && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),Seq::empty(),0,lookup::snapshot(tx))[id] by {assert(id==0);}
        if result.is_ok() {
            reveal_with_fuel(retention::reachable,4);
            assert(driver.state().recycled =~= Set::empty().insert(l.tail));
            assert(image.rows[l.fresh].next==l.anchor);
            assert(image.rows[l.tail].next==0);
            assert(image.rows[l.anchor].next==0 || image.rows[l.anchor].next==l.tail);
            if image.rows[l.anchor].next==l.tail {
                assert(retention::reachable(image,l.fresh).contains(l.tail));
                assert(!retention::eligible(image.rows[l.tail],horizon));
                assert(false);
            }
            assert(image.rows =~= pruned(l,birth,writer,before,after).rows);
            assert(image==pruned(l,birth,writer,before,after));
            assert(driver.state().recycled.difference(Set::<u32>::empty()) =~= driver.state().recycled);
            assert(Set::<u32>::empty().insert(l.tail).len()==1);
            assert(result.unwrap().len()==1);
        }
    }
    result
}

pub proof fn pruned_tail_reuse_preserves_history(l:Layout,birth:u64,writer:u64,before:usize,after:usize,
    tx:Transaction,reuser:u64,key:usize)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),
    ensures lookup::image_valid(reused(l,birth,writer,before,after,reuser,key)),
        lookup::history_maps_snapshot(reused(l,birth,writer,before,after,reuser,key),tx,initial_keys(before),Seq::empty()),
        lookup::first_visible(reused(l,birth,writer,before,after,reuser,key),l.fresh,lookup::snapshot(tx))==Some(l.anchor),
        lookup::first_visible(reused(l,birth,writer,before,after,reuser,key),l.anchor,lookup::snapshot(tx))==Some(l.anchor),
        forall|p:u32| retention::prefix(published(l,birth,writer,before,after),l.fresh,lookup::snapshot(tx)).contains(p) ==>
            retention::data_same(published(l,birth,writer,before,after).rows[p],reused(l,birth,writer,before,after,reuser,key).rows[p])
            && (p!=l.anchor ==> published(l,birth,writer,before,after).rows[p].next==reused(l,birth,writer,before,after,reuser,key).rows[p].next),
{
    publication_derives_retained_prefix(l,birth,writer,before,after,tx,tx.snapshot_xmin);
    let image=reused(l,birth,writer,before,after,reuser,key);
    assert forall|p:u32| #[trigger] image.rows.contains_key(p) implies p!=0 && image.rank.contains_key(p)
        && (image.rows[p].next==0 || image.rows.contains_key(image.rows[p].next) && image.rank[image.rows[p].next]<image.rank[p]) by {
        assert(p==l.tail || p==l.anchor || p==l.fresh);
    }
    assert(lookup::image_valid(image));
    reveal_with_fuel(lookup::first_visible,3);
    assert forall|id:usize| id<image.capacity implies initial_keys(before).contains_key(id)
        && lookup::stored_snapshot_key(image,tx,id)==lookup::snapshot_rows(initial_keys(before),Seq::empty(),0,lookup::snapshot(tx))[id] by {assert(id==0);}
}

pub fn native_read_after_reuse<D:lookup::Storage>(driver:&D,tx:&mut Transaction,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(reuser):Ghost<u64>,Ghost(key):Ghost<usize>)
    ->(result:Result<Option<Option<usize>>,lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(*old(tx)),birth,writer),
        old(tx).write_set.len()==0,old(tx).read_set.len()==0,
        driver.image()==reused(l,birth,writer,before,after,reuser,key),
    ensures lookup::transaction_view_same(*old(tx),*final(tx)),
        result.is_ok() ==> result.unwrap()==Some(Some(before))
            && final(tx).read_set@==seq![lookup::ReadRecord{row_id:0,row_ptr:l.anchor,xmin:birth}],
{
    proof {pruned_tail_reuse_preserves_history(l,birth,writer,before,after,*tx,reuser,key);}
    lookup::read(driver,tx,0)
}

pub fn native_constructor_clears_metadata(value:Option<usize>,writer:u64,next:u32)->(cell:initialization::Cell)
    ensures cell.row.value==value,cell.row.xmin==writer,cell.row.xmax==0,cell.row.next==next,
        !cell.row.locked,cell.row.owner==0,cell.recycle_next==0,
{
    initialization::new_row(value,writer,next)
}

pub fn native_initialize_preserves_reader<D:initialization::Initialization>(driver:&mut D,row_ptr:u32,key:usize,reuser:u64,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(result:Result<(),lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(tx),birth,writer),row_ptr==l.tail,
        old(driver).state().image==pruned(l,birth,writer,before,after),
        old(driver).state().exclusive.contains(l.tail),
        old(driver).state().protected==Set::empty().insert(l.fresh).insert(l.anchor),
    ensures result.is_ok() ==> final(driver).state().image==reused(l,birth,writer,before,after,reuser,key)
        && lookup::history_maps_snapshot(final(driver).state().image,tx,initial_keys(before),Seq::empty())
        && lookup::first_visible(final(driver).state().image,l.anchor,lookup::snapshot(tx))==Some(l.anchor),
{
    proof {
        seeded_native_preconditions(l,birth,writer,before,after);
        publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
        retention::splice_valid(published(l,birth,writer,before,after),l.anchor,l.tail);
        assert(initialization::permitted(driver.state(),row_ptr,0));
    }
    let result=initialization::initialize_row(driver,row_ptr,Some(key),reuser,0);
    proof {
        if result.is_ok() {
            assert(driver.state().image==reused(l,birth,writer,before,after,reuser,key));
            pruned_tail_reuse_preserves_history(l,birth,writer,before,after,tx,reuser,key);
        }
    }
    result
}

pub fn native_validation_rejects_invisible_writer<D:lookup::Storage>(driver:&D,tx:&Transaction,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(reuser):Ghost<u64>,Ghost(key):Ghost<usize>)
    ->(result:Result<bool,lookup::Error>)
    requires distinct(l),birth>1,writer>0,old_reader(lookup::snapshot(*tx),birth,writer),
        tx.read_set@==seq![lookup::ReadRecord{row_id:0,row_ptr:l.anchor,xmin:birth}],
        driver.image()==reused(l,birth,writer,before,after,reuser,key),
    ensures result.is_ok() ==> result.unwrap(),
{
    proof {lookup::later_deletion_rejects_recorded_read(driver.image(),*tx,0);}
    lookup::has_serialization_conflict(driver,tx)
}

pub proof fn old_snapshot_cases_have_witnesses()
    ensures exists|l:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(l) && birth>1
        && old_reader(tx,birth,writer) && writer<tx.xmax && tx.active.contains(writer),
        exists|l:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(l) && birth>1
        && old_reader(tx,birth,writer) && writer>=tx.xmax,
{
    let l=Layout{tail:1,anchor:2,fresh:3};
    let active=Snapshot{txid:5,xmin:3,xmax:8,active:seq![7u64]};
    let future=Snapshot{txid:5,xmin:3,xmax:8,active:Seq::empty()};
    assert(distinct(l));
    assert(old_reader(active,2,7));
    assert(old_reader(future,2,9));
    assert(7<active.xmax && active.active.contains(7u64));
    assert(9>=future.xmax);
    assert(exists|layout:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(layout) && birth>1
        && old_reader(tx,birth,writer) && writer<tx.xmax && tx.active.contains(writer));
    assert(exists|layout:Layout,birth:u64,writer:u64,tx:Snapshot| distinct(layout) && birth>1
        && old_reader(tx,birth,writer) && writer>=tx.xmax);
}

pub fn native_vacuum_releases_anchor<D:retention::Vacuum>(driver:&mut D,horizon:u64,
    Ghost(l):Ghost<Layout>,Ghost(birth):Ghost<u64>,Ghost(writer):Ghost<u64>,
    Ghost(before):Ghost<usize>,Ghost(after):Ghost<usize>,Ghost(tx):Ghost<Transaction>)
    ->(result:Result<Vec<retention::Reclaimed>,lookup::Error>)
    requires distinct(l),1<birth<writer<horizon<=tx.snapshot_xmin,
        retention::snapshot_well_formed(lookup::snapshot(tx)),old(driver).partition_held(0),
        old(driver).state().image==pruned(l,birth,writer,before,after),
        old(driver).state().recycled==Set::empty().insert(l.tail),
    ensures result.is_ok() ==> final(driver).state().recycled.contains(l.anchor)
        && !final(driver).state().recycled.contains(l.fresh),
{
    proof {
        seeded_native_preconditions(l,birth,writer,before,after);
        publication::publication_prefix_valid(seeded(l,birth,writer,before,after),write(l),writer,3);
        retention::splice_valid(published(l,birth,writer,before,after),l.anchor,l.tail);
        reveal_with_fuel(retention::reachable,3);
        assert(retention::admissible(driver.state().image,lookup::snapshot(tx),horizon));
        assert(driver.state().recycled.disjoint(retention::reachable(driver.state().image,l.fresh)));
    }
    retention::vacuum_row_acquired(driver,0,horizon,Ghost(lookup::snapshot(tx)))
}
}
