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
    (forall|id: usize| id < s.capacity ==> s.heads.contains_key(id)
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
        Action::Publish{writer,stamp} => a.active.contains(writer) && !a.published.contains_key(writer)
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
