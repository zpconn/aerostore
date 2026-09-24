use crate::{ordinary,lookup,postings,publication};
use ordinary::OrdinaryStorage;
use publication::PublicationStorage;
use vstd::prelude::*;
verus! {
broadcast use vstd::seq_lib::group_seq_properties;

pub open spec fn last_at(writes:Seq<ordinary::Write>,row:usize,index:usize,n:int)->bool {
    index<n && writes[index as int].row_id==row
    && forall|j:int| index<j<n ==> writes[j].row_id!=row
}
pub open spec fn prefix_map(writes:Seq<ordinary::Write>,n:int,map:Map<usize,usize>)->bool {
    0<=n<=writes.len()
    && (forall|row:usize| #[trigger] map.contains_key(row) <==>
        exists|i:int| 0<=i<n && writes[i].row_id==row)
    && (forall|row:usize| #[trigger] map.contains_key(row) ==> last_at(writes,row,map[row],n))
}
pub open spec fn selection_safe(tx:ordinary::Transaction,indices:Seq<usize>)->bool {
    forall|i:int| 0<=i<indices.len() ==> indices[i]<tx.write_set.len()
        && last_at(tx.write_set@,tx.write_set[indices[i] as int].row_id,indices[i],tx.write_set.len() as int)
}
pub open spec fn selected_contains(tx:ordinary::Transaction,indices:Seq<usize>,row:usize)->bool {
    exists|i:int| 0<=i<indices.len() && tx.write_set[indices[i] as int].row_id==row
}
pub open spec fn selection_complete(tx:ordinary::Transaction,indices:Seq<usize>)->bool {
    forall|j:int| #![trigger tx.write_set[j]] 0<=j<tx.write_set.len() ==>
        selected_contains(tx,indices,tx.write_set[j].row_id)
}
pub open spec fn selection_ordered(tx:ordinary::Transaction,indices:Seq<usize>)->bool {
    forall|i:int,j:int| 0<=i<j<indices.len() ==>
        tx.write_set[indices[i] as int].row_id<tx.write_set[indices[j] as int].row_id
}
pub open spec fn selected(tx:ordinary::Transaction,indices:Seq<usize>)->bool {
    selection_safe(tx,indices) && selection_complete(tx,indices) && selection_ordered(tx,indices)
}

// Standard ordered-map operations are the explicit library boundary. The
// final-write property is derived below from insert-overwrite and ordered
// enumeration, rather than being postulated for final_write_indices.
pub open spec fn enumerates(map:Map<usize,usize>,keys:Seq<usize>,values:Seq<usize>)->bool {
    keys.len()==values.len() && keys.to_set()==map.dom()
    && (forall|i:int| 0<=i<keys.len() ==> values[i]==map[keys[i]])
    && (forall|i:int,j:int| 0<=i<j<keys.len() ==> keys[i]<keys[j])
}
pub trait RowMapPrimitives:Sized {
    spec fn map(&self)->Map<usize,usize>;
    fn new()->(map:Self) ensures map.map()==Map::<usize,usize>::empty();
    fn insert(&mut self,row:usize,index:usize)
        ensures final(self).map()==old(self).map().insert(row,index);
    fn into_values(self)->(values:Vec<usize>)
        ensures exists|keys:Seq<usize>| enumerates(self.map(),keys,values@);
}
pub proof fn prefix_insert(writes:Seq<ordinary::Write>,n:int,map:Map<usize,usize>)
    requires prefix_map(writes,n,map),0<=n<writes.len(),n<=usize::MAX,
    ensures prefix_map(writes,n+1,map.insert(writes[n].row_id,n as usize)),
{
    let after=map.insert(writes[n].row_id,n as usize);
    assert forall|row:usize| #[trigger] after.contains_key(row) <==>
        exists|i:int| 0<=i<n+1 && writes[i].row_id==row by {
        if row==writes[n].row_id {assert(writes[n].row_id==row);}
        else if exists|i:int| 0<=i<n+1 && writes[i].row_id==row {
            let i=choose|i:int| 0<=i<n+1 && writes[i].row_id==row;
            assert(i<n);
        }
    }
    assert forall|row:usize| #[trigger] after.contains_key(row) implies last_at(writes,row,after[row],n+1) by {
        if row!=writes[n].row_id {
            assert(last_at(writes,row,map[row],n));
        }
    }
}
pub proof fn map_yields_selection(tx:ordinary::Transaction,map:Map<usize,usize>,values:Seq<usize>)
    requires prefix_map(tx.write_set@,tx.write_set.len() as int,map),
        exists|keys:Seq<usize>| enumerates(map,keys,values),
    ensures selected(tx,values),values.no_duplicates(),
{
    reveal(selection_complete);
    let keys=choose|keys:Seq<usize>| enumerates(map,keys,values);
    assert forall|i:int| 0<=i<values.len() implies values[i]<tx.write_set.len()
        && last_at(tx.write_set@,tx.write_set[values[i] as int].row_id,values[i],tx.write_set.len() as int) by {
        assert(keys.to_set().contains(keys[i]));
        assert(map.contains_key(keys[i]));
    }
    assert(selection_safe(tx,values));
    assert(selection_complete(tx,values)) by {
    assert forall|j:int| #![trigger tx.write_set[j]] 0<=j<tx.write_set.len() implies selected_contains(tx,values,tx.write_set[j].row_id) by {
        let row=tx.write_set[j].row_id;
        assert(map.contains_key(row));
        let i=choose|i:int| 0<=i<keys.len() && keys[i]==row;
        assert(tx.write_set[values[i] as int].row_id==row);
    }
    }
    assert forall|i:int,j:int| 0<=i<j<values.len() implies
        tx.write_set[values[i] as int].row_id<tx.write_set[values[j] as int].row_id by {
        assert(map.contains_key(keys[i]));assert(map.contains_key(keys[j]));
    }
    assert(selection_ordered(tx,values));
    reveal(selected);
    assert(selected(tx,values));
    assert forall|i:int,j:int| 0<=i<j<values.len() implies values[i]!=values[j] by {}
}
pub fn final_write_indices<M:RowMapPrimitives>(tx:&ordinary::Transaction)->(indices:Vec<usize>)
    ensures selected(*tx,indices@),indices@.no_duplicates(),
{
    /* NATIVE_FINAL_WRITES */
}
pub proof fn selected_one_row(tx:ordinary::Transaction,indices:Seq<usize>)
    requires selected(tx,indices),tx.write_set.len()>0,
        forall|i:int| 0<=i<tx.write_set.len() ==> #[trigger] tx.write_set[i].row_id==tx.write_set[0].row_id,
    ensures indices.len()==1,indices[0]==tx.write_set.len()-1,
{
    assert(tx.write_set[0].row_id==tx.write_set[0].row_id);
    let i=choose|i:int| 0<=i<indices.len()
        && tx.write_set[indices[i] as int].row_id==tx.write_set[0].row_id;
    if indices.len()>1 {
        assert(tx.write_set[indices[0] as int].row_id==tx.write_set[0].row_id);
        assert(tx.write_set[indices[1] as int].row_id==tx.write_set[0].row_id);
        assert(tx.write_set[indices[0] as int].row_id<tx.write_set[indices[1] as int].row_id);
    }
    if indices[0]<tx.write_set.len()-1 {
        let last=(tx.write_set.len()-1) as int;
        assert(tx.write_set[last].row_id==tx.write_set[0].row_id);
        assert(last_at(tx.write_set@,tx.write_set[indices[0] as int].row_id,indices[0],tx.write_set.len() as int));
    }
}

pub open spec fn admissible(image:lookup::Image,tx:ordinary::Transaction,indices:Seq<usize>)->bool {
    indices.len()==1 && indices[0]<tx.write_set.len()
    && image.rows.contains_key(tx.write_set[indices[0] as int].base_offset)
    && image.rows.contains_key(tx.write_set[indices[0] as int].new_offset)
}
pub open spec fn expected_change(image:lookup::Image,tx:ordinary::Transaction,index:usize)->postings::IndexChange {
    let w=tx.write_set[index as int];
    postings::IndexChange{binding:0,row_id:w.row_id,
        before:image.rows[w.base_offset].value,after:image.rows[w.new_offset].value}
}
pub open spec fn extracted(image:lookup::Image,tx:ordinary::Transaction,indices:Seq<usize>,changes:Seq<postings::IndexChange>)->bool {
    admissible(image,tx,indices)
    && image.rows.contains_key(tx.write_set[indices[0] as int].base_offset)
    && image.rows.contains_key(tx.write_set[indices[0] as int].new_offset)
    && {let change=expected_change(image,tx,indices[0]);
        changes==if change.before==change.after {Seq::empty()} else {seq![change]}}
}
pub trait PlanningPrimitives:OrdinaryStorage {
    spec fn bindings(&self)->usize;
    spec fn encodable(&self,binding:usize,key:usize,row:usize)->bool;
    spec fn infallible_keys(&self)->bool;
    fn binding_count(&self)->(n:usize) ensures n==self.bindings();
    fn key(&self,binding:usize,value:&Option<usize>)->(key:Option<usize>)
        requires binding<self.bindings(),ensures key==*value;
    fn prevalidate(&self,binding:usize,key:&usize,row:&usize)->(result:Result<(),lookup::Error>)
        requires binding<self.bindings(),
        ensures result.is_ok() ==> self.encodable(binding,*key,*row),
            self.infallible_keys() ==> result.is_ok();
}
pub open spec fn checked<D:PlanningPrimitives>(driver:D,changes:Seq<postings::IndexChange>)->bool {
    forall|i:int| 0<=i<changes.len() ==>
        (changes[i].before.is_some() ==> driver.encodable(changes[i].binding,changes[i].before.unwrap(),changes[i].row_id))
        && (changes[i].after.is_some() ==> driver.encodable(changes[i].binding,changes[i].after.unwrap(),changes[i].row_id))
}
pub fn index_changes<D:PlanningPrimitives>(driver:&D,tx:&ordinary::Transaction,indices:&Vec<usize>)
    ->(result:Result<Vec<postings::IndexChange>,lookup::Error>)
    requires admissible(driver.image(),*tx,indices@),driver.bindings()==1,
    ensures result.is_ok() ==> extracted(driver.image(),*tx,indices@,result.unwrap()@),
        result.is_ok() ==> checked(*driver,result.unwrap()@),
        driver.infallible() && driver.infallible_keys() ==> result.is_ok(),
{
    /* NATIVE_INDEX_CHANGES */
}
pub fn repeated_write_selection_is_live()->(tx:ordinary::Transaction)
    ensures tx.write_set.len()==3 && tx.write_set[0].row_id==tx.write_set[2].row_id
        && selected(tx,seq![2usize,1usize]),
{
    let mut writes=Vec::new();
    writes.push(ordinary::Write{row_id:3,base_offset:1,new_offset:2,dirty_columns_bitmask:0});
    writes.push(ordinary::Write{row_id:7,base_offset:3,new_offset:4,dirty_columns_bitmask:0});
    writes.push(ordinary::Write{row_id:3,base_offset:1,new_offset:5,dirty_columns_bitmask:0});
    ordinary::Transaction{txid:8,write_set:writes}
}
impl PlanningPrimitives for publication::Memory {
    open spec fn bindings(&self)->usize {1}
    open spec fn encodable(&self,binding:usize,key:usize,row:usize)->bool {true}
    open spec fn infallible_keys(&self)->bool {true}
    fn binding_count(&self)->(n:usize) {1}
    fn key(&self,binding:usize,value:&Option<usize>)->(key:Option<usize>) {*value}
    fn prevalidate(&self,binding:usize,key:&usize,row:&usize)->(result:Result<(),lookup::Error>) {Ok(())}
}
pub fn key_change_has_live_execution(before_key:Option<usize>,after_key:Option<usize>)->(result:bool)
    ensures result,
{
    let driver=publication::Memory{head:1,
        base:lookup::Row{xmin:1,xmax:0,next:0,value:before_key,locked:false,owner:0},
        fresh:lookup::Row{xmin:9,xmax:0,next:1,value:after_key,locked:false,owner:0}};
    let mut writes=Vec::new();
    writes.push(ordinary::Write{row_id:0,base_offset:1,new_offset:2,dirty_columns_bitmask:0});
    let tx=ordinary::Transaction{txid:9,write_set:writes};
    let mut indices=Vec::new();indices.push(0);
    match index_changes(&driver,&tx,&indices) {
        Err(_)=>false,
        Ok(changes)=>if before_key==after_key {changes.len()==0} else {
            changes.len()==1 && changes[0].binding==0 && changes[0].row_id==0
                && changes[0].before==before_key && changes[0].after==after_key
        },
    }
}
}
