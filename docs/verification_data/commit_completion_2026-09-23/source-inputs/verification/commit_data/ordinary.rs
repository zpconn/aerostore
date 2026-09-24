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
    /* NATIVE_ORDINARY_PUBLICATION */
}
impl OrdinaryStorage for publication::Memory {
    fn load_value(&self,p:u32)->(value:Option<usize>) {
        if p==1 {self.base.value} else {self.fresh.value}
    }
}
}
