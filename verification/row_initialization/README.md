# Source-bound recycled-row initialization

This component translates the actual `OccRow::new` field expressions and
`initialize_row` resolver/`ptr::write` sequence into the shared lookup `Image`
and `Row` vocabulary. `Cell` additionally carries native `recycle_next`, so all
seven constructor fields are checked: value, xmin, xmax, next, lock bit, lock
owner and recycler link. Successful initialization writes exactly the fresh
cell; resolution failure leaves the state unchanged. The proof also preserves
image validity and every declared protected row.

The caller must supply exclusive allocation ownership and show the destination
is outside the protected set. The proof derives preservation of those protected
rows from the single destination write; it does not assume the whole initializer
is safe. Valid next-pointer/rank constraints preserve the acyclic lookup image.
Root composition derives the protected traversal prefix and identifies a
detached recyclable tail before calling this initializer.

Raw pointer resolution, exclusive allocation ownership and the field effect of
one `ptr::write` remain primitives. There is no proof here of native allocator
ownership, address provenance, concurrent readers' complete hazard set, free-list
exclusion, or Rust's full memory model. The indexed value is the same abstract
key projection used by lookup, with the raw generic row mapping still explicit.
This is proof-only code and adds no production checks or fields.

```sh
python3 verification/row_initialization/generate.py --check
python3 -m unittest discover -s verification/row_initialization -p 'test_*.py'
```

`render_module` embeds this component in the storage composition. `ROOTS` and
`MUTATIONS` in `generate.py` declare its four proof functions and eight native
semantic controls for corrupted constructor metadata and initializer arguments.
`mutation_source` scopes each mutation to the exact native method. The parent
storage campaign checks the embedded functions and owns their proof receipts;
the component has no separate claim of complete allocator verification.
