#!/usr/bin/env python3
"""Adversarial change-detection checks for the source-reviewed P0 inventory."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from check_p0_contracts import ROOT, INVENTORY, functions, validate


class P0ContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="aerostore-p0-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inventory = json.loads((ROOT / INVENTORY).read_text())
        paths = set(self.inventory["modules"]) | {INVENTORY}
        paths.update(ref for contract in self.inventory["contracts"].values()
                     for ref in contract["references"])
        for name in paths:
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)

    def write_inventory(self):
        (self.root / INVENTORY).write_text(json.dumps(self.inventory))

    def mutate_source(self, name, before, after):
        path = self.root / "aerostore_core/src" / name
        source = path.read_text()
        self.assertIn(before, source)
        path.write_text(source.replace(before, after, 1))

    def rejects(self, expected):
        result = validate(self.root)
        self.assertFalse(result["passed"], result)
        self.assertTrue(any(expected in error for error in result["errors"]), result)

    def test_current_inventory_is_consistent(self):
        result = validate(self.root)
        self.assertTrue(result["passed"], result)
        self.assertGreater(result["checked_public_apis"], 400)

    def test_new_public_api_requires_contract(self):
        with (self.root / "aerostore_core/src/vacuum.rs").open("a") as out:
            out.write("\npub fn unchecked_vacuum_horizon() -> u64 { u64::MAX }\n")
        self.rejects("public API coverage changed")

    def test_deleted_api_entry_is_not_coverage(self):
        self.inventory["apis"].pop()
        self.write_inventory()
        self.rejects("public API coverage changed")

    def test_duplicate_api_entry_fails(self):
        self.inventory["apis"].append(copy.deepcopy(self.inventory["apis"][0]))
        self.write_inventory()
        self.rejects("duplicate API")

    def test_signature_change_fails(self):
        self.mutate_source("occ_partitioned.rs", "pub fn capacity(&self) -> usize", "pub fn capacity(&self) -> u64")
        self.rejects("API signature/body changed: occ_partitioned::OccTable::capacity")

    def test_public_horizon_behavior_change_fails(self):
        self.mutate_source("occ_partitioned.rs", "self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))", "self.vacuum_reclaim_before(requested_xmin)")
        self.rejects("API signature/body changed: occ_partitioned::OccTable::vacuum_reclaim_once")

    def test_private_lock_helper_change_requires_review(self):
        self.mutate_source("occ_partitioned.rs", "lock_indices.sort_unstable();", "lock_indices.reverse();")
        self.rejects("source changed: aerostore_core/src/occ_partitioned.rs")

    def test_new_source_module_is_not_silently_excluded(self):
        (self.root / "aerostore_core/src/new_transactions.rs").write_text("pub fn commit() {}")
        self.rejects("core source module coverage changed")

    def test_legacy_cannot_be_promoted_by_an_api_row(self):
        entry = next(e for e in self.inventory["apis"] if e["source"].endswith("occ_legacy.rs"))
        entry["classification"] = "covered"
        entry["contract"] = "commit"
        self.write_inventory()
        self.rejects("API/module scope mismatch")

    def test_missing_error_contract_fails(self):
        self.inventory["contracts"]["commit"]["failure"] = ""
        self.write_inventory()
        self.rejects("missing failure contract: commit")

    def test_unknown_contract_fails(self):
        self.inventory["apis"][0]["contract"] = "proof-by-wish"
        self.write_inventory()
        self.rejects("unknown API contract")

    def test_graph_cannot_reference_missing_function(self):
        self.inventory["lock_edges"][0]["references"] = ["occ_partitioned::OccTable::imagined_lock"]
        self.write_inventory()
        self.rejects("missing lock source function")

    def test_graph_cannot_reference_missing_lock(self):
        self.inventory["lock_edges"][0]["to"] = "imagined_lock"
        self.write_inventory()
        self.rejects("unknown lock node")

    def test_removing_lock_edge_leaves_uncovered_path(self):
        self.inventory["lock_edges"].pop()
        self.write_inventory()
        self.rejects("missing lock edge in path")

    def test_released_horizon_lock_is_not_nested(self):
        # Treating the public horizon scan as held over vacuum would introduce
        # the inverse of the real commit/checkpoint partition -> lifecycle edge.
        edge = next(e for e in self.inventory["lock_edges"] if e["id"] == "lifecycle_before_partition")
        edge["kind"] = "nested"
        self.write_inventory()
        self.rejects("cycle in declared blocking lock graph")

    def test_completion_claim_cannot_hide_declared_semantic_gap(self):
        self.inventory["p0_complete"] = True
        self.inventory["audit_gaps"].append(dict(id="undefined_failure", blocks_p0=True))
        self.write_inventory()
        self.rejects("P0 completion flag disagrees")

    def test_require_complete_refuses_review_or_semantic_gaps(self):
        self.inventory["p0_complete"] = False
        self.inventory["audit_gaps"] = [dict(id="pending_review", blocks_p0=True)]
        self.write_inventory()
        self.assertTrue(validate(self.root)["passed"])
        result = validate(self.root, require_complete=True)
        self.assertFalse(result["passed"])
        self.assertIn("P0 exit audit remains open", result["errors"])

    def test_nonfunction_surface_is_required(self):
        self.inventory["nonfunction_surface"]["public_data"] = ""
        self.write_inventory()
        self.rejects("missing nonfunction public-surface contract")

    def test_scanner_handles_comments_literals_lifetimes_and_trait_methods(self):
        source = '''
            // pub fn fake() {}
            /* outer /* pub fn fake2() {} */ comment */
            pub trait Field { fn field<'a>(&'a self) -> &'a str; }
            pub struct Table;
            impl Table {
                pub fn read(&self) -> &'static str { r###"pub fn fake3() { }"### }
                pub(crate) fn internal() {}
            }
            #[cfg(test)] mod tests { pub fn only_test() {} }
        '''
        apis = [f["id"] for f in functions(source, "example") if f["public"]]
        self.assertEqual(apis, ["example::Field::field", "example::Table::read"])

    def test_array_semicolons_and_const_braces_do_not_end_signatures(self):
        source = '''
            pub trait Bytes {
                fn copy(&self, out: &mut [u8; 32]) -> [u64; 4];
            }
            pub fn read<const N: usize>(out: &mut [u8; N]) -> [u64; 4] {
                [1; 4]
            }
            pub fn take(value: Array<{ 3 + 1 }>) -> [u8; 4] { [0; 4] }
        '''
        apis = [f for f in functions(source, "example") if f["public"]]
        self.assertEqual(apis[0]["signature"], "fn copy(&self, out: &mut [u8; 32]) -> [u64; 4]")
        self.assertEqual(apis[1]["signature"], "pub fn read<const N: usize>(out: &mut [u8; N]) -> [u64; 4]")
        self.assertEqual(apis[2]["signature"], "pub fn take(value: Array<{ 3 + 1 }>) -> [u8; 4]")
        changed = functions(source.replace("[1; 4]", "[2; 4]"), "example")
        self.assertNotEqual(apis[1]["body_sha256"], changed[1]["body_sha256"])


if __name__ == "__main__":
    unittest.main()
