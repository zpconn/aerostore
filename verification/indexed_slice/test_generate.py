#!/usr/bin/env python3
import unittest
from unittest.mock import patch
import generate


class IndexedSliceAdapterTests(unittest.TestCase):
    def test_current_embedded_modules(self):
        result=generate.render()
        self.assertEqual(generate.OUTPUT.read_text(), result)
        for name in generate.COMPONENTS:
            self.assertEqual(result.count(generate.component(name).OUTPUT.read_text()), 1)

    def test_native_guard_drop_before_raw_rejected(self):
        source=generate.SOURCE.read_text()
        source=source.replace("let candidates = index.transactional_raw_lookup(predicate)?;",
            "drop(guards); let candidates = index.transactional_raw_lookup(predicate)?;",1)
        with self.assertRaisesRegex(ValueError,"raw lookup"):
            generate.render(source=source)

    def test_native_capture_bypass_rejected(self):
        source=generate.SOURCE.read_text()
        start=source.index("    pub fn index_lookup(")
        first=source.index("        for bucket in &buckets {",start)
        second=source.index("        for bucket in &buckets {",first+1)
        source=source[:second]+source[second:].replace("for bucket in &buckets {","if false { for bucket in &buckets {",1)
        with self.assertRaises(ValueError):
            generate.render(source=source)

    def test_missing_or_duplicate_component_rejected(self):
        template=generate.TEMPLATE.read_text()
        for replacement in ("", "/* CAPTURE_MODULE */ /* CAPTURE_MODULE */"):
            with self.assertRaisesRegex(ValueError,"missing/duplicate component"):
                generate.render(template=template.replace("/* CAPTURE_MODULE */",replacement))

    def test_stale_component_rejected(self):
        original=generate.component
        def stale(name):
            module=original(name)
            if name=="lookup":
                module.render=lambda _source: "stale"
            return module
        with patch.object(generate,"component",stale):
            with self.assertRaisesRegex(ValueError,"stale indexed-slice component"):
                generate.render()

    def test_owned_borrow_precedes_consumption_and_materialization(self):
        template=generate.TEMPLATE.read_text()
        stages=["ownership::acquire_index_bucket(driver, binding, bucket, &guards",
            "capture::capture_dependencies(&index, &mut captured, &buckets)",
            "driver.raw_lookup(query, &guards[0], Tracked(&*authority))",
            "ownership::release_all(driver, guards, Tracked(&mut *authority))",
            "lookup::materialize_after_checked_history::<D, C, P>",
            "ownership::acquire_index_bucket(driver, binding, bucket, &validation_guards",
            "predicate::index_read_conflict(&index, &validation_tx)",
            "lookup::has_serialization_conflict(validation_driver, tx)",
            "ownership::release_all(driver, validation_guards, Tracked(&mut *authority))"]
        positions=[template.index(stage) for stage in stages]
        self.assertEqual(positions,sorted(positions))
        self.assertNotIn("assume(",template)
        self.assertNotIn("external_body",template)


if __name__=="__main__":
    unittest.main()
