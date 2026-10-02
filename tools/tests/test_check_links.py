from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import check_links


class CheckLinksTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "-q")

    def git(self, *args):
        return subprocess.check_output(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", *args],
                                       cwd=self.root, text=True, stderr=subprocess.PIPE)

    def write(self, name, content, tracked=True):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        if tracked: self.git("add", "--", name)

    def test_index_content_is_authority_ignored_files_do_not_mask_missing_links(self):
        self.write("README.md", "[evidence](target/receipt.json)\n")
        self.write(".gitignore", "/target/\n")
        self.write("target/receipt.json", "{}", tracked=False)
        result = check_links.check(self.root)
        self.assertFalse(result["passed"])
        self.assertIn("absent from tracked tree", result["errors"][0]["error"])
        self.write("README.md", "fixed only in worktree", tracked=False)
        self.assertFalse(check_links.check(self.root)["passed"])

    def test_fences_and_inline_code_are_not_links(self):
        self.write("README.md", "```md\n[bad](missing.md)\n```\n~~~\n![bad](missing.png)\n~~~\n`[bad](missing.md)`\n")
        self.assertTrue(check_links.check(self.root)["passed"])

    def test_relative_links_images_reference_links_and_titles(self):
        self.write("README.md", '[nested](docs/guide.md "Guide")\n[ref][guide]\n[guide][]\n[guide]\n[guide]: docs/guide.md\n![image](<docs/a b.png>)\n')
        self.write("docs/guide.md", "[home](../README.md)\n[dir](../docs/)\n")
        self.write("docs/a b.png", "image")
        self.assertTrue(check_links.check(self.root)["passed"])

    def test_heading_duplicates_unicode_inline_markup_and_html_anchors(self):
        self.write("README.md", "# Header `code`!\n# Header `code`!\n## Café *notes*\n<a id='custom'></a>\n"
                   "[one](#header-code) [two](#header-code-1) [unicode](#caf%C3%A9-notes) [html](#custom)\n"
                   "Setext heading\n---\n[setext](#setext-heading)\n")
        self.assertTrue(check_links.check(self.root)["passed"])

    def test_missing_anchor_and_line_range_are_reported(self):
        self.write("README.md", "[missing](#nope)\n[bad line](code.py#L3)\n")
        self.write("code.py", "one\ntwo\n")
        result = check_links.check(self.root)
        self.assertEqual(len(result["errors"]), 2)
        self.write("README.md", "[line](code.py#L1-L2)\n")
        self.assertTrue(check_links.check(self.root)["passed"])

    def test_external_urls_are_never_fetched_and_absolute_local_fails(self):
        self.write("README.md", "[external](https://example.invalid/nope#anchor) [mail](mailto:test@example.invalid)\n"
                   "[local](/home/user/private.log)\n")
        result = check_links.check(self.root)
        self.assertEqual(result["external_links_not_fetched"], 2)
        self.assertEqual(len(result["errors"]), 1)

    def test_parent_escape_and_symlink_target_fail(self):
        self.write("README.md", "[escape](../outside)\n[link](alias.md)\n")
        (self.root / "alias.md").symlink_to("../outside")
        self.git("add", "alias.md")
        result = check_links.check(self.root)
        self.assertFalse(result["passed"])
        self.assertTrue(any("escapes" in item["error"] for item in result["errors"]))
        self.assertTrue(any("symlink" in item["error"] for item in result["errors"]))

    def test_tree_ref_is_independent_of_index_changes(self):
        self.write("README.md", "# Valid\n[valid](#valid)\n")
        self.git("commit", "-qm", "good")
        self.write("README.md", "[broken](missing)\n")
        self.assertTrue(check_links.check(self.root, "HEAD")["passed"])
        self.assertFalse(check_links.check(self.root)["passed"])

    def test_html_links_and_escaped_balanced_destinations(self):
        self.write("README.md", '<a href="docs/file(1).md#hello">hello</a>\n[file](docs/file(1).md)\n')
        self.write("docs/file(1).md", "# Hello\n")
        self.assertTrue(check_links.check(self.root)["passed"])

    def test_undefined_reference_fails(self):
        self.write("README.md", "[label][absent]\n")
        result = check_links.check(self.root)
        self.assertFalse(result["passed"])
        self.assertIn("undefined reference", result["errors"][0]["error"])


if __name__ == "__main__":
    unittest.main()
