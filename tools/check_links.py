#!/usr/bin/env python3
"""Check local Markdown links against the Git index, or an explicit Git tree.

Ignored/untracked working files cannot satisfy a link. External URLs are counted
but never fetched. Fenced/inline code is ignored; headings, HTML IDs, reference
links, images, and source-line anchors are supported without third-party modules.
"""
from __future__ import annotations

import argparse
import html
from html.parser import HTMLParser
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import unicodedata
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
MAX_DOCUMENT = 4 * 1024**2


def without_fences(text):
    result, fence = [], None
    for line in text.splitlines():
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if fence:
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence) and not marker[2].strip():
                fence = None
            result.append("")
        elif marker:
            fence = marker[1]
            result.append("")
        else:
            result.append(line)
    return "\n".join(result)


def without_inline_code(text):
    return re.sub(r"(`+)(.+?)\1(?!`)", lambda match: " " * len(match[0]), text, flags=re.S)


def reference_id(value):
    return " ".join(value.split()).casefold()


def unescape(value):
    return html.unescape(re.sub(r"\\([!\"#$%&'()*+,\-./:;<=>?@\[\]\\^_`{|}~ ])", r"\1", value))


def destination(text, start):
    """Read a CommonMark-style destination, leaving optional titles aside."""
    position = start
    while position < len(text) and text[position].isspace():
        position += 1
    if position < len(text) and text[position] == "<":
        end = position + 1
        while end < len(text):
            if text[end] == ">" and text[end - 1] != "\\":
                return unescape(text[position + 1:end])
            end += 1
        return None
    begin, depth = position, 0
    while position < len(text):
        char = text[position]
        if char == "\\" and position + 1 < len(text):
            position += 2
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            if not depth:
                break
            depth -= 1
        elif char.isspace() and not depth:
            break
        position += 1
    return unescape(text[begin:position])


class Tags(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links, self.ids = [], set()

    def handle_starttag(self, tag, attributes):
        values = dict(attributes)
        for key in ("id", "name" if tag == "a" else "id"):
            if values.get(key):
                self.ids.add(values[key])
        key = "href" if tag == "a" else "src" if tag in {"img", "source"} else None
        if key and values.get(key):
            self.links.append((self.getpos()[0], values[key]))

    handle_startendtag = handle_starttag


def parse_document(text):
    visible = without_fences(text)
    prose = without_inline_code(visible)
    tags = Tags()
    tags.feed(prose)
    references, definitions = {}, []
    for match in re.finditer(r"(?m)^ {0,3}\[([^\]\n]+)\]:[ \t]*(.*)$", prose):
        references.setdefault(reference_id(match[1]), destination(match[2], 0))
        definitions.append((match.start(), match.end()))
    scan = list(prose)
    for begin, end in definitions:
        scan[begin:end] = " " * (end - begin)
    scan = "".join(scan)
    links = list(tags.links)
    for match in re.finditer(r"(?<!!)\[([^\]\n]*)\]|!\[([^\]\n]*)\]", scan):
        label = match[1] if match[1] is not None else match[2]
        end = match.end()
        line = prose.count("\n", 0, match.start()) + 1
        if end < len(scan) and scan[end] == "(":
            target = destination(scan, end + 1)
            if target is not None:
                links.append((line, target))
        elif end < len(scan) and scan[end] == "[":
            close = scan.find("]", end + 1)
            if close >= 0:
                key = reference_id(scan[end + 1:close] or label)
                links.append((line, references.get(key, "missing-reference:" + key)))
        elif reference_id(label) in references:
            # The second bracket in [text][reference] is not another link.
            if match.start() == 0 or scan[match.start() - 1] != "]":
                links.append((line, references[reference_id(label)]))
    # Validate defined destinations too, including definitions used elsewhere in
    # renderer-specific Markdown; unused broken definitions are still defects.
    for key, target in references.items():
        if target is not None:
            links.append((1, target))
    anchors = set(tags.ids)
    lines = visible.splitlines()
    for index, line in enumerate(lines):
        atx = re.match(r"^ {0,3}#{1,6}(?:[ \t]+(.*)|$)", line)
        heading = atx[1] if atx else None
        if heading is not None:
            heading = re.sub(r"[ \t]+#+[ \t]*$", "", heading)
        elif index + 1 < len(lines) and line.strip() and re.fullmatch(r" {0,3}(?:=+|-+)[ \t]*", lines[index + 1]):
            heading = line.strip()
        if heading is None:
            continue
        heading = re.sub(r"!?\[([^\]]+)\]\([^)]*\)", r"\1", heading)
        heading = html.unescape(re.sub(r"<[^>]*>", "", heading)).lower()
        slug = "".join(char for char in heading if char in "-_ " or not unicodedata.category(char).startswith(("P", "S", "C"))).replace(" ", "-")
        candidate, suffix = slug, 0
        while candidate in anchors:
            suffix += 1
            candidate = slug + "-" + str(suffix)
        anchors.add(candidate)
    return links, anchors


class TrackedTree:
    def __init__(self, root, ref=None):
        self.root, self.entries, self.cache = root, {}, {}
        command = ["git", "--no-replace-objects"]
        args = ["ls-tree", "-r", "-z", "--full-tree", ref] if ref else ["ls-files", "--stage", "-z"]
        raw = subprocess.check_output(command + args, cwd=root)
        for row in raw.split(b"\0"):
            if not row:
                continue
            metadata, name = row.split(b"\t", 1)
            mode, second, third = metadata.split()
            if ref:
                oid = third
            else:
                if third != b"0":
                    raise ValueError("unmerged index entries cannot be link-checked")
                oid = second
            self.entries[name.decode()] = (mode, oid.decode())
        self.directories = {str(parent) for name in self.entries for parent in PurePosixPath(name).parents}

    def read(self, name):
        if name not in self.cache:
            mode, oid = self.entries[name]
            if mode not in {b"100644", b"100755"}:
                raise ValueError("link target is a symlink or non-file")
            command = ["git", "--no-replace-objects", "cat-file"]
            size = int(subprocess.check_output(command + ["-s", oid], cwd=self.root))
            if size > MAX_DOCUMENT:
                raise ValueError("Markdown/anchor target exceeds 4 MiB limit")
            self.cache[name] = subprocess.check_output(command + ["blob", oid], cwd=self.root)
        return self.cache[name]


def resolve(source, target):
    if target.startswith("missing-reference:"):
        raise ValueError("undefined reference link: " + target.split(":", 1)[1])
    parts = urlsplit(target)
    if parts.scheme or parts.netloc:
        if parts.scheme == "file":
            raise ValueError("file:// links are not public repository links")
        return None
    if re.search(r"%(?![0-9a-fA-F]{2})", parts.path + parts.fragment):
        raise ValueError("malformed percent escape")
    path, anchor = unquote(parts.path, errors="strict"), unquote(parts.fragment, errors="strict")
    if "\\" in path or "\0" in path:
        raise ValueError("non-portable local path")
    if not path:
        return source, anchor
    components = [] if path.startswith("/") else list(PurePosixPath(source).parent.parts)
    for part in path.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not components:
                raise ValueError("local link escapes repository")
            components.pop()
        else:
            components.append(part)
    return "/".join(components) or ".", anchor


def check(root, ref=None):
    tree = TrackedTree(root, ref)
    errors, checked, external = [], 0, 0
    documents = sorted(name for name in tree.entries if PurePosixPath(name).suffix.lower() in {".md", ".markdown"})
    parsed = {}
    def document(name):
        if name not in parsed:
            parsed[name] = parse_document(tree.read(name).decode("utf-8"))
        return parsed[name]
    for name in documents:
        try:
            links, _ = document(name)
        except (ValueError, UnicodeError) as error:
            errors.append({"file": name, "line": 1, "error": str(error)})
            continue
        for line, target in links:
            try:
                resolved = resolve(name, target)
                if resolved is None:
                    external += 1
                    continue
                checked += 1
                path, anchor = resolved
                if path not in tree.entries:
                    if path not in tree.directories:
                        raise ValueError("target is absent from tracked tree: " + path)
                    if anchor:
                        candidates = [candidate for candidate in tree.entries if PurePosixPath(candidate).parent.as_posix() == path
                                      and PurePosixPath(candidate).name.lower() == "readme.md"]
                        if len(candidates) != 1:
                            raise ValueError("directory anchor has no unique tracked README")
                        path = candidates[0]
                elif tree.entries[path][0] not in {b"100644", b"100755"}:
                    raise ValueError("link target is a symlink or non-file")
                if anchor:
                    line_anchor = re.fullmatch(r"L([1-9][0-9]*)(?:-L([1-9][0-9]*))?", anchor)
                    if line_anchor:
                        start, end = int(line_anchor[1]), int(line_anchor[2] or line_anchor[1])
                        if not 1 <= start <= end <= len(tree.read(path).splitlines()):
                            raise ValueError("source-line anchor is outside the tracked file")
                    elif PurePosixPath(path).suffix.lower() in {".md", ".markdown"}:
                        if anchor not in document(path)[1]:
                            raise ValueError("heading/HTML anchor is absent: " + anchor)
                    else:
                        raise ValueError("unsupported anchor on non-Markdown target: " + anchor)
            except (ValueError, UnicodeError) as error:
                errors.append({"file": name, "line": line, "target": target, "error": str(error)})
    return {"passed": not errors, "tree": ref or "index", "markdown_files": len(documents),
            "local_links_checked": checked, "external_links_not_fetched": external, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--ref", help="check this committed tree instead of the index")
    args = parser.parse_args()
    result = check(args.root.resolve(), args.ref)
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
