#!/usr/bin/env python3
"""Check the reviewed P0 API/lock inventory against its exact source inputs.

This is coverage/change detection, not Rust semantic analysis or a proof that a
documented precondition is enforced. There is deliberately no refresh command.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = "verification/contracts/p0_inventory.json"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def mask_noncode(source: str) -> str:
    """Preserve positions while masking Rust comments and string/char literals.

    This narrow lexer is intentionally not a Rust parser. Source fingerprints
    require review even for syntax the inventory scanner does not understand.
    Lifetimes remain code; nested block comments and raw strings are supported.
    """
    out = list(source)
    i = 0
    while i < len(source):
        start = i
        if source.startswith("//", i):
            end = source.find("\n", i)
            i = len(source) if end == -1 else end
        elif source.startswith("/*", i):
            i += 2
            depth = 1
            while i < len(source) and depth:
                if source.startswith("/*", i):
                    depth += 1
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise ValueError("unclosed block comment")
        elif (match := re.match(r'(?:br|r)(#*)"', source[i:])):
            closing = '"' + match[1]
            end = source.find(closing, i + len(match[0]))
            if end < 0:
                raise ValueError("unclosed raw string")
            i = end + len(closing)
        elif source[i] == '"' or (source[i] == "'" and re.match(r"'(?:\\.|[^'\\\n])'", source[i:])):
            quote = source[i]
            i += 1
            while i < len(source):
                if source[i] == "\\":
                    i += 2
                elif source[i] == quote:
                    i += 1
                    break
                else:
                    i += 1
        else:
            i += 1
            continue
        for p in range(start, i):
            if out[p] != "\n":
                out[p] = " "
    return "".join(out)


def closing(code: str, start: int, left: str = "{", right: str = "}") -> int:
    depth = 0
    for i in range(start, len(code)):
        if code[i] == left:
            depth += 1
        elif code[i] == right:
            depth -= 1
            if depth == 0:
                return i
    raise ValueError(f"unclosed {left} at {start}")


def signature_end(code: str, start: int) -> int:
    """Find a body/trait terminator outside parameter and type delimiters."""
    parens = brackets = angles = 0
    i = start
    while i < len(code):
        char = code[i]
        if char == "(":
            parens += 1
        elif char == ")":
            parens -= 1
        elif char == "[":
            brackets += 1
        elif char == "]":
            brackets -= 1
        elif char == "<":
            angles += 1
        elif char == ">" and (i == 0 or code[i - 1] != "-"):
            angles = max(0, angles - 1)
        elif char == "{" and (parens or brackets or angles):
            # A braced const expression can occur inside a type argument.
            i = closing(code, i)
        elif char in "{;" and not (parens or brackets or angles):
            return i
        if parens < 0 or brackets < 0:
            raise ValueError("unbalanced function signature")
        i += 1
    raise ValueError("missing function signature terminator")


def functions(source: str, module: str) -> list[dict]:
    code = mask_noncode(source)
    # Unit-test modules are not exported production APIs. Other cfg branches are
    # conservatively included; restricted visibility is retained for graph refs.
    for match in reversed(list(re.finditer(r'#\[cfg\(test\)\]\s*(?:pub\s+)?mod\s+\w+\s*\{', code))):
        end = closing(code, code.index("{", match.start()))
        code = code[:match.start()] + " " * (end + 1 - match.start()) + code[end + 1:]
    owners = []
    for match in re.finditer(r'\b(impl|trait)\b[^{};]*\{', code):
        head = code[match.start():match.end() - 1].strip()
        kind = match[1]
        rest = head[len(kind):].strip()
        if rest.startswith("<"):
            rest = rest[closing(rest, 0, "<", ">") + 1:].strip()
        if " for " in rest:
            rest = rest.split(" for ", 1)[1]
        rest = re.sub(r"^&(?:'\w+\s+)?(?:mut\s+)?", "", rest).lstrip("$")
        owner = re.match(r'(\w+)', rest)
        if not owner:
            raise ValueError(f"unsupported owner declaration: {head}")
        owners.append((match.start(), closing(code, match.end() - 1), owner[1], kind))
    result = []
    pattern = r'\b(?P<visibility>pub(?:\([^)]*\))?\s+)?(?:(?:unsafe|const|async)\s+)*fn\s+(?P<name>\w+)'
    for match in re.finditer(pattern, code):
        owner = next((o for o in reversed(owners) if o[0] < match.start() < o[1]), None)
        start = match.start()
        end_sig = signature_end(code, match.end())
        end = end_sig if code[end_sig] == ";" else closing(code, end_sig)
        visibility = (match["visibility"] or "").strip()
        public = visibility == "pub" or bool(owner and owner[3] == "trait" and not visibility)
        identifier = "::".join([module] + ([owner[2]] if owner else []) + [match["name"]])
        signature = " ".join(source[start:end_sig].split())
        result.append({"id": identifier, "public": public, "signature": signature,
                       "line": source.count("\n", 0, start) + 1,
                       "body_sha256": digest(source[start:end + 1].encode())})
    ids = [f["id"] for f in result if f["public"]]
    if len(set(ids)) != len(ids):
        raise ValueError(f"ambiguous public function identity in {module}")
    return result


def validate(root: Path, require_complete: bool = False) -> dict:
    inv = json.loads((root / INVENTORY).read_text())
    errors = []
    if inv.get("format_version") != 1:
        errors.append("unsupported inventory format")
    actual_paths = {str(p.relative_to(root)) for p in (root / "aerostore_core/src").glob("*.rs")}
    modules = inv["modules"]
    if set(modules) != actual_paths:
        errors.append("core source module coverage changed")
    actual_public, all_functions = {}, {}
    for name, module in modules.items():
        path = root / name
        if not path.is_file():
            errors.append(f"missing source: {name}")
            continue
        if digest(path.read_bytes()) != module["sha256"]:
            errors.append(f"source changed: {name}")
        if module["classification"] not in {"covered", "boundary", "excluded", "reexport"} or not module["reason"].strip():
            errors.append(f"invalid module scope: {name}")
        for fn in functions(path.read_text(), path.stem):
            all_functions.setdefault(fn["id"], []).append(fn)
            if fn["public"]:
                actual_public[fn["id"]] = dict(fn, source=name)
    entries = inv["apis"]
    if len(entries) != len({e["id"] for e in entries}):
        errors.append("duplicate API inventory entry")
    if set(actual_public) != {e["id"] for e in entries}:
        errors.append("public API coverage changed")
    contracts = inv["contracts"]
    nodes = inv["lock_nodes"]
    open_semantics = []
    for entry in entries:
        actual = actual_public.get(entry["id"])
        if actual and any(entry[k] != actual[k] for k in ("source", "signature", "body_sha256")):
            errors.append(f"API signature/body changed: {entry['id']}")
        if entry["contract"] not in contracts:
            errors.append(f"unknown API contract: {entry['id']}")
        if entry["classification"] not in {"covered", "boundary", "excluded"}:
            errors.append(f"invalid API classification: {entry['id']}")
        module = modules.get(entry["source"])
        if module and module["classification"] != entry["classification"]:
            errors.append(f"API/module scope mismatch: {entry['id']}")
        if (entry["classification"] == "excluded") != (entry["contract"] == "excluded"):
            errors.append(f"excluded API contract mismatch: {entry['id']}")
        if entry.get("semantics") == "open" and entry["classification"] != "excluded":
            open_semantics.append(entry["id"])
        elif entry.get("semantics") != "defined":
            errors.append(f"undefined API semantic status: {entry['id']}")
    for identifier, contract in contracts.items():
        for field in ("preconditions", "success", "failure", "limitations"):
            if not isinstance(contract.get(field), str) or not contract[field].strip():
                errors.append(f"missing {field} contract: {identifier}")
        for ref in contract["references"]:
            if not (root / ref).is_file():
                errors.append(f"missing contract reference: {ref}")
    for field in ("public_data", "implicit_traits", "macros_and_reexports"):
        if not inv["nonfunction_surface"].get(field, "").strip():
            errors.append(f"missing nonfunction public-surface contract: {field}")
    for node, description in nodes.items():
        if not isinstance(description, str) or not description.strip():
            errors.append(f"undocumented lock node: {node}")
    adjacency = {node: set() for node in nodes}
    edge_ids = set()
    for edge in inv["lock_edges"]:
        if edge["id"] in edge_ids:
            errors.append("duplicate lock edge")
        edge_ids.add(edge["id"])
        if edge["from"] not in nodes or edge["to"] not in nodes:
            errors.append(f"unknown lock node: {edge['id']}")
            continue
        if edge["kind"] not in {"nested", "released_before", "caller_held", "nonblocking_owner", "callback_restriction", "progress_dependency"}:
            errors.append(f"unknown lock relation: {edge['id']}")
        if not edge["reason"].strip() or not edge["references"]:
            errors.append(f"undocumented lock relation: {edge['id']}")
        for ref in edge["references"]:
            if ref not in all_functions:
                errors.append(f"missing lock source function: {ref}")
        if edge["kind"] in {"nested", "caller_held", "progress_dependency"}:
            adjacency[edge["from"]].add(edge["to"])
    path_ids, referenced_edges = set(), set()
    for path in inv["lock_paths"]:
        if path["id"] in path_ids:
            errors.append("duplicate lock path")
        path_ids.add(path["id"])
        if not path["edges"] or not path["scope"].strip():
            errors.append(f"empty lock path: {path['id']}")
        for edge in path["edges"]:
            referenced_edges.add(edge)
            if edge not in edge_ids:
                errors.append(f"missing lock edge in path {path['id']}: {edge}")
    if referenced_edges != edge_ids:
        errors.append("lock graph/path coverage differs")
    visiting, visited = set(), set()
    def visit(node):
        if node in visiting:
            errors.append(f"cycle in declared blocking lock graph: {node}")
            return
        if node in visited:
            return
        visiting.add(node)
        for successor in adjacency[node]:
            visit(successor)
        visiting.remove(node)
        visited.add(node)
    for node in nodes:
        visit(node)
    gaps = [gap["id"] for gap in inv["audit_gaps"] if gap["blocks_p0"]]
    complete = not open_semantics and not gaps
    if inv["p0_complete"] != complete:
        errors.append("P0 completion flag disagrees with semantic audit gaps")
    if require_complete and not complete:
        errors.append("P0 exit audit remains open")
    return {"passed": not errors, "p0_complete": complete and not errors,
            "checked_modules": len(modules), "checked_public_apis": len(entries),
            "declared_lock_edges": len(edge_ids), "open_semantics": open_semantics,
            "blocking_audit_gaps": gaps, "errors": errors,
            "scope": "Reviewed contract coverage and exact-source freshness; not semantic verification or inferred deadlock freedom."}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    try:
        report = validate(args.root.resolve(), args.require_complete)
    except (OSError, ValueError, KeyError, TypeError) as error:
        report = {"passed": False, "errors": [str(error)]}
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
