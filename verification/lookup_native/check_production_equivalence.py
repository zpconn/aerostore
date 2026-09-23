#!/usr/bin/env python3
"""Compare reviewed Rust/Cargo inputs after narrowly excluding test-only edits.

This is lexical change detection, not a compiler or semantic-equivalence proof.
Only the named cfg(test) modules and the exact reviewed snapshot hook may differ.
"""
from pathlib import Path
import argparse
import hashlib
import json
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]
NATIVE_ROOTS = ["Cargo.toml", "Cargo.lock", "rust-toolchain", "rust-toolchain.toml", ".cargo",
                "aerostore_core", "aerostore_verified", "aerostore_macros", "aerostore_tcl"]
TEST_MODULES = {
    "aerostore_core/src/procarray.rs": {"tests"},
    "aerostore_core/src/occ_partitioned.rs": {"predicate_completion_tests"},
}
PROC = "aerostore_core/src/procarray.rs"
HOOK_DECLARATION = """static SNAPSHOT_ACQUIRING_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);"""
HOOK_CALL = """#[cfg(test)] SNAPSHOT_ACQUIRING_HOOK.with(|hook| {
    if let Some(hook) = hook.borrow_mut().take() { hook(); }
});"""


def digest(data):
    return hashlib.sha256(data).hexdigest()


def rust_tokens(source):
    """Retain literal spelling and punctuation jointness; drop only comments/space."""
    out, pos = [], 0
    while pos < len(source):
        rest = source[pos:]
        if rest[0].isspace():
            pos += 1
        elif rest.startswith("//"):
            end = source.find("\n", pos)
            pos = len(source) if end < 0 else end + 1
        elif rest.startswith("/*"):
            depth, pos = 1, pos + 2
            while depth:
                if pos >= len(source):
                    raise ValueError("unterminated block comment")
                if source.startswith("/*", pos):
                    depth, pos = depth + 1, pos + 2
                elif source.startswith("*/", pos):
                    depth, pos = depth - 1, pos + 2
                else:
                    pos += 1
        elif (raw := re.match(r'(?:br|cr|r)(#*)"', rest)):
            ending = '"' + raw[1]
            end = source.find(ending, pos + raw.end())
            if end < 0:
                raise ValueError("unterminated raw string")
            end += len(ending)
            out.append(source[pos:end]); pos = end
        elif (quoted := re.match(r'(?:b|c)?"(?:\\.|[^"\\])*"', rest, re.S)):
            out.append(quoted[0]); pos += quoted.end()
        elif (char := re.match(r"b?'(?:\\(?:u\{[0-9A-Fa-f_]+\}|x[0-9A-Fa-f]{2}|.)|[^'\\\r\n])'", rest)):
            out.append(char[0]); pos += char.end()
        elif (token := re.match(r"'[A-Za-z_][A-Za-z_0-9]*|[A-Za-z_][A-Za-z_0-9]*|[0-9][A-Za-z_0-9]*(?:\.[0-9][A-Za-z_0-9]*)?|<<=|>>=|\.\.=|\.\.\.|::|->|=>|==|!=|<=|>=|&&|\|\||\+=|-=|\*=|/=|%=|\^=|&=|\|=|<<|>>|\.\.|[{}()\[\];,:.!?+*/%<>=&|^~@$#-]", rest)):
            out.append(token[0]); pos += token.end()
        else:
            raise ValueError(f"unsupported Rust lexical input at {pos}: {rest[:30]!r}")
    return out


def balanced_end(tokens, start):
    pairs, stack = {"{": "}", "[": "]", "(": ")"}, []
    for index in range(start, len(tokens)):
        token = tokens[index]
        if token in pairs:
            stack.append(pairs[token])
        elif token in pairs.values():
            if not stack or stack.pop() != token:
                raise ValueError("unbalanced Rust delimiters")
            if not stack:
                return index + 1
    raise ValueError("unclosed Rust delimiters")


def locations(tokens, needle):
    return [i for i in range(len(tokens)) if tokens[i:i + len(needle)] == needle]


def production_tokens(name, source):
    tokens = rust_tokens(source)
    exclusions = []
    if name not in TEST_MODULES:
        return tokens, exclusions
    prefix = rust_tokens("#[cfg(test)] mod")
    pos, level, kept = 0, 0, []
    while pos < len(tokens):
        if level == 0 and tokens[pos:pos + len(prefix)] == prefix:
            module = tokens[pos + len(prefix)]
            opening = pos + len(prefix) + 1
            if module in TEST_MODULES[name]:
                if tokens[opening] != "{":
                    raise ValueError("reviewed test module has changed item shape: " + module)
                end = balanced_end(tokens, opening)
                exclusions.append("cfg(test) module " + module)
                pos = end
                continue
            # Unreviewed test modules remain in the comparison. Their edits
            # must not disappear merely because they carry cfg(test).
        level += (tokens[pos] == "{") - (tokens[pos] == "}")
        if level < 0:
            raise ValueError("invalid item nesting")
        kept.append(tokens[pos]); pos += 1
    if level:
        raise ValueError("unclosed item nesting")
    tokens = kept
    if name != PROC:
        return tokens, exclusions

    # The extra declaration must be inside the existing cfg(test) thread_local
    # block; the extra call must be the first statement of this one method.
    block_prefix = rust_tokens("#[cfg(test)] thread_local! {")
    blocks = locations(tokens, block_prefix)
    if len(blocks) != 1:
        raise ValueError("reviewed ProcArray test-hook declaration block changed")
    opening = blocks[0] + len(block_prefix) - 1
    closing = balanced_end(tokens, opening)
    declaration = rust_tokens(HOOK_DECLARATION)
    declarations = locations(tokens, declaration)
    if declarations:
        if len(declarations) != 1 or not opening < declarations[0] < closing or declarations[0] + len(declaration) > closing:
            raise ValueError("snapshot hook declaration is not uniquely test-only")
        at = declarations[0]
        tokens[at:at + len(declaration)] = []
        exclusions.append("exact cfg(test) SNAPSHOT_ACQUIRING_HOOK declaration")
    call = rust_tokens(HOOK_CALL)
    calls = locations(tokens, call)
    if calls:
        functions = locations(tokens, ["fn", "create_transaction_snapshot", "("])
        if len(calls) != 1 or len(functions) != 1:
            raise ValueError("snapshot hook call is not unique")
        opening = tokens.index("{", functions[0])
        if calls[0] != opening + 1:
            raise ValueError("snapshot hook moved from its reviewed pre-acquisition position")
        tokens[calls[0]:calls[0] + len(call)] = []
        exclusions.append("exact cfg(test) SNAPSHOT_ACQUIRING_HOOK call")
    if bool(declarations) != bool(calls):
        raise ValueError("snapshot hook declaration/call mismatch")
    return tokens, exclusions


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def selected(name):
    return name.endswith((".rs", ".toml")) or name in ("Cargo.lock", "rust-toolchain")


def current_inventory():
    names = git("ls-files", "--cached", "--others", "--exclude-standard", "--", *NATIVE_ROOTS).decode().splitlines()
    return sorted({name for name in names if selected(name)})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", default="1e7311a")
    parser.add_argument("--output", type=Path, default=ROOT / "target/verification-lookup-native/production-equivalence.json")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "target"):
        parser.error("diagnostic evidence must be below target/")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {"schema": 1, "passed": False, "formal_semantic_equivalence_proved": False,
              "scope": "reviewed Rust/Cargo production-token change detection", "files": {}}
    try:
        baseline = git("rev-parse", "--verify", args.baseline + "^{commit}").decode().strip()
        report["baseline_commit"] = baseline
        report["script_sha256"] = digest(Path(__file__).read_bytes())
        baseline_files = sorted(name for name in git("ls-tree", "-r", "--name-only", baseline, "--", *NATIVE_ROOTS).decode().splitlines() if selected(name))
        current_files = current_inventory()
        if baseline_files != current_files:
            report["added_inputs"] = sorted(set(current_files) - set(baseline_files))
            report["removed_inputs"] = sorted(set(baseline_files) - set(current_files))
            raise ValueError("Rust/Cargo input inventory changed")
        differences, changed = [], []
        for name in baseline_files:
            if not (ROOT / name).is_file() or (ROOT / name).is_symlink():
                raise ValueError("missing or nonregular working input: " + name)
            before = git("show", f"{baseline}:{name}")
            after = (ROOT / name).read_bytes()
            if before != after:
                changed.append(name)
            if name.endswith(".rs"):
                left, ignored_left = production_tokens(name, before.decode())
                right, ignored_right = production_tokens(name, after.decode())
                before_projection = json.dumps(left, ensure_ascii=False, separators=(",", ":")).encode()
                after_projection = json.dumps(right, ensure_ascii=False, separators=(",", ":")).encode()
            else:
                before_projection, after_projection = before, after
                ignored_left, ignored_right = [], []
            report["files"][name] = {"baseline_sha256": digest(before), "current_sha256": digest(after),
                "baseline_production_token_sha256": digest(before_projection),
                "current_production_token_sha256": digest(after_projection),
                "baseline_exclusions": ignored_left, "current_exclusions": ignored_right}
            if before_projection != after_projection:
                differences.append(name)
        report["checked_files"] = len(baseline_files)
        report["changed_inputs"] = changed
        report["production_differences"] = differences
        report["script_stable"] = digest(Path(__file__).read_bytes()) == report["script_sha256"]
        report["source_stable"] = report["script_stable"] and current_inventory() == current_files and all(
            digest((ROOT / name).read_bytes()) == data["current_sha256"] for name, data in report["files"].items())
        if differences or not report["source_stable"]:
            raise ValueError("production projection differs or source changed during comparison")
        report["passed"] = True
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "checked_files": report.get("checked_files"),
                      "error": report.get("error"), "receipt": str(output)}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
