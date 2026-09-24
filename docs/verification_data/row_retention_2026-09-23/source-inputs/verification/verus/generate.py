#!/usr/bin/env python3
"""Expose proof-only comments without changing the production executable body."""
from pathlib import Path
import argparse
import re

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aerostore_verified/src/lib.rs"
SPECS = Path(__file__).with_name("spec.rs")
OUTPUT = Path(__file__).with_name("kernels.verus.rs")
ANNOTATION = re.compile(r"/\*@([\s\S]*?)@\*/")
PROOF = re.compile(r"/\*PROOF-BEGIN\*/[\s\S]*?/\*PROOF-END\*/")
INTERFACES = {
    "pub fn canonical_buckets_sort(input: &[usize], bucket_count: usize) -> Result<Vec<usize>, usize>": "bucket_result(input@, bucket_count, result)",
    "pub fn canonical_buckets_bitmap(input: &[usize], bucket_count: usize) -> Result<Vec<usize>, usize>": "bucket_result(input@, bucket_count, result)",
    "pub fn stamp_precedes_snapshot(stamp: u64, transaction_id: u64) -> bool": "result <==> stamp < transaction_id",
}


def strip_ordinary_comments(source: str) -> str:
    """Lex the deliberately small kernel subset; preserve only proof comments.

    String/character literals and macros are not needed by this pilot. Reject
    them rather than pretending a regex can parse general Rust source safely.
    Nested ordinary block comments are accepted but cannot hide proof roots.
    """
    result = []
    i = 0
    while i < len(source):
        if source.startswith("//", i):
            end = source.find("\n", i)
            i = len(source) if end < 0 else end
        elif source.startswith("/*@", i):
            end = source.find("@*/", i + 3)
            if end < 0:
                raise ValueError("unclosed proof annotation")
            result.append(source[i:end + 3])
            i = end + 3
        elif source.startswith("/*", i):
            depth = 1
            i += 2
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
                raise ValueError("unclosed Rust block comment")
            result.append(" ")
        else:
            if source[i] in ('"', "'"):
                raise ValueError("literal/character syntax is outside the pilot adapter subset")
            result.append(source[i])
            i += 1
    return "".join(result)


def render(source: str) -> str:
    # This adapter is an explicit trusted transformation, frozen by the gate.
    # It admits no user-editable public preconditions or postconditions.
    source = strip_ordinary_comments(source)
    if re.search(r"\b(assume|admit|axiom|external_body|external_fn_specification|assume_specification)\b|verify\s*\(\s*false\s*\)", source):
        raise ValueError("verification bypass is forbidden in the production kernel source")
    tests = list(re.finditer(r"#\[cfg\(test\)\]\s*mod tests;", source))
    if len(tests) != 1:
        raise ValueError("expected the single fixed production test module")
    source = source[:tests[0].start()] + source[tests[0].end():]
    executable = ANNOTATION.sub("", source)
    if re.search(r"\b(mod|use|extern|unsafe|macro_rules)\b|\b[A-Za-z_][A-Za-z_0-9]*\s*!\s*[({\[]", executable):
        raise ValueError("imports, modules, unsafe, and macros are outside the kernel adapter subset")
    if "#![" in executable:
        raise ValueError("crate-level attributes are outside the kernel adapter subset")
    for attribute in re.findall(r"#\[([^\]]+)\]", executable):
        if attribute != "inline":
            raise ValueError("unapproved production/proof configuration attribute: " + attribute)
    plain = ANNOTATION.sub("", source)
    for annotation in ANNOTATION.findall(source):
        annotation = annotation.strip()
        if any(token in annotation for token in ('"', "//", "/*", "*/")):
            raise ValueError("strings and nested comments are outside the proof adapter grammar")
        if annotation.startswith("invariant"):
            if any(token in annotation for token in ("{", "}", ";")) or re.search(r"\b(requires|ensures|fn|return|let|while|loop|unsafe|extern)\b", annotation):
                raise ValueError("loop annotation contains more than invariants/decreases")
        elif annotation.startswith("proof {"):
            depth = 0
            for index, character in enumerate(annotation[annotation.index("{"):]):
                if character == "{":
                    depth += 1
                elif character == "}":
                    depth -= 1
                    if depth == 0 and index != len(annotation[annotation.index("{"):]) - 1:
                        raise ValueError("executable suffix after proof block")
                if depth < 0:
                    raise ValueError("unbalanced proof block")
            if depth != 0:
                raise ValueError("unbalanced proof block")
        elif not re.fullmatch(r"let ghost [A-Za-z_][A-Za-z_0-9]* = [A-Za-z_][A-Za-z_0-9]*@;", annotation):
            raise ValueError("annotation is not an isolated proof block, ghost capture, or invariant")
    body = ANNOTATION.sub(lambda m: "/*PROOF-BEGIN*/" + m[1] + "/*PROOF-END*/", source)
    for signature, contract in INTERFACES.items():
        name = signature.split("(")[0].split()[-1]
        matches = list(re.finditer(r"pub\s+fn\s+" + name + r"\s*\([^)]*\)\s*->\s*(?:Result<Vec<usize>,\s*usize>|bool)", body))
        if len(matches) != 1 or re.sub(r"\s", "", matches[0][0]) != re.sub(r"\s", "", signature):
            raise ValueError("missing or modified public proof root: " + signature)
        end = matches[0].end()
        body = body[:end] + "/*PROOF-BEGIN*/\n ensures " + contract + ", /*PROOF-END*/" + body[end:]
    result_types = ("Result<Vec<usize>, usize>", "bool")
    for typ in result_types:
        body = body.replace("-> " + typ, "-> (result: " + typ + ")")
    erased = PROOF.sub("", body)
    for typ in result_types:
        erased = erased.replace("-> (result: " + typ + ")", "-> " + typ)
    if erased != plain:
        raise ValueError("generated executable body differs from the production source")
    return "// Generated by verification/verus/generate.py; do not edit.\nuse vstd::prelude::*;\nverus! {\n" + SPECS.read_text() + "\n" + body + "\n}\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    generated = render(SOURCE.read_text())
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != generated:
            raise SystemExit("Verus adapter is stale; run verification/verus/generate.py")
    else:
        OUTPUT.write_text(generated)


if __name__ == "__main__":
    main()
