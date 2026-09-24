#!/usr/bin/env python3
"""Compose native final-write planning and base validation with completion."""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "planned.rs"
OUTPUT = HERE / "planned.verus.rs"
SOURCE = ROOT / "aerostore_core/src/occ_partitioned.rs"


def component(name):
    spec = importlib.util.spec_from_file_location("planned_" + name,
        ROOT / "verification" / name / "generate.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def check_order(template):
    calls = ("planning::final_write_indices::<M>(&tx)",
             "planning::index_changes(&storage.rows,&tx,&indices)",
             "admission::has_write_base_conflict(&storage.rows,&tx,&indices)",
             "publish_then_complete::<P,R,L,I,C>(storage,publisher,token,&plan,&ordinary_plan)")
    lexer = component("lookup").adapter
    lexer.TOKEN = re.compile(lexer.TOKEN.pattern + "|@")
    tokens = lexer.tokenize(template)
    positions = []
    for call in calls:
        needle = lexer.tokenize(call)
        matches = [i for i in range(len(tokens)) if tokens[i:i+len(needle)] == needle]
        if len(matches) != 1:
            raise ValueError("missing/duplicate planned commit operation")
        positions.append(matches[0])
    if positions != sorted(positions):
        raise ValueError("planned commit operation order changed")


def render(template=None):
    source = SOURCE.read_text()
    # Completion embeds the actual ordinary commit interval, whose exact checked
    # native prefix includes final selection, key extraction, locks and validation.
    completion = component("commit_completion").render()
    result = TEMPLATE.read_text() if template is None else template
    check_order(result)
    return ("// Generated native planning/base-validation/publication join.\n" + completion
        + "\npub mod planning {\n" + component("write_plan").render_module(source) + "\n}\n"
        + "\npub mod admission {\n" + component("write_admission").render_module(source) + "\n}\n"
        + result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != expected:
            raise SystemExit("stale planned commit composition")
    else:
        OUTPUT.write_text(expected)


if __name__ == "__main__":
    main()
