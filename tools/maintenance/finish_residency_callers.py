"""Replay the C21 reviewed caller corrections from the retained first gate."""
import argparse
import ast
import json
from pathlib import Path
import subprocess


BASE = "b66becd2e55eebe623228c3eca90bf5fcf57506b"


def complete_residency_callers(checkout: Path) -> list[str]:
    def source(path):
        return subprocess.check_output(
            ["git", "-C", str(checkout), "show", BASE + ":" + path], text=True)

    fields = next(n for n in ast.parse(source(
        "src/prismabuild/submission_types.py")).body
        if isinstance(n, ast.ClassDef) and n.name == "SubmissionRequest")
    allowed = {n.target.id for n in fields.body if isinstance(n, ast.AnnAssign)
               and isinstance(n.target, ast.Name)}
    paths = subprocess.check_output(
        ["git", "-C", str(checkout), "ls-tree", "-r", "--name-only", BASE,
         "tests"], text=True).splitlines()
    replacements = {}
    path = "tools/fleet/tier_loop.py"
    before = source(path)
    assert before.count("residency_plan.ConsumerWindow.decision_owned(") == 2
    replacements[path] = (before, before.replace(
        "residency_plan.ConsumerWindow.decision_owned(",
        "residency_plan.window_owned("))
    for path in paths:
        if not path.endswith(".py"):
            continue
        before = source(path)
        if ".filing_options(" not in before:
            continue
        edits = []
        lines = before.splitlines(keepends=True)
        for node in ast.walk(ast.parse(before)):
            if not isinstance(node, ast.Assign) or not any(
                    isinstance(t, ast.Name) and t.id == "args" for t in node.targets):
                continue
            value = node.value
            if not isinstance(value, ast.Call) or ast.unparse(value.func) != "types.SimpleNamespace":
                continue
            assert not value.args and all(k.arg in allowed for k in value.keywords), path
            start = sum(map(len, lines[:value.func.lineno - 1])) + value.func.col_offset
            end = sum(map(len, lines[:value.func.end_lineno - 1])) + value.func.end_col_offset
            edits.append((start, end))
        if edits:
            after = before
            for start, end in sorted(edits, reverse=True):
                after = after[:start] + "submit_core.SubmissionRequest" + after[end:]
            if path in ("tests/test_a_stage_mover_declares_the_cpu_and_retries_it_owns.py",
                        "tests/test_an_egress_declares_the_cpu_it_owns.py"):
                loop = "    for name, value in overrides.items():\n        setattr(args, name, value)"
                assert after.count(loop) == 1 and "from dataclasses import" not in after
                after = after.replace("import types\n", "import types\nfrom dataclasses import replace\n")
                after = after.replace(loop, "    args = replace(args, **overrides)")
            replacements[path] = before, after
    assert len(replacements) == 12
    for path, (before, after) in replacements.items():
        target = checkout / path
        assert target.read_text() in (before, after), path
        ast.parse(after, filename=path)
        target.write_text(after)
    return sorted(replacements)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkout", type=Path)
    args = parser.parse_args()
    print(json.dumps({"base": BASE, "migrated": complete_residency_callers(args.checkout)}))
