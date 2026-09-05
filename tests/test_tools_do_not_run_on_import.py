"""Importing a fleet tool must not do the tool's work.

Read statically, with ``ast``, and never by importing the files under test.
That is not a stylistic preference: the defect this covers is that importing
``worker.py`` served the live pool queue and importing ``render_identity.py``
rendered a model and overwrote a recorded result in the live store. A test
that imported them to prove importing them is unsafe would commit the very
act it is checking for, on whichever box ran the suite. Both happened on
2026-09-05, to a session sweeping these modules for their constants.

So the property is a property of the source: at module level a tool may
declare constants, define functions and classes, import, and run the
``sys.path`` bootstrap every fleet tool needs before it can import
``prismabuild``. Anything else belongs under ``if __name__ == "__main__"``.

Two readings, because one of them alone would have missed the worse half.
Reading the statements catches ``print(...)`` and a bare call, but an
assignment is declarative by shape, so ``outcome = q.serve_once(...)`` --
the statement that actually claimed an action off the live queue -- reads
like a constant. What separates the two is the callee: ``SH = Path(...)``
and ``RUNTIME_ROOT = generation_root(__file__)`` call something imported,
while ``q.serve_once(...)`` calls a method on an object this module built a
line earlier. Constructing a value is declarative; asking an object you
just constructed to go do something is not.

Known limit, recorded rather than papered over: work reached through an
imported callable, such as a module-level ``shutil.rmtree(...)`` bound to a
name, still reads as a constant to both passes. Closing that needs to know
what the callee does, which no static reading can supply.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

TOOLS = sorted((Path(__file__).resolve().parents[1] / "tools" / "fleet").glob("*.py"))

#: The bootstrap every tool runs before it can import ``prismabuild``: a tool
#: is executed out of whichever runtime generation it was published into, so
#: it has to put its own directory and that generation's ``src`` on the path
#: at module level. It touches no shared state and starts no work.
BOOTSTRAP_CALLS = frozenset({"sys.path.insert", "os.environ.setdefault"})


def _statement_kind(node: ast.stmt) -> str | None:
    """What ``node`` does at module level, or ``None`` if it is declarative."""

    if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef,
                         ast.AsyncFunctionDef, ast.ClassDef, ast.Assign,
                         ast.AnnAssign, ast.AugAssign)):
        return None
    if isinstance(node, ast.Expr):
        if isinstance(node.value, ast.Constant):
            return None  # a docstring or a bare string
        if isinstance(node.value, ast.Call):
            called = ast.unparse(node.value.func)
            if called in BOOTSTRAP_CALLS:
                return None
            return f"calls {called}()"
        return f"evaluates {ast.unparse(node.value)}"
    if isinstance(node, ast.If):
        if "__name__" in ast.unparse(node.test):
            return None
        return f"branches on {ast.unparse(node.test)}"
    return ast.unparse(node).split("\n", 1)[0]


def _side_effects(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for node in tree.body:
        kind = _statement_kind(node)
        if kind is not None:
            found.append(f"{path.name}:{node.lineno} {kind}")
    return found


def _method_calls_on_own_objects(path: Path) -> list[str]:
    """Module-level calls to a method of a name the module itself assigned."""

    tree = ast.parse(path.read_text(), filename=str(path))
    built: set[str] = set()
    found = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if isinstance(node, ast.If) and "__name__" in ast.unparse(node.test):
            continue
        for sub in ast.walk(node):
            if not (isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Attribute)):
                continue
            base = sub.func.value
            while isinstance(base, (ast.Attribute, ast.Subscript)):
                base = base.value
            if isinstance(base, ast.Name) and base.id in built:
                found.append(f"{path.name}:{sub.lineno} {ast.unparse(sub)[:70]}")
        # After the walk: a name is only "the module's own" from the line
        # after it is bound, so a self-referential right-hand side does not
        # count itself.
        if isinstance(node, ast.Assign):
            built.update(t.id for t in node.targets if isinstance(t, ast.Name))
    return found


@pytest.mark.parametrize("path", TOOLS, ids=lambda p: p.name)
def test_a_tool_does_no_work_at_module_level(path: Path) -> None:
    assert _side_effects(path) == [], (
        f"{path.name} does work when it is merely imported. Move it into a "
        "function called under `if __name__ == \"__main__\"`."
    )


def test_the_reader_sees_work_that_is_not_behind_the_guard() -> None:
    """The check is not vacuous: unguarded work is reported, guarded is not."""

    unguarded = Path(__file__).parent / "_unguarded_probe.py"
    unguarded.write_text("import json\nX = 1\nprint(json.dumps({}))\n")
    guarded = Path(__file__).parent / "_guarded_probe.py"
    guarded.write_text(
        "import json\nX = 1\n\n\ndef main():\n    print(json.dumps({}))\n\n\n"
        'if __name__ == "__main__":\n    main()\n'
    )
    try:
        assert _side_effects(unguarded) == ["_unguarded_probe.py:3 calls print()"]
        assert _side_effects(guarded) == []
    finally:
        unguarded.unlink()
        guarded.unlink()


def test_the_bootstrap_itself_is_not_reported() -> None:
    """``sys.path`` setup is the one module-level call a fleet tool may make."""

    probe = Path(__file__).parent / "_bootstrap_probe.py"
    probe.write_text(
        "import sys\nfrom pathlib import Path\n"
        "sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))\n"
    )
    try:
        assert _side_effects(probe) == []
    finally:
        probe.unlink()


@pytest.mark.parametrize("path", TOOLS, ids=lambda p: p.name)
def test_a_tool_drives_none_of_its_own_objects_at_module_level(path: Path) -> None:
    assert _method_calls_on_own_objects(path) == [], (
        f"{path.name} drives an object it constructed, at import time. This is "
        "the shape that served the live queue: build the object inside a "
        "function and call it there."
    )


def test_the_second_reading_separates_a_constant_from_a_command() -> None:
    """It must fire on the queue shape and stay quiet on the constant shape."""

    probe = Path(__file__).parent / "_own_object_probe.py"
    probe.write_text(
        "from pathlib import Path\n"
        "from prismabuild import pool\n"
        "SH = Path('/mnt/shared/prismabuild-fleet')\n"
        "q = pool.PoolQueue(SH / 'pb-queue')\n"
        "outcome = q.serve_once(tags=['gb10'])\n"
    )
    try:
        found = _method_calls_on_own_objects(probe)
        assert found == ["_own_object_probe.py:5 q.serve_once(tags=['gb10'])"]
        # ``SH = Path(...)`` and ``pool.PoolQueue(...)`` are construction from
        # imported names and must not be reported.
        assert not any("Path(" in f or "PoolQueue(" in f for f in found)
    finally:
        probe.unlink()
