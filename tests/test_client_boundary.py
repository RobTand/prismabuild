"""Freeze what PrismaBuild core knows about its clients (#1250).

PrismaBuild (PB) exists independently of Tessera and PrismaQuant (PQ)
(Rob, 2026-09-27). A client may drive PB through its public interface, and
PB may host a plugin a client registers, but PB core does not import a
client, load a client's files, branch on a client's formats or name a
client's venv. This test is the mechanical half of that rule, step 0 of
the decoupling plan in the 2026-09-27 coupling inventory.

It scans ``src/prismabuild/`` and every tool ``tools/fleet/publish_runtime.py``
publishes (``FLEET_SCRIPTS`` and ``TOP_LEVEL_SCRIPTS``, read from that file
with ``ast``, so the scope follows the published generation). In each module
it parses with ``ast`` and finds:

- **imports** of ``tessera`` and ``prismaquant``, at any depth;
- **loads**: ``import_module``, ``spec_from_file_location`` and
  ``run_path`` calls whose arguments name a client path. The strings
  read are the call's own arguments plus the values assigned, in the same
  function, to a name or attribute the call passes. A static guard, not
  execution;
- **literals**: ``str``/``bytes`` constants outside docstrings matching
  ``prismaquant\\.(?!prismabuild\\.)``, or containing ``src/tessera/`` or
  ``prismaquant-cu130``.

Each finding is one line of ``tests/boundary_allowlists/client_references.txt``
(``path<TAB>kind<TAB>value``, one line per occurrence, keyed by module,
callee or matched token, never by line number).

Two vocabularies are frozen beside it:

- ``core_vocabulary.txt``: the members of ``core._ARTIFACT_FAMILIES`` and
  ``core._PYTHON_DISTRIBUTIONS``, which carried a quantization format
  (``codebook``) and a retired client lane (``gridbook``) into scheduler core.
  Step 4 (#1076) made both submitter-declared fields, so the list is empty and
  a vocabulary reintroduced under either name fails;
- ``legacy_ids.txt``: every distinct ``prismaquant.prismabuild.*`` record ID
  the scanned code spells. They are inside hashed action bodies and stay as
  legacy; a new record type uses ``prismabuild.*``. The one pool-export row
  is a documented one-time compatibility correction for an already deployed
  wire ID whose readers require exact schema equality (#1384); it is not a
  mechanism for adding future IDs.

Every list is held to exact equality with the code:

- a new reference, member or ID fails;
- a removed one fails until its line is deleted in the same change.

The lists only shrink, apart from the one documented #1384 correction above;
step 0 lands no CI half for them, because PB has no pull-request CI that
checks out code (see the PR). Step 4 of the plan (#1076) moved the Tessera
drivers and ``render_identity.py`` to PrismaQuant, took the GPU-interpreter
signature from configuration and replaced the vocabularies with
submitter-declared fields, which emptied both lists. The legacy IDs stay
unless a wire version changes identity anyway (step 8).
"""
from __future__ import annotations

import ast
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOWLISTS = Path(__file__).with_name("boundary_allowlists")
PUBLISHER = "tools/fleet/publish_runtime.py"
CLIENTS = ("tessera", "prismaquant")
VOCABULARIES = ("_ARTIFACT_FAMILIES", "_PYTHON_DISTRIBUTIONS")
_LOADERS = {"import_module", "spec_from_file_location", "run_path"}
_CLIENT_PATH = re.compile(r"tessera|prismaquant", re.IGNORECASE)
_PQ_TOKEN = re.compile(r"prismaquant\.(?!prismabuild\.)[A-Za-z0-9_.]*")
_SUBSTRINGS = ("src/tessera/", "prismaquant-cu130")
_LEGACY_ID = re.compile(r"prismaquant\.prismabuild\.[A-Za-z0-9_.]*")

_DOC_OWNERS = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def _docstring_ids(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, _DOC_OWNERS) and node.body:
            first = node.body[0]
            if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, (str, bytes))):
                ids.add(id(first.value))
    return ids


def _text(value: str | bytes) -> str:
    return value.decode("latin-1") if isinstance(value, bytes) else value


def _escape(value: str) -> str:
    return value.encode("unicode_escape").decode("ascii")


def _strings(node: ast.AST) -> list[str]:
    return [_text(n.value) for n in ast.walk(node)
            if isinstance(n, ast.Constant) and isinstance(n.value, (str, bytes))]


def _base(node: ast.AST) -> str | None:
    """The name or attribute a call argument reads, as source text."""
    while isinstance(node, (ast.Subscript, ast.Starred)):
        node = node.value
    if isinstance(node, (ast.Name, ast.Attribute)):
        return ast.unparse(node)
    return None


def _scope_nodes(node: ast.AST):
    """A lexical scope's nodes, not the bodies of nested definitions."""
    for child in ast.iter_child_nodes(node):
        yield child
        if not isinstance(child, _SCOPES):
            yield from _scope_nodes(child)


def _loads(scope: ast.AST) -> list[str]:
    """Callees of loader calls in ``scope`` that name a client path."""
    nodes = list(_scope_nodes(scope))
    assigned: dict[str, list[ast.AST]] = {}
    for node in nodes:
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                assigned.setdefault(ast.unparse(target), []).append(node.value)
    found: list[str] = []
    for node in nodes:
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        callee = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if callee not in _LOADERS:
            continue
        arguments = list(node.args) + [k.value for k in node.keywords]
        read = [s for arg in arguments for s in _strings(arg)]
        for arg in arguments:
            for value in assigned.get(_base(arg) or "", []):
                read += _strings(value)
        if any(_CLIENT_PATH.search(s) for s in read):
            found.append(callee)
    for child in nodes:
        if isinstance(child, _SCOPES):
            found += _loads(child)
    return found


def findings(source: str) -> tuple[list[tuple[str, str]], set[str]]:
    """Client references in one module, and the legacy IDs it spells."""
    tree = ast.parse(source)
    docstrings = _docstring_ids(tree)
    found: list[tuple[str, str]] = []
    ids: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [("import", a.name) for a in node.names
                      if a.name.split(".")[0] in CLIENTS]
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module and node.module.split(".")[0] in CLIENTS:
                found.append(("import", node.module))
        elif (isinstance(node, ast.Constant) and isinstance(node.value, (str, bytes))
                and id(node) not in docstrings):
            text = _text(node.value)
            found += [("literal", _escape(m)) for m in _PQ_TOKEN.findall(text)]
            for substring in _SUBSTRINGS:
                found += [("literal", substring)] * text.count(substring)
            ids.update(m.rstrip(".") for m in _LEGACY_ID.findall(text))
    found += [("load", callee) for callee in _loads(tree)]
    return found, ids


def scanned(root: Path = ROOT) -> list[Path]:
    published: dict[str, tuple[str, ...]] = {}
    for node in ast.parse((root / PUBLISHER).read_text(encoding="utf-8")).body:
        target = node.targets[0] if isinstance(node, ast.Assign) else getattr(node, "target", None)
        if isinstance(target, ast.Name) and target.id in ("FLEET_SCRIPTS", "TOP_LEVEL_SCRIPTS"):
            published[target.id] = ast.literal_eval(node.value)
    paths = sorted((root / "src" / "prismabuild").rglob("*.py"))
    paths += [root / "tools" / "fleet" / n for n in published["FLEET_SCRIPTS"] if n.endswith(".py")]
    paths += [root / "tools" / n for n in published["TOP_LEVEL_SCRIPTS"] if n.endswith(".py")]
    return paths


def scan(root: Path = ROOT) -> tuple[Counter, Counter]:
    references: Counter = Counter()
    ids: set[str] = set()
    for path in scanned(root):
        rel = path.relative_to(root).as_posix()
        found, spelled = findings(path.read_text(encoding="utf-8"))
        for kind, value in found:
            references[f"{rel}\t{kind}\t{value}"] += 1
        ids |= spelled
    return references, Counter(ids)


def vocabulary(root: Path = ROOT) -> Counter:
    members: Counter = Counter()
    tree = ast.parse((root / "src" / "prismabuild" / "core.py").read_text(encoding="utf-8"))
    for node in tree.body:
        target = node.targets[0] if isinstance(node, ast.Assign) else getattr(node, "target", None)
        if isinstance(target, ast.Name) and target.id in VOCABULARIES:
            value = node.value
            if isinstance(value, ast.Call) and getattr(value.func, "id", None) == "frozenset":
                value = value.args[0]
            for member in ast.literal_eval(value):
                members[f"{target.id}\t{member}"] += 1
    return members


def allowed(name: str) -> Counter:
    lines = (ALLOWLISTS / name).read_text(encoding="utf-8").splitlines()
    return Counter(line for line in lines if line and not line.startswith("#"))


def assert_equal(live: Counter, name: str, remedy: str) -> None:
    frozen = allowed(name)
    new = sorted(k for k in live if k not in frozen)
    grown = {k: (frozen[k], v) for k, v in live.items() if k in frozen and v > frozen[k]}
    shrunk = {k: (n, live.get(k, 0)) for k, n in frozen.items() if live.get(k, 0) < n}
    assert not new, f"new entries not in {name}: {new}; {remedy}"
    assert not grown, f"entries in {name} gained occurrences (allowed, live): {grown}"
    assert not shrunk, (
        f"entries removed (allowed, live): {shrunk}; delete their lines from "
        f"tests/boundary_allowlists/{name} in the same change")


def test_scanner_sees_every_reference_form():
    source = (
        '"""import prismaquant.allocator in a docstring is prose."""\n'
        "import importlib.util, runpy, sys\n"
        "import tessera.serving_parts as parts\n"
        "from prismaquant.production_weight_cache import render\n"
        "def parts_of(root):\n"
        "    path = root / 'src/tessera/serving_parts.py'\n"
        "    return importlib.util.spec_from_file_location('x', path)\n"
        "def run():\n"
        "    sys.argv = ['encoder/experiments/export_tessera_serving.py']\n"
        "    runpy.run_path(sys.argv[0])\n"
        "    importlib.import_module('dagster')\n"
        "    return (b'prismaquant.pack.v1', f'{sys}/prismaquant-cu130/bin',\n"
        "            'prismaquant.prismabuild.action.v2', 'prismaquant.prismabuild.pbrun_x.')\n"
    )
    found, ids = findings(source)
    assert sorted(found) == [
        ("import", "prismaquant.production_weight_cache"),
        ("import", "tessera.serving_parts"),
        ("literal", "prismaquant-cu130"), ("literal", "prismaquant.pack.v1"),
        ("literal", "src/tessera/"),
        ("load", "run_path"), ("load", "spec_from_file_location"),
    ]
    assert ids == {"prismaquant.prismabuild.action.v2", "prismaquant.prismabuild.pbrun_x"}


def test_the_scope_follows_the_published_tools():
    paths = {p.relative_to(ROOT).as_posix() for p in scanned()}
    assert "tools/fleet/pbrun.py" in paths
    assert "src/prismabuild/core.py" in paths
    assert "tools/fleet/profile_finish_census.py" not in paths


def test_client_references_equal_the_frozen_allowlist():
    references, _ = scan()
    assert_equal(references, "client_references.txt",
                 "PrismaBuild core does not know its clients: take the value from "
                 "the submitter or from configuration, or host a plugin the client registers")


def test_legacy_record_ids_equal_the_frozen_allowlist():
    _, ids = scan()
    assert_equal(ids, "legacy_ids.txt",
                 "a new record type uses the prismabuild.* namespace")


def test_core_vocabularies_equal_the_frozen_snapshot():
    assert_equal(vocabulary(), "core_vocabulary.txt",
                 "a client format or distribution is a submitter-declared field, "
                 "not a member of a core vocabulary")
