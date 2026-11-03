"""Synthetic-only contracts for the operator-local endings projection (#2726)."""
from __future__ import annotations

import ast
import copy
import importlib.util
import json
from pathlib import Path
import sys
import typing

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "prismabuild.public_evidence.v1"


def _module():
    __tracebackhide__ = True
    assert importlib.util.find_spec("prismabuild.operator_view") is not None, (
        "operator-local projection is not implemented"
    )
    from prismabuild import operator_view

    return operator_view


def _view(record):
    __tracebackhide__ = True
    return _module().operator_ending_view(record)


def _public(record):
    from prismabuild.public_evidence import public_evidence_view

    return public_evidence_view(record)


def _assert_equal(actual, expected):
    __tracebackhide__ = True
    if actual != expected:
        raise AssertionError("operator projection does not match its contract")


def _assert_absent(data, sentinels):
    __tracebackhide__ = True
    if any(sentinel.encode("ascii") in data for sentinel in sentinels):
        raise AssertionError("operator projection contains unapproved data")


def _transports():
    from prismabuild.submission_types import SubmissionRequest

    return typing.get_args(typing.get_type_hints(SubmissionRequest)["transport"])


class _String(str):
    pass


class _Hostile:
    def __str__(self):
        raise AssertionError("unexpected scalar conversion")

    def __repr__(self):
        raise AssertionError("unexpected scalar conversion")

    def __eq__(self, other):
        raise AssertionError("unexpected untrusted equality")

    def __hash__(self):
        return 1


class _LookalikeKey:
    """Dict lookup would accept this as a string key; iteration must not."""
    def __init__(self, spelling):
        self.spelling = spelling

    def __hash__(self):
        return hash(self.spelling)

    def __eq__(self, other):
        raise AssertionError("untrusted key compared")


class _Dict(dict):
    def items(self):
        raise AssertionError("dict subclass inspected")


class _List(list):
    def __iter__(self):
        raise AssertionError("list subclass inspected")


def test_valid_host_and_transport_are_kept():
    for host in ("a", "fleet-01.example", "HOST.9", "a" * 253):
        for transport in _transports():
            _assert_equal(_view({"host": host, "transport": transport}), {
                "schema": SCHEMA, "evidence": {"host": host, "transport": transport}})


def test_public_fields_match_public_view():
    source = {
        "status": "succeeded", "returncode": -9,
        "action_key": "1" * 64, "action_manifest_sha256": "2" * 64,
        "stdout_sha256": "3" * 64, "stderr_sha256": "4" * 64,
        "started_at": 1, "finished_at": 2.5, "observed_at": 3,
        "result": {"sha256": "5" * 64, "bytes": 17},
        "children": [{"status": "failed", "returncode": 7}],
        "host": "fleet-01", "transport": _transports()[0],
    }
    expected = _public(source)
    expected["evidence"].update(host=source["host"], transport=source["transport"])
    _assert_equal(_view(source), expected)
    # Build from the public function, rather than duplicating its projection.
    from prismabuild import public_evidence
    from unittest.mock import patch

    with patch.object(public_evidence, "public_evidence_view",
                      wraps=public_evidence.public_evidence_view) as called:
        _assert_equal(_view(source), expected)
        assert called.call_count == 1
        assert called.call_args.args[0] is source


def test_view_is_fresh_and_does_not_mutate_input():
    source = {"status": "running", "host": "fleet-01", "children": [
        {"returncode": 7}], "result": {"bytes": 12}}
    original = copy.deepcopy(source)
    first = _view(source)
    second = _view(source)
    _assert_equal(first, second)
    assert first is not second
    assert first["evidence"] is not second["evidence"]
    first["evidence"]["children"][0]["returncode"] = 99
    first["evidence"]["result"]["bytes"] = 99
    _assert_equal(source, original)
    _assert_equal(second, _view(source))


def test_invalid_lookalike_and_overlong_hosts_are_omitted():
    for host in ("", "a" * 254, "a" * 1024, "host\n", "host\r\n", "hоst",
                 ".host", "-host", " host", "host ", "host/path", "host\x1b",
                 "host:22", 1, True, None, b"host", [], {}, _String("host"), _Hostile()):
        _assert_equal(_view({"host": host}), _public({"host": host}))
    for key in (_String("host"), _LookalikeKey("host"), _Hostile()):
        _assert_equal(_view({key: "fleet-01"}), {"schema": SCHEMA, "evidence": {}})


def test_invalid_transports_are_omitted_and_not_restated():
    module = _module()
    source = Path(module.__file__).read_text()
    literals = [node.value for node in ast.walk(ast.parse(source))
                if isinstance(node, ast.Constant) and type(node.value) is str]
    assert not any(value in literals for value in _transports())
    for transport in ("POOL", "Slurm", "pool\n", "unknown", "", 0, True, None,
                      b"pool", [], {}, _String(_transports()[0]), _Hostile()):
        _assert_equal(_view({"transport": transport}), _public({"transport": transport}))
    for key in (_String("transport"), _LookalikeKey("transport")):
        _assert_equal(_view({key: _transports()[0]}), {"schema": SCHEMA, "evidence": {}})


def test_superseded_is_kept_only_as_exact_match():
    _assert_equal(_view({"status": "superseded"}), {
        "schema": SCHEMA, "evidence": {"status": "superseded"}})
    for status in ("Superseded", "SUPERSEDED", "superseded ", "superseded\n",
                   " superseded", "superſeded", _String("superseded"), _Hostile(),
                   None, 1, True, [], {}, "unknown", "pending", "ready", "running",
                   "succeeded", "failed", "cancelled", "withdrawn", "interrupted",
                   "timed_out", _String("running")):
        _assert_equal(_view({"status": status}), _public({"status": status}))
    for key in (_String("status"), _LookalikeKey("status")):
        _assert_equal(_view({key: "superseded"}), {"schema": SCHEMA, "evidence": {}})


def test_sentinels_claims_and_argv_never_appear():
    sentinels = [f"synthetic-private-{index}" for index in range(8)]
    private = {"capability": sentinels[0], "token": sentinels[1],
               "claim": {"value": sentinels[2]}, "control": {"value": sentinels[3]},
               "argv": ["native", sentinels[4]], "path": sentinels[5],
               "notes": sentinels[6], "reason": sentinels[7]}
    source = {
        **private, "host": "fleet-01", "status": "superseded",
        "transport": _transports()[0], "unknown": private,
        "result": {**private, "bytes": 12},
        "children": [{**private, "host": "child-host", "transport": _transports()[0],
                      "status": "superseded", "returncode": 7,
                      "result": {**private, "bytes": 1}},
                     [private, sentinels, {"unknown": private}]],
    }
    projected = _view(source)
    data = json.dumps(projected, ensure_ascii=True, allow_nan=False).encode("ascii")
    _assert_absent(data, sentinels)
    _assert_equal(projected, {"schema": SCHEMA, "evidence": {
        "host": "fleet-01", "transport": _transports()[0], "status": "superseded",
        "result": {"bytes": 12}, "children": [{"returncode": 7, "result": {"bytes": 1}}]}})
    with pytest.raises(AssertionError, match="unapproved data"):
        _assert_absent(sentinels[0].encode("ascii"), sentinels)


def test_non_ending_and_over_limit_input_yields_only_public_view():
    from prismabuild import public_evidence

    record = {"host": "fleet-01", "transport": _transports()[0], "status": "superseded"}
    for value in (None, 1, True, "text", b"text", (), _Hostile(), _Dict(record),
                  _List([record]), [record], [[record]], {"unknown": record}):
        _assert_equal(_view(value), _public(value))
    at_limit = {f"ignored-{index}": None for index in range(public_evidence._MAX_ITEMS - 3)}
    at_limit.update(record)
    _assert_equal(_view(at_limit), {"schema": SCHEMA, "evidence": record})
    at_limit["one-too-many"] = None
    _assert_equal(_view(at_limit), _public(at_limit))
    for value in ("host", "transport", "status"):
        over = dict(at_limit)
        over[value] = record[value]
        _assert_equal(_view(over), {"schema": SCHEMA, "evidence": {}})
    cycle = {"status": "running"}
    cycle["children"] = [cycle]
    deep = {"status": "running"}
    for _ in range(public_evidence._MAX_DEPTH + 2):
        deep = {"children": [deep]}
    wide = {"children": [{"children": [{"returncode": 0}] * 8}] * 1024}
    for value in (cycle, deep, wide):
        _assert_equal(_view(value), _public(value))
        with_host = {**value, "host": "fleet-01"}
        expected = _public(with_host)
        expected["evidence"]["host"] = "fleet-01"
        _assert_equal(_view(with_host), expected)


def test_scope_shape_and_envelope_interpretations():
    root = {"host": "fleet-01", "transport": _transports()[0], "status": "superseded"}
    _assert_equal(_view({"host": "fleet-01"}), {"schema": SCHEMA, "evidence": {"host": "fleet-01"}})
    _assert_equal(_view(root), {"schema": SCHEMA, "evidence": root})
    nested = {"children": [root, {"status": "running", "host": "child-host"}], "result": root}
    _assert_equal(_view(nested), _public(nested))
    _assert_equal(_view([root]), _public([root]))
    # Interpretation: already-marked envelopes are public-only; no extra extraction.
    for value in ({"schema": SCHEMA, "evidence": root, **root},
                  {"schema": SCHEMA, "evidence": [root], **root},
                  {"schema": SCHEMA, **root},
                  {"schema": SCHEMA, "evidence": None, **root}):
        _assert_equal(_view(value), _public(value))
    _assert_equal(_public(_view(root)), _public(root))
    _assert_equal(_view(_view(root)), _public(root))
    # An unrecognized marker remains an ordinary record, not an envelope.
    _assert_equal(_view({"schema": "other", **root}), {"schema": SCHEMA, "evidence": root})
    for key in (_String("schema"), _LookalikeKey("schema")):
        _assert_equal(_view({key: SCHEMA, **root}), {"schema": SCHEMA, "evidence": root})


def test_operator_view_yields_no_field_policy_rows():
    _module()
    sys.path.insert(0, str(ROOT / "tests/compat/support"))
    from wire_inventory import field_policies

    _assert_equal(field_policies(ROOT, paths=["src/prismabuild/operator_view.py"]), [])


# Explicit conservative markers, not a classification of every possible I/O API.
# open/Path.open write modes are inspected below instead of flagging read-only open.
_WRITER_MARKERS = (
    "write_text", "write_bytes", "os.replace", "os.rename", "json.dump",
    "upload", "--json",
)
_WRITE_MODES = ("w", "a", "x", "+")


def _imports_operator_view(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name.split(".")[-1] == "operator_view" for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if (module.split(".")[-1] == "operator_view"
                    or any(alias.name == "operator_view" for alias in node.names)
                    or (module == "prismabuild" and any(alias.name == "*" for alias in node.names))):
                return True
        elif isinstance(node, ast.Call):
            # Also cover literal dynamic imports, including aliased import_module.
            if any(isinstance(arg, ast.Constant) and type(arg.value) is str
                   and arg.value.lstrip(".").split(".")[-1] == "operator_view"
                   for arg in node.args):
                return True
    return False


def _writer_marked(source, tree):
    if any(marker in source for marker in _WRITER_MARKERS):
        return True
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = (node.func.id if isinstance(node.func, ast.Name) else
                node.func.attr if isinstance(node.func, ast.Attribute) else "")
        if name != "open":
            continue
        modes = [kw.value for kw in node.keywords if kw.arg == "mode"]
        # Positional modes on open(path, mode) versus Path.open(mode). Do not
        # mistake a read-only path containing 'a'/'w' for a write mode.
        position = 0 if isinstance(node.func, ast.Attribute) else 1
        if len(node.args) > position:
            modes.append(node.args[position])
        if any(isinstance(mode, ast.Constant) and type(mode.value) is str
               and any(flag in mode.value for flag in _WRITE_MODES) for mode in modes):
            return True
    return False


def _scan_importers(root):
    importers, unsafe = [], []
    for directory in ("src/prismabuild", "tools/fleet"):
        for path in sorted((root / directory).rglob("*.py")):
            source = path.read_text()
            tree = ast.parse(source)
            if _imports_operator_view(tree):
                importers.append(path.relative_to(root).as_posix())
                if _writer_marked(source, tree):
                    unsafe.append(path.relative_to(root).as_posix())
    return importers, unsafe


def test_no_writer_imports_operator_view():
    assert (ROOT / "src/prismabuild/operator_view.py").is_file(), "operator view must exist"
    importers, unsafe = _scan_importers(ROOT)
    _assert_equal(unsafe, [])
    _assert_equal(importers, [])


def test_importer_detector_flags_marked_synthetic_importer(tmp_path):
    assert (ROOT / "src/prismabuild/operator_view.py").is_file(), "operator view must exist"
    imports = (
        "import prismabuild.operator_view as local",
        "from prismabuild.operator_view import operator_ending_view as local",
        "from prismabuild import operator_view as local",
        "from .operator_view import operator_ending_view",
        "from . import operator_view",
        "from prismabuild.operator_view import *",
        "local = __import__('prismabuild.operator_view')",
        "local = importlib.import_module('prismabuild.operator_view')",
    )
    operations = ("open('out', 'w')", "path.open(mode='a')", "path.write_text('x')",
                  "path.write_bytes(b'x')", "os.replace('a', 'b')", "json.dump({}, sink)",
                  "upload_artifact(data)", "parser.add_argument('--json')")
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    target = package / "fake.py"
    for statement in imports:
        for operation in operations:
            target.write_text(statement + "\n" + operation + "\n")
            _assert_equal(_scan_importers(tmp_path), (["src/prismabuild/fake.py"], ["src/prismabuild/fake.py"]))
    target.write_text("from prismabuild import operator_view\nopen('data', 'r')\n")
    _assert_equal(_scan_importers(tmp_path), (["src/prismabuild/fake.py"], []))
    target.write_text("import json\npath.write_text('x')\n")
    _assert_equal(_scan_importers(tmp_path), ([], []))


def test_docs_describe_operator_local_view():
    text = (ROOT / "docs/public_evidence.md").read_text()
    assert "## Operator-local view" in text
    section = text.split("## Operator-local view", 1)[1]
    for phrase in ("operator_ending_view", "host", "transport", "superseded", "fullmatch",
                   "^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$", "exact", "str", "interpretation",
                   "root", "built-in", "dict", "schema", "evidence", "envelope",
                   "public-only", "empty", "submission_types.SubmissionRequest.transport",
                   "stdlib", "authenticated operator", "fleet host", "never saved",
                   "uploaded", "published", "JSON", "public_evidence_view"):
        assert phrase in section, "operator-local documentation is incomplete"
