"""Run the original generic methods and their current defaults on real tokens."""
from __future__ import annotations

import ast
import errno
import json
import os
from pathlib import Path
import subprocess

import pytest

from prismabuild import pool


BASE = "578e54d0788c0f3b138f5dcf217e980322bb7726"
OWNER, TARGET = "a" * 64, "b" * 64


@pytest.fixture(scope="module")
def original_methods():
    root = Path(__file__).resolve().parents[1]
    source = subprocess.check_output(
        ["git", "show", f"{BASE}:src/prismabuild/pool.py"], cwd=root, text=True)
    ledger = next(node for node in ast.parse(source).body
                  if isinstance(node, ast.ClassDef) and node.name == "ResourceLedger")
    methods = {}
    for name in ("commit_acquire", "transfer_tokens"):
        method = next(node for node in ledger.body
                      if isinstance(node, ast.FunctionDef) and node.name == name)
        namespace = dict(vars(pool))
        exec(compile(ast.Module(body=[method], type_ignores=[]),
                     f"{BASE}:src/prismabuild/pool.py", "exec"), namespace)
        methods[name] = namespace[name]
    return methods


def make_ledger(root, purpose):
    queue = pool.PoolQueue(root)
    queue.records.ensure_layout()
    # This file compares the generic mechanism itself, including both guards.
    return (queue.ledger("collector-host") if purpose == "host"
            else queue.tier("prismabuild-stage:collector")._ledger)


def capture(root, ledger, method, args, monkeypatch, *, fault_at=None):
    trace = []
    fault = OSError(errno.EIO, "collector default fault")

    def relative(value):
        if isinstance(value, (str, Path)):
            text = os.fspath(value).replace(os.fspath(root), "ROOT")
            return text.replace(args[1], "HANDLE") if method.__name__ == "commit_acquire" else text
        return value

    def wrapped(name, real):
        def call(*positional, **keywords):
            trace.append((name, tuple(relative(v) for v in positional),
                          tuple((k, relative(v)) for k, v in sorted(keywords.items()))))
            if fault_at is not None and fault_at(name, positional):
                raise fault
            return real(*positional, **keywords)
        return call

    result, failure = None, None
    with monkeypatch.context() as patch:
        for name in ("exists", "is_dir", "is_file", "mkdir", "rmdir", "stat", "open"):
            patch.setattr(Path, name, wrapped(name, getattr(Path, name)))
        patch.setattr(os, "rename", wrapped("rename", os.rename))
        patch.setattr(pool.immutable_files, "scan_optional",
                      wrapped("scan_optional", pool.immutable_files.scan_optional))
        patch.setattr(pool.record_store, "read_document",
                      wrapped("read_document", pool.record_store.read_document))
        try:
            result = method(ledger, *args)
        except Exception as exc:
            failure = (type(exc), str(exc), exc.errno if isinstance(exc, OSError) else None,
                       exc is fault, type(exc.__cause__) if exc.__cause__ else None)
    state = [(relative(path.relative_to(root)),
              None if path.is_dir() else path.read_bytes())
             for path in sorted(root.rglob("*"))]
    return result, failure, trace, state


@pytest.mark.parametrize("purpose", ["host", "tier"])
@pytest.mark.parametrize("case", ["normal", "collision", "rename-failure", "missing-source",
                                 "mkdir-failure", "rmdir-failure", "metadata-unreadable"])
def test_commit_default_matches_original_effect_and_read_order(
        tmp_path, monkeypatch, original_methods, purpose, case):
    outcomes = []
    for variant, method in (("original", original_methods["commit_acquire"]),
                            ("current", pool.ResourceLedger.commit_acquire)):
        root = tmp_path / variant
        ledger = make_ledger(root, purpose)
        ledger.ensure_capacity({"cpu": 5})
        handle = ledger.begin_acquire(OWNER, {"cpu": 2})
        assert handle is not None
        source, target = ledger.held_dir / handle, ledger.held_dir / OWNER
        target.mkdir()
        if case == "collision":
            token = next(source.iterdir())
            (target / token.name).write_bytes(token.read_bytes())
        if case == "missing-source":
            ledger.abandon_acquire(handle)
        else:
            (source / pool.cpu_admission.METADATA).write_text(json.dumps({"borrowed_cpu": 2}))
            (source / pool.gpu_admission.METADATA).write_text(json.dumps({"borrowed_gpu": 3}))
        blocked = {"rename-failure": "rename", "mkdir-failure": "mkdir",
                   "rmdir-failure": "rmdir", "metadata-unreadable": "read_document"}.get(case)

        def fault_at(name, values):
            if name != blocked:
                return False
            path = Path(values[0])
            return (path.parent == source and path.name.startswith("cpu-") if name == "rename"
                    else path == target if name == "mkdir"
                    else path == source if name == "rmdir"
                    else path == target / pool.cpu_admission.METADATA)

        outcomes.append(capture(root, ledger, method, (OWNER, handle), monkeypatch,
                                fault_at=fault_at))
    assert outcomes[0] == outcomes[1]
    if case in {"mkdir-failure", "metadata-unreadable"}:
        assert outcomes[0][1][3] is True
    else:
        assert outcomes[0][1] is None


@pytest.mark.parametrize("purpose", ["host", "tier"])
@pytest.mark.parametrize("case", ["normal", "already-moved", "collision", "missing-both",
                                 "rename-failure", "private-endpoint", "duplicate-name"])
def test_named_transfer_default_matches_original_effect_and_read_order(
        tmp_path, monkeypatch, original_methods, purpose, case):
    outcomes = []
    for variant, method in (("original", original_methods["transfer_tokens"]),
                            ("current", pool.ResourceLedger.transfer_tokens)):
        root = tmp_path / variant
        ledger = make_ledger(root, purpose)
        ledger.ensure_capacity({"cpu": 5})
        assert ledger.acquire(OWNER, {"cpu": 2})
        source, target = ledger.held_dir / OWNER, ledger.held_dir / TARGET
        names = sorted(path.name for path in source.iterdir())
        target.mkdir()
        if case == "already-moved":
            for name in names:
                os.rename(source / name, target / name)
        elif case == "collision":
            (target / names[0]).write_bytes((source / names[0]).read_bytes())
        elif case == "missing-both":
            (source / names[0]).unlink()
        elif case == "duplicate-name":
            names.append(names[0])
        from_key = "claiming.private" if case == "private-endpoint" else OWNER

        def fault_at(name, values):
            return (case == "rename-failure" and name == "rename"
                    and Path(values[0]) == source / names[0])

        outcomes.append(capture(root, ledger, method, (from_key, TARGET, names), monkeypatch,
                                fault_at=fault_at))
    assert outcomes[0] == outcomes[1]
    if case in {"private-endpoint", "duplicate-name"}:
        assert outcomes[0][1][0] is pool.PoolContractError
    else:
        assert outcomes[0][1] is None


@pytest.mark.parametrize("purpose", ["host", "tier"])
def test_collectors_name_only_actual_token_renames(tmp_path, purpose):
    ledger = make_ledger(tmp_path / "queue", purpose)
    ledger.ensure_capacity({"cpu": 3})
    handle = ledger.begin_acquire(OWNER, {"cpu": 2})
    source = ledger.held_dir / handle
    names = sorted(path.name for path in source.iterdir())
    (source / pool.cpu_admission.METADATA).write_text(json.dumps({"borrowed_cpu": 3}))
    committed = []
    assert ledger.commit_acquire(OWNER, handle, moved_tokens=committed) == 5
    assert sorted(committed) == names
    moved = []
    assert ledger.transfer_tokens(OWNER, TARGET, names, moved_tokens=moved) == 2
    assert sorted(moved) == names
    retried = []
    assert ledger.transfer_tokens(OWNER, TARGET, names, moved_tokens=retried) == 2
    assert retried == []
