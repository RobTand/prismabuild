"""Preserve the main-branch byte recipes while consolidating digest sites."""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from prismabuild import checkout_recovery as recovery, core

MAIN = "4815485c5060f3cecbf42655a37c7585e14c1791"
ROOT = Path(__file__).resolve().parents[1]


def main_definitions(path, names, *, revision=MAIN, **bindings):
    source = subprocess.run(["git", "show", f"{revision}:{path}"], cwd=ROOT,
                            check=True, text=True, capture_output=True).stdout
    tree = ast.parse(source)
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
             and node.name in names]
    assert {node.name for node in nodes} == set(names)
    namespace = {"__name__": "main_recipe", "hashlib": hashlib, "json": json,
                 "Path": Path, **bindings}
    code = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(code), path, "exec"), namespace)
    return SimpleNamespace(**namespace)


def fleet_module(name):
    path = ROOT / "tools" / "fleet" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"digest_sites_{name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("payload", [pytest.param(b"", id="empty"), pytest.param(b"raw\x00\xff\n", id="raw"), pytest.param(b"native\x00payload" * 200000, id="multiple-chunks")])
def test_checkout_stream_digest_and_size_match_main(payload):
    old = main_definitions("src/prismabuild/checkout_recovery.py", ["_hash_stream"],
                           CHUNK_BYTES=recovery.CHUNK_BYTES)
    assert recovery._hash_stream(io.BytesIO(payload)) == old._hash_stream(io.BytesIO(payload))


def test_checkout_json_digest_hashes_exact_read_bytes(tmp_path):
    raw = b'{ "z": "caf\\u00e9", "a": [null, 1.25] }\n'
    path = tmp_path / "record.json"
    path.write_bytes(raw)
    old = main_definitions("src/prismabuild/checkout_recovery.py", ["_json"],
                           pb=core, MAX_JSON_BYTES=recovery.MAX_JSON_BYTES,
                           _refuse=lambda text: pytest.fail(text))
    assert recovery._json(path, "test record") == old._json(path, "test record")


@pytest.mark.parametrize("manifest", [{}, {"z": {"bytes": 7, "text": "λ"}, "a": {"items": [True, None, -0.0]}}])
def test_checkout_manifest_digest_matches_main_order_and_line_profile(manifest):
    old = main_definitions("src/prismabuild/checkout_recovery.py", ["_manifest_digest"], pb=core)
    assert recovery._manifest_digest(manifest) == old._manifest_digest(manifest)


def test_recovery_parser_refusal_bytes_match_main(capsys):
    import argparse
    module = fleet_module("pbrecover_checkout")
    old = main_definitions("tools/fleet/pbrecover_checkout.py", ["_JSONParser"],
                           argparse=argparse, recovery=recovery)
    with pytest.raises(SystemExit) as before:
        old._JSONParser().error("bad λ input")
    expected = capsys.readouterr().out
    with pytest.raises(SystemExit) as after:
        module._JSONParser().error("bad λ input")
    assert after.value.code == before.value.code == 2
    assert capsys.readouterr().out == expected


def test_recovery_main_result_bytes_and_nonfinite_refusal_match_main(monkeypatch, capsys):
    module = fleet_module("pbrecover_checkout")
    result = {"schema": recovery.RESULT_SCHEMA, "complete": False,
              "status": "refused", "errors": ["bad λ input"], "removed": []}
    monkeypatch.setattr(sys, "argv", ["pbrecover_checkout", "--queue-root", "/q",
        "--bank-root", "/b", "--maintenance-owner", "owner", "--candidates", "/c"])
    monkeypatch.setattr(module.recovery, "_directory", lambda *args: None)
    monkeypatch.setattr(module.pool, "PoolQueue", lambda path: None)
    monkeypatch.setattr(module, "_pbrecover_checkout_read", lambda path: [])
    monkeypatch.setattr(module.recovery, "prepare_checkout_recovery", lambda *args, **kwargs: result)
    old_recovery = SimpleNamespace(**vars(module.recovery))
    old_recovery._path = recovery._checkout_recovery_path
    old = main_definitions("tools/fleet/pbrecover_checkout.py", ["_JSONParser", "main"],
                           argparse=module.argparse, recovery=old_recovery, pool=module.pool,
                           pb=core, _read=module._pbrecover_checkout_read, __doc__=module.__doc__)
    assert old.main() == 2
    expected = capsys.readouterr().out
    assert module.main() == 2
    assert capsys.readouterr().out == expected
    result["extra"] = float("nan")
    for main_function in (old.main, module.main):
        with pytest.raises(ValueError):
            main_function()
        assert capsys.readouterr().out == ""


def test_gang_main_result_bytes_match_main(monkeypatch, capsys, tmp_path):
    module = fleet_module("pbgang")
    old = main_definitions("tools/fleet/pbgang.py", ["main"],
                           argparse=module.argparse, secrets=module.secrets, sys=sys,
                           subprocess=module.subprocess, pool=module.pool, _gang=module._gang,
                           SH=module.SH, SCHEMA=module.SCHEMA, __doc__=module.__doc__,
                           load=module._pbgang_load_manifest,
                           member_command=module.member_command, withdraw=module.withdraw)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"members": [{"tag": "one", "argv": ["/bin/true"]},
                                              {"tag": "two", "argv": ["/bin/true"]}]}))
    keys = iter(["a" * 64, "b" * 64])
    monkeypatch.setattr(module.secrets, "token_hex", lambda count: "c" * 32)
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs:
        SimpleNamespace(returncode=0, stderr="", stdout=json.dumps({"status": "submitted", "action_key": next(keys)})))
    monkeypatch.setattr(module.pool, "PoolQueue", lambda path:
        SimpleNamespace(item_path=lambda state, key: key,
                        residency_plan_path=lambda key: tmp_path / f"missing-{key}.json"))
    monkeypatch.setattr(module.pool, "_read_json", lambda path: {})
    monkeypatch.setattr(module._gang, "publish_group", lambda *args, **kwargs:
        {"skew_s": 1.25, "priority": 7})

    argv = ["--manifest", str(manifest), "--cwd", str(tmp_path)]
    assert old.main(argv) == 0
    expected = capsys.readouterr().out
    keys = iter(["a" * 64, "b" * 64])
    assert module.main(argv) == 0
    assert capsys.readouterr().out == expected


def test_rollout_private_ascii_line_profile_matches_main():
    module = fleet_module("qualify_rollout")
    old = main_definitions("tools/fleet/qualify_rollout.py", ["canonical"])
    for value in [{"z": "λ", "a": [1.25, None]}, {"x": float("nan")}, []]:
        assert module._qualify_rollout_canonical(value) == old.canonical(value)


RESIDENT_MAIN = "3916c1f621"


@pytest.mark.parametrize("size", [0, (8 << 20) - 1, 8 << 20, (8 << 20) + 1, (16 << 20) + 7])
def test_resident_copy_and_hash_match_main_at_native_block_boundaries(tmp_path, size):
    import os
    import stat
    from prismabuild import local_resident, digest_primitives, resident_sets

    old = main_definitions("src/prismabuild/local_resident.py", ["hash_file", "copy_file"],
        revision=RESIDENT_MAIN, os=os, stat=stat, resident_sets=resident_sets,
        BLOCK_BYTES=local_resident.BLOCK_BYTES)
    assert core.new_sha256 is digest_primitives.new_sha256 is hashlib.sha256
    payload = (b"resident\x00\xff\n" * (size // 11 + 1))[:size]
    source = tmp_path / "source"
    source.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    assert local_resident.hash_file(source) == old.hash_file(source) == (size, expected)
    entry = {"path": str(source), "bytes": size, "sha256": expected}
    before, after = tmp_path / "before", tmp_path / "after"
    assert old.copy_file(source, before, entry) == local_resident.copy_file(source, after, entry) == expected
    assert before.read_bytes() == after.read_bytes() == payload


def test_resident_copy_keeps_the_open_descriptor_and_fsync_order(tmp_path, monkeypatch):
    import os
    import stat
    from prismabuild import local_resident

    source, moved, destination = (tmp_path / name for name in ("source", "opened", "destination"))
    payload = b"held descriptor\x00\xff"
    source.write_bytes(payload)
    real_fdopen, real_fsync = os.fdopen, os.fsync
    events = []

    def swap_after_open(fd, mode, **kwargs):
        if mode == "rb":
            source.rename(moved)
            source.symlink_to(tmp_path / "missing")
        return real_fdopen(fd, mode, **kwargs)

    def sync(fd):
        events.append("file" if stat.S_ISREG(os.fstat(fd).st_mode) else "directory")
        return real_fsync(fd)

    monkeypatch.setattr(local_resident.os, "fdopen", swap_after_open)
    monkeypatch.setattr(local_resident.os, "fsync", sync)
    entry = {"path": str(source), "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
    assert local_resident.copy_file(source, destination, entry) == entry["sha256"]
    assert destination.read_bytes() == payload
    assert events == ["file", "directory"]
    with pytest.raises(OSError):
        local_resident.hash_file(source)
    with pytest.raises(OSError):
        local_resident.copy_file(source, tmp_path / "refused", entry)
    link = tmp_path / "destination-link"
    link.symlink_to(destination)
    with pytest.raises(OSError):
        local_resident.copy_file(moved, link, entry)
    assert destination.read_bytes() == payload


def test_resident_json_profiles_and_real_writer_match_main(tmp_path):
    import os
    import tempfile
    from prismabuild import resident_sets

    old = main_definitions("src/prismabuild/resident_sets.py", ["_json", "write_record", "fsync_directory"],
        revision=RESIDENT_MAIN, os=os, tempfile=tempfile)
    value = {"z": "模型 λ", "a": [-0.0, 1.25, True, None], "nested": {"b": "\n"}}
    assert core.compact_ascii_json_bytes(value) == old._json(value)
    assert core.compact_ascii_json_bytes(value) != core._canonical_bytes(value)
    before, after = tmp_path / "before.json", tmp_path / "after.json"
    old.write_record(before, value)
    resident_sets.write_record(after, value)
    assert before.read_bytes() == after.read_bytes() == old._json(value) + b"\n"
    for nonfinite in (float("nan"), float("inf"), -float("inf")):
        mixed = {"unicode": "λ", "number": nonfinite}
        assert core.sorted_json(mixed) == json.dumps(mixed, sort_keys=True)
        for writer, path in ((old.write_record, before), (resident_sets.write_record, after)):
            with pytest.raises(ValueError):
                writer(path, mixed)
            assert path.read_bytes() == old._json(value) + b"\n"
        with pytest.raises(ValueError):
            core.sorted_json(mixed, allow_nan=False)

