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


def main_definitions(path, names, **bindings):
    source = subprocess.run(["git", "show", f"{MAIN}:{path}"], cwd=ROOT,
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
        SimpleNamespace(item_path=lambda state, key: key))
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
