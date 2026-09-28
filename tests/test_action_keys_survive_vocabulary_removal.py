"""Existing action keys survive the core vocabulary removal (#1076).

Core used to close two vocabularies: ``artifact_family`` had to be
``generic`` or ``codebook``, and a nonportable toolchain could name only
``torch``, ``transformers``, ``vllm`` and ``gridbook`` as distributions.
Both are now submitter-declared, and both fields are inside the hashed
action body. Widening what core accepts must not move a key, and every
action already in the CAS must still validate to the key it was published
under.

Two kinds of evidence, because the real records alone do not reach the
branches that changed:

- ``fixtures/recorded_actions/`` holds request records copied read-only
  from the fleet CAS on 2026-09-27, named by their action keys. They cover
  portable, platform-keyed measurement, container, data-manifest and
  argv0-bound actions. Each must validate, and recompute to its file name.
- ``GOLDEN`` holds keys sealed by origin/main ``81035feb5320``, before the
  change, for bodies that exercise the removed vocabulary: a ``codebook``
  family, and toolchains that declare the four old distribution names.
  The same bodies must seal to the same keys now.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402

RECORDED = Path(__file__).with_name("fixtures") / "recorded_actions"

#: Sealed by ``src/prismabuild/core.py`` at 81035feb5320, before #1076.
GOLDEN = {
    "generic_portable":
        "dad45861b0c22b41937706bd6590838f65e107c563c8a12bb951086b354dc3b7",
    "codebook_platform_keyed":
        "d2c677db19f4a10e6997979207694f96c9650663daf8621f782a919ca9bb642a",
    "codebook_host_class_measurement":
        "f349f4df50be8216b74c4986c8e5048f7a7620a852966c46101def6d07b9d0ac",
    "distributions_platform_keyed":
        "cec8eaa4428df9ed428a368c1d7f659c4d764bd56dcfafcfb04a6e34c007ddd7",
    "distributions_portable":
        "e24c6919be2b139bbbc2d70bd51dbf363a0711a020be1e93f4e1e1f4a6fefc49",
}

_PLATFORM = {"argv0.sha256": "4" * 64, "argv0.bytes": "4096", "system": "Linux",
             "machine": "aarch64", "libc": "glibc-2.39"}


def _body(family: str, portability: str, toolchain: dict[str, str],
          task_class: str = "generation") -> dict[str, object]:
    closure = {"schema": pb.CODE_CLOSURE_SCHEMA_V1,
               "files": [{"path": "task_code.py", "sha256": "1" * 64, "bytes": 17}]}
    return {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/legacy-vocabulary", "definition_version": "v1",
                 "task_class": task_class, "determinism": "deterministic",
                 "artifact_family": family, "artifact_kind": "fp8-lut",
                 "argv": ["/usr/bin/python3", "-c", "pass"],
                 "working_directory": ".", "result_path": "result.bin"},
        "inputs": [{"id": "model/weights", "sha256": "3" * 64, "bytes": 30}],
        "code_closure": {**closure, "closure_sha256": pb.canonical_sha256(closure)},
        "params": {"alpha": 1},
        "environment": {"variables": {"DECLARED": "yes"}, "toolchain": toolchain},
        "execution_scope": {
            "portability": portability,
            "platform_key": "linux-aarch64-sm121" if portability == "platform_keyed" else None,
            "host_class": "gb10" if portability == "host_class_keyed" else None},
    }


BODIES = {
    "generic_portable": lambda: _body("generic", "portable", {}),
    "codebook_platform_keyed": lambda: _body("codebook", "platform_keyed", dict(_PLATFORM)),
    "codebook_host_class_measurement": lambda: _body(
        "codebook", "host_class_keyed", dict(_PLATFORM), "measurement"),
    "distributions_platform_keyed": lambda: _body("generic", "platform_keyed", {
        **_PLATFORM, "python": "3.12", "torch": "2.11", "transformers": "5.5",
        "vllm": "0.20", "gridbook": "0.4"}),
    "distributions_portable": lambda: _body(
        "generic", "portable", {"torch": "2.11", "gridbook": "0.4"}),
}


def _recorded() -> list[Path]:
    return sorted(RECORDED.glob("*.json"))


def test_the_sample_covers_more_than_one_scope():
    scopes = {json.loads(p.read_text())["execution_scope"]["portability"]
              for p in _recorded()}
    assert len(_recorded()) >= 8
    assert {"portable", "platform_keyed"} <= scopes


@pytest.mark.parametrize("path", _recorded(), ids=lambda p: p.stem[:12])
def test_a_recorded_action_validates_to_its_published_key(path: Path):
    record = json.loads(path.read_text(encoding="utf-8"))
    assert pb.validate_action(record)["action_key"] == path.stem
    body = {key: value for key, value in record.items() if key != "action_key"}
    assert pb.seal_action(body)["action_key"] == path.stem


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_a_body_using_the_removed_vocabulary_keeps_its_key(name: str):
    sealed = pb.seal_action(BODIES[name]())
    assert sealed["action_key"] == GOLDEN[name]
    assert pb.validate_action(sealed)["action_key"] == GOLDEN[name]
