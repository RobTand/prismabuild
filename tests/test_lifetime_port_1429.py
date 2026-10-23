"""Seal-time refusal for an unreadable all-phase lifetime contract (#1429)."""
import sys

import pytest

from prismabuild import core as pb


def test_invalid_lifetime_cannot_be_sealed_as_an_ordinary_parameter(tmp_path):
    (tmp_path / "task.py").write_text("print('ok')\n")
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/lifetime-port", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(tmp_path, ["task.py"]),
        "params": {"lifetime": {"schema": "prismabuild.action_lifetime.v1", "fence_s": 1}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    with pytest.raises(pb.ActionContractError, match="fence_s"):
        pb.seal_action(body)
