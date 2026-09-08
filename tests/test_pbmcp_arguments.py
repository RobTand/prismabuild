"""Advertised tool schemas must be enforced before dispatch or shared reads."""

import io
import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp
import pbmcp_fixture as fx


@pytest.mark.parametrize("name,arguments", [
    ("pb_action", {}),
    ("pb_runtime", {"unknown": True}),
    ("pb_log", {"key_prefix": fx.DONE_KEY, "tail_lines": -1}),
    ("pb_action", {"key_prefix": fx.DONE_KEY, "tail_lines": -1}),
    ("pb_log", {"key_prefix": fx.DONE_KEY, "tail_lines": True}),
    ("pb_log", {"key_prefix": fx.DONE_KEY, "tail_lines": 1.5}),
    ("pb_log", {"key_prefix": fx.DONE_KEY, "stream": "other"}),
    ("pb_log", {"key_prefix": fx.DONE_KEY, "attempt": 0}),
    ("pb_actions", {"tags": "x86"}),
    ("pb_actions", {"tags": [42]}),
    ("pb_actions", {"priority_min": "1"}),
    ("pb_actions", {"limit": -1}),
    ("pb_verify_claim", {"sha256": "a" * 64, "hash_payload": "false"}),
])
def test_bad_arguments_return_invalid_params_before_any_read(
    tmp_path, monkeypatch, name, arguments,
):
    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    reads = []

    def forbidden(*args, **kwargs):
        reads.append(args[0])
        raise AssertionError("invalid input reached the shared reader")

    monkeypatch.setattr(pbmcp.Call, "read", forbidden)
    response = pbmcp.Server(session).handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    })
    assert response.get("error", {}).get("code") == -32602, response
    assert reads == []


def test_revision_requiring_batch_receipt_is_not_negotiated(tmp_path):
    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    source = io.BytesIO((json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26"},
    }) + "\n").encode())
    target = io.BytesIO()
    assert pbmcp.Server(session, stdin=source, stdout=target).serve() == 0
    assert json.loads(target.getvalue())["result"]["protocolVersion"] == "2024-11-05"
