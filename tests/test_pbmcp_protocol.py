"""``pbmcp`` speaks the MCP subset a client's handshake actually performs.

Driven over real pipes against the real entry point, because the failures
this has to catch are failures of the process boundary: a stray ``print``
into the transport, a response to a notification, a frame the client cannot
split.  A test that imported the module and called ``handle`` would prove
none of them.

The handshake modelled here is the one Claude Code and the opencode/Codex
clients perform: ``initialize``, the ``notifications/initialized`` that must
NOT be answered, then ``tools/list`` and probes at ``resources/list`` and
``prompts/list`` that a server which errors on them looks broken for.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp_fixture as fx  # noqa: E402

TOOL = REPOSITORY / "tools" / "fleet" / "pbmcp.py"
#: Every tool the issue's contract names.  Listed here rather than imported
#: so that dropping one from the module is a failure rather than a rename.
EXPECTED_TOOLS = ("pb_status", "pb_action", "pb_actions", "pb_verify_claim",
                  "pb_log", "pb_runtime")


class Client:
    """A minimal MCP client: one JSON object per line, in and out."""

    def __init__(self, process: subprocess.Popen) -> None:
        self.process = process
        self.next_id = 0

    def send(self, method: str, params: dict | None = None,
             *, notification: bool = False) -> dict | None:
        message: dict = {"jsonrpc": "2.0", "method": method,
                         "params": params if params is not None else {}}
        if not notification:
            self.next_id += 1
            message["id"] = self.next_id
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()
        if notification:
            return None
        return self.read()

    def send_raw(self, text: str) -> dict:
        assert self.process.stdin is not None
        self.process.stdin.write(text + "\n")
        self.process.stdin.flush()
        return self.read()

    def read(self) -> dict:
        assert self.process.stdout is not None
        line = self.process.stdout.readline()
        assert line, "the server closed the transport"
        return json.loads(line)

    def call(self, name: str, arguments: dict | None = None) -> dict:
        return self.send("tools/call", {"name": name,
                                        "arguments": arguments or {}})


def payload_of(response: dict) -> dict:
    content = response["result"]["content"]
    assert content[0]["type"] == "text"
    return json.loads(content[0]["text"])


@pytest.fixture()
def client(tmp_path: Path):
    fleet = fx.build(tmp_path)
    process = subprocess.Popen(
        [sys.executable, str(TOOL),
         "--queue-root", str(fleet.queue_root),
         "--cas-root", str(fleet.cas_root),
         "--repo-link", str(fleet.repo_link)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1)
    try:
        yield Client(process), fleet
    finally:
        if process.stdin is not None:
            process.stdin.close()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - a hung server
            process.kill()
            process.wait(timeout=10)


def test_the_handshake_completes_and_echoes_an_agreed_revision(client) -> None:
    session, _fleet = client
    response = session.send("initialize", {
        "protocolVersion": "2025-06-18", "capabilities": {},
        "clientInfo": {"name": "test", "version": "1"}})
    result = response["result"]
    assert response["jsonrpc"] == "2.0" and response["id"] == 1
    assert result["protocolVersion"] == "2025-06-18", (
        "a server that answers with its own favourite revision tells the "
        "client nothing about whether they agree")
    assert result["capabilities"]["tools"] == {}
    assert result["serverInfo"]["name"] == "prismabuild"
    assert "pbrun" in result["instructions"], (
        "the instructions have to send an agent that wants to submit to the "
        "tool that gates submission")


def test_an_unknown_revision_falls_back_rather_than_refusing(client) -> None:
    session, _fleet = client
    response = session.send("initialize", {"protocolVersion": "1999-01-01"})
    assert response["result"]["protocolVersion"] == "2024-11-05"


def test_a_notification_is_not_answered(client) -> None:
    """Answering ``notifications/initialized`` breaks the client's handshake."""

    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    session.send("notifications/initialized", notification=True)
    # The very next line on the transport must be the ping's reply, not a
    # response or an error for the notification.
    reply = session.send("ping")
    assert reply["id"] == 2 and reply["result"] == {}


def test_tools_list_declares_every_tool_with_help_a_model_can_use(client) -> None:
    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    tools = session.send("tools/list")["result"]["tools"]
    assert tuple(tool["name"] for tool in tools) == EXPECTED_TOOLS
    for tool in tools:
        assert tool["description"].strip(), tool["name"]
        schema = tool["inputSchema"]
        assert schema["type"] == "object"
        for name, declared in (schema.get("properties") or {}).items():
            assert declared.get("description"), f"{tool['name']}.{name}"


def test_the_probes_a_client_makes_are_answered_empty_not_refused(client) -> None:
    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    assert session.send("resources/list")["result"] == {"resources": []}
    assert session.send("prompts/list")["result"] == {"prompts": []}


def test_a_tool_call_returns_json_text_content(client) -> None:
    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    body = payload_of(session.call("pb_status"))
    assert body["schema"] == "prismaquant.prismabuild.pbmcp.v1"
    assert body["tool"] == "pb_status"
    for field in ("generated_unix", "complete", "timed_out", "generation",
                  "generation_stale", "started_from_generation", "deadline_s"):
        assert field in body, field
    assert body["complete"] is True and body["timed_out"] == []


def test_an_unknown_method_is_a_protocol_error(client) -> None:
    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    response = session.send("does/not/exist")
    assert response["error"]["code"] == -32601


def test_malformed_arguments_are_a_protocol_error(client) -> None:
    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    response = session.send("tools/call", {"name": "pb_status",
                                           "arguments": "not an object"})
    assert response["error"]["code"] == -32602


def test_a_tool_that_cannot_answer_is_a_result_not_a_protocol_error(client) -> None:
    """A model can act on ``isError``; it cannot act on a transport failure."""

    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    response = session.call("pb_action", {"key_prefix": "ffffffff"})
    assert "error" not in response
    assert response["result"]["isError"] is True
    assert "no action" in payload_of(response)["error"]


def test_an_unknown_tool_names_the_tools_there_are(client) -> None:
    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    response = session.call("pb_nonsense")
    assert response["result"]["isError"] is True
    assert set(payload_of(response)["tools"]) == set(EXPECTED_TOOLS)


def test_a_broken_frame_does_not_end_the_session(client) -> None:
    session, _fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    broken = session.send_raw("{not json")
    assert broken["error"]["code"] == -32700
    assert session.send("ping")["result"] == {}


def test_nothing_but_json_rpc_reaches_the_transport(client) -> None:
    """One stray ``print`` in a reader would desynchronise every client."""

    session, fleet = client
    session.send("initialize", {"protocolVersion": "2024-11-05"})
    for name, arguments in (
        ("pb_status", {}),
        ("pb_action", {"key_prefix": fx.DONE_KEY[:12]}),
        ("pb_actions", {"limit": 10}),
        ("pb_log", {"key_prefix": fx.DONE_KEY[:12]}),
        ("pb_runtime", {}),
        ("pb_verify_claim", {"sha256": fx.claim_digest(fleet)}),
    ):
        response = session.call(name, arguments)
        assert response["id"] == session.next_id, name
        assert payload_of(response)["tool"] == name


@pytest.mark.skipif(not Path("/usr/bin/python3").exists(),
                    reason="no system interpreter on this box")
def test_it_answers_under_the_system_interpreter(tmp_path: Path) -> None:
    """The dependency claim, checked rather than asserted in a docstring.

    The server is launched as ``/usr/bin/python3
    /mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py`` from boxes that
    have no venv and no ``mcp`` SDK.  Stdlib-only is what makes that work, and
    the way to keep it true is to run it that way.
    """

    fleet = fx.build(tmp_path)
    process = subprocess.Popen(
        ["/usr/bin/python3", str(TOOL),
         "--queue-root", str(fleet.queue_root),
         "--cas-root", str(fleet.cas_root),
         "--repo-link", str(fleet.repo_link)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1)
    session = Client(process)
    try:
        assert session.send("initialize", {"protocolVersion": "2024-11-05"})[
            "result"]["serverInfo"]["name"] == "prismabuild"
        assert payload_of(session.call("pb_status"))["complete"] is True
    finally:
        assert process.stdin is not None
        process.stdin.close()
        process.wait(timeout=30)
    assert process.stderr is not None
    assert process.stderr.read() == "", "the server wrote to stderr"
