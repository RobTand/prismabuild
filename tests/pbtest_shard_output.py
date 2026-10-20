"""What a real shard prints, for tests that stand in for pbrun.

Since #942 every shard runs pytest under ``pbtest_outcomes``' recorder, so a
real shard's output carries a ``pbtest-outcomes`` record beside its summary,
and since #941 pbtest reconciles the two by node ID.  A stand-in that prints
only ``1 passed in 0.01s`` is therefore a shard whose recorder printed
nothing, which pbtest reports as unreconciled.  Stand-ins that model a clean
shard print both, built here by the recorder's own schema so they cannot
drift from it.
"""
from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass
from pathlib import Path

import pbtest_outcomes
from prismabuild import action_client as client, action_wait, client_channel


@dataclass(frozen=True)
class ClientReply:
    output: str
    verdict: client.ClientVerdict
    identity: client.ClientIdentity | None = None
    readers: action_wait.ReaderHistory = action_wait.ReaderHistory()


def emit_client_events(process, kwargs, reply: ClientReply, *, expected_identity=None):
    writer = client_channel.ClientEventWriter(os.dup(kwargs['pass_fds'][0]),
        expected_identity=expected_identity)
    try:
        if reply.identity is not None and expected_identity is None:
            writer.send(reply.identity)
        writer.send(client.ClientEnding(reply.verdict, reply.readers))
    finally:
        writer.close()
    return process


def pool_identity(key: str, generation: float | None = 1234.5):
    return client.PoolClientIdentity(key, generation, Path('/configured/queue'),
        Path('/configured/cas'), None, client.ClientArrival.SUBMITTED)


def finish_submission(report: client.ClientReport, *, key='a' * 64, generation=1234.5,
                      transport='pool') -> int:
    identity = (pool_identity(key, generation) if transport == 'pool'
                else client.SlurmClientIdentity(key, generation, Path('/configured/queue'),
                    Path('/configured/cas'), None, '123', client.ClientArrival.SUBMITTED))
    report.publish_identity(identity)
    return report.finish(client.ClientSubmitted(identity))


def execution_reply(output: str, returncode: int, *, timed_out=False, key='a' * 64):
    identity = pool_identity(key)
    return ClientReply(output, client.ClientExecution(key, identity.published_unix,
        'timeout' if timed_out else 'executed' if returncode == 0 else 'failed',
        returncode, None, None, timed_out, returncode), identity)


def submission_command(command):
    """Keep logical arguments after the process's private descriptor closes."""
    words = list(command)
    end = words.index('--') if '--' in words else len(words)
    if '--client-events-fd' in words[:end]:
        position = words.index('--client-events-fd')
        del words[position:position + 2]
    return words


def shard_environment(command):
    """Read only the pbrun --env values before the command separator."""
    flags = list(command[:command.index("--")])
    return dict(flags[index + 1].split("=", 1)
                for index, flag in enumerate(flags) if flag == "--env")


def admitted_child(command, *, environment=None):
    """Execute a generated child inside a test's admitted action, with env."""
    child = command[command.index("--") + 1:]
    return child, {
        **(os.environ if environment is None else environment), **shard_environment(command)}


def bind_output(output: str, command) -> str:
    """Migrate a historical stand-in to the binding of its submitted command.

    Only test transports use this: real payloads emit their own v2 records.
    """
    binding = pbtest_outcomes.binding_from_argv(list(command))
    if binding is None:
        return output
    lines = []
    for line in output.splitlines(keepends=True):
        for prefix in (pbtest_outcomes.PREFIX, pbtest_outcomes.COMPLETION_PREFIX):
            if line.startswith(prefix):
                record = json.loads(line[len(prefix):])
                record["schema"] = record["schema"].removesuffix("v1") + "v2"
                record["record_binding"] = binding
                line = prefix + json.dumps(record) + "\n"
                break
        lines.append(line)
    return "".join(lines)


def shard_output(passed: int = 1, skipped: int = 0, *, file: str = "tests/test_one.py",
                 duration: str = "0.01s", prefix: str = "") -> str:
    """A shard's stdout: ``prefix``, then a record that reconciles, then the summary."""

    nodeids = [f"{file}::test_{index}" for index in range(passed + skipped)]
    if passed == 1 and skipped == 0:
        nodeids = [f"{file}::test_one"]
    reports = [[nodeid, "call", "passed", None, None] for nodeid in nodeids[:passed]]
    reports += [[nodeid, "setup", "skipped", "fixture reason", f"{file}:1"]
                for nodeid in nodeids[passed:]]
    record = pbtest_outcomes.PREFIX + json.dumps({
        "schema": pbtest_outcomes.SCHEMA, "collect_only": False,
        "collected": nodeids, "reports": reports, "uncounted": [],
    })
    parts = [f"{passed} passed"] + ([f"{skipped} skipped"] if skipped else [])
    return f"{prefix}{record}\n{', '.join(parts)} in {duration}\n"


#: The one clean shard most stand-ins model: ``tests/test_one.py::test_one``.
ONE_PASS = shard_output()


def shard_output_for(command) -> str:
    """The clean output of the shard ``command`` submits: one test per file it runs.

    A shard's files close its argv, so each stand-in shard names only its
    own tests.  Two stand-in shards that printed the same record would
    collect the same node ID, which pbtest fails as a test both ran.
    """

    payload = list(command)[list(command).index("--") + 1:] if "--" in command else []
    files = [part for part in payload if part.endswith(".py") and "\n" not in part]
    if not files:
        return bind_output(ONE_PASS, command)
    nodeids = [f"{file}::test_one" for file in files]
    record = pbtest_outcomes.PREFIX + json.dumps({
        "schema": pbtest_outcomes.SCHEMA, "collect_only": False,
        "collected": nodeids,
        "reports": [[nodeid, "call", "passed", None, None] for nodeid in nodeids],
        "uncounted": [],
    })
    return bind_output(f"{record}\n{len(nodeids)} passed in 0.01s\n", command)


class ShardProcess:
    """A finished stand-in shard, readable the way ``pbtest`` reads a real one.

    ``pbtest`` streams each shard's stdout line by line while the shard runs,
    so a long ``pbrun`` wait is not silent (#1048), and then waits for its
    exit. A stand-in defines ``communicate`` and ``returncode``; this answers
    ``stdout`` and ``wait`` from them.
    """

    @property
    def stdout(self):
        stream = self.__dict__.get("_stdout")
        if stream is None:
            stream = self.__dict__["_stdout"] = io.StringIO(self.communicate()[0] or "")
        return stream

    def wait(self, timeout=None):
        return self.returncode
