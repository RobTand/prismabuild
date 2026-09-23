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

import json

import pbtest_outcomes


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
        return ONE_PASS
    nodeids = [f"{file}::test_one" for file in files]
    record = pbtest_outcomes.PREFIX + json.dumps({
        "schema": pbtest_outcomes.SCHEMA, "collect_only": False,
        "collected": nodeids,
        "reports": [[nodeid, "call", "passed", None, None] for nodeid in nodeids],
        "uncounted": [],
    })
    return f"{record}\n{len(nodeids)} passed in 0.01s\n"
