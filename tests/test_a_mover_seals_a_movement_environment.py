"""A mover is sealed with a movement environment, not its consumer's (#996).

A mover used to seal its consumer's whole environment minus the two
container-owner variables, and its argv exported the consumer's ``PATH`` head
on the stage host.  So a Stage A row sealed on a GB10 carried the Spark's venv
``PATH`` head, its thread caps, its spool root and pacing opt-ins and its
``PRISMAQUANT_*`` reader settings into an x86 mover on dl380g10.  Inert while
``movement_tools`` names the interpreter off the tier record; not inert for a
mover that shells out, which would find the Spark's runtime first.  And the
consumer's environment was part of every mover's key, so an environment-only
resubmission of the consumer re-keyed its movers as well.

A mover's environment is now its own: the tier interpreter's directory at the
head of a system ``PATH``, a fixed locale, and the container-owner pair
derived for the mover.  The pool and CAS roots it works on are sealed on its
command, where they always were.

Fixture concessions: the tier is announced by a real ``tier_loop.cycle`` and
the rows are sealed by the real ``pbrun.residency_stage_rows``, exactly as
``test_a_measurement_consumers_movers_are_ordinary_movement_work`` does; the
sealed requests are captured rather than published to a CAS.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import movement_actions, pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
CONSUMER = "c" * 64
PHASE_BYTES = 2 * storage_tiers.GIB
#: The Spark's venv, leading the consumer's PATH: nothing on dl380g10.
FOREIGN_HEAD = "/home/rob/venvs/gb10-cu130/bin"
CONSUMER_VARIABLES = {
    "PATH": f"{FOREIGN_HEAD}:/opt/prismabuild/generation/tools:/usr/bin:/bin",
    "PRISMAQUANT_X": "1",
    "OMP_NUM_THREADS": "20",
    "PRISMABUILD_PRODUCED_SPOOL_ROOT": "/home/rob/spool",
    "HOME": "/home/rob",
}


class _Cas:
    def __init__(self, manifest_path: Path) -> None:
        self._manifest = manifest_path
        self.actions: dict[str, dict] = {}

    def input_path(self, entry):
        return self._manifest

    def publish_action_request(self, action) -> None:
        self.actions[str(action["action_key"])] = dict(action)


def _template(digest: str, size: int, variables: dict[str, str]) -> dict[str, object]:
    import pbrun

    return {
        "cas": None, "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "log_name": "x.log", "stamp_name": "pbrun.stamp",
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": "b" * 64,
                    "bytes": 4096}],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"command": ["python", "stage_a.py"], "cwd": "/home/rob",
                   "demand": {"cpu": 1},
                   "placement": {"required_tags": ["sparky"]},
                   "checkout_snapshot": {
                       "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                       "commit": "a" * 40, "subdirectory": ".",
                       "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                                 "sha256": "b" * 64, "bytes": 4096}},
                   "retry_policy": {"max_attempts": 1, "retry_safe": False},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": dict(variables), "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }


def _seal(tmp_path: Path, variables: dict[str, str]):
    """Seal one consumer's stage rows the way ``pbrun`` does."""

    import pbrun

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {},
        "annotations": {"phases": [{"name": "phase-0", "bytes": PHASE_BYTES,
                                    "cumulative_bytes": PHASE_BYTES}]},
        "mount_prefix": "/mnt/shared",
        "entries": [{"path": "/mnt/shared/part-0", "offset": 0,
                     "bytes": PHASE_BYTES, "sha256": None}],
        "entry_count": 1, "total_bytes": PHASE_BYTES}))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": 8 * PHASE_BYTES}}

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = pbrun.resolve_stage_tier(queue, None)
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=4, residency_mover_max_attempts=3)
    cas = _Cas(manifest_path)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size, variables),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue,
        cas=cas)
    phase = staged["plan"]["phases"][0]
    return tier, {"mover": cas.actions[str(phase["mover_row"]["action_key"])],
                  "egress": cas.actions[str(phase["egress_row"]["action_key"])]}


@pytest.mark.parametrize("node", ["mover", "egress"])
def test_a_movement_node_carries_none_of_its_consumers_environment(
        tmp_path, node: str) -> None:
    """The acceptance fixture: ``PRISMAQUANT_X=1`` and a foreign PATH head in,
    neither out -- in the sealed variables or in the argv."""

    tier, bodies = _seal(tmp_path, CONSUMER_VARIABLES)
    body = bodies[node]
    variables = body["environment"]["variables"]
    assert "PRISMAQUANT_X" not in variables
    assert FOREIGN_HEAD not in variables["PATH"].split(":")
    assert FOREIGN_HEAD not in " ".join(body["task"]["argv"])
    # Nothing else of the consumer's either: its thread caps, spool root and
    # home are the consumer's settings, not the stage host's.
    for name in ("OMP_NUM_THREADS", "PRISMABUILD_PRODUCED_SPOOL_ROOT", "HOME"):
        assert name not in variables, name
    # What it does carry: the tier interpreter first, then the system.
    interpreter = str(tier["mover_python"])
    assert body["params"]["command"][0] == interpreter
    assert variables["PATH"].split(":")[0] == str(Path(interpreter).parent)
    assert set(variables) == {"PATH", "LANG", "LC_ALL",
                              pool.CONTAINER_OWNER_ENV, pool.CONTAINER_MARKER_ENV}
    # The pool and CAS roots are the command's, as they always were.
    assert "--pool-root" in body["params"]["command"]


def _shared_template(variables: dict[str, str]) -> dict[str, object]:
    import pbrun

    return {
        "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"cwd": "/home/rob",
                   "retry_policy": {"max_attempts": 1, "retry_safe": False}},
        "environment": {"variables": dict(variables), "toolchain": {}},
    }


def _shared_seal(variables: dict[str, str]) -> dict[str, object]:
    return movement_actions.seal_movement_action(
        _shared_template(variables),
        command=["/opt/mover-venv/bin/python", "/opt/tools/stage_move.py",
                 "--pool-root", "/pool", "--cas-root", "/cas"],
        demand={"cpu": 1, "mem_gb": 1}, tags=["dl380g10"], log_name="m.log",
        retry_policy={"max_attempts": 3, "retry_safe": True})


def test_the_shared_construction_seals_the_movement_environment() -> None:
    """``seal_movement_action`` itself, which the produced-output lane and the
    spool export call as well as pbrun."""

    body = _shared_seal(CONSUMER_VARIABLES)
    variables = body["environment"]["variables"]
    assert variables["PATH"] == "/opt/mover-venv/bin:/usr/local/bin:/usr/bin:/bin"
    assert "PRISMAQUANT_X" not in variables
    assert FOREIGN_HEAD not in " ".join(body["task"]["argv"])


def test_a_movers_key_does_not_read_its_consumers_environment() -> None:
    """Two templates that differ only in their environment seal one mover.

    Held to the shared construction, because pbrun freezes a consumer's plan
    and a second seal of one consumer reuses it.  The consumer key itself is
    on a stage mover's command and the consumer's environment is in *that*
    key, so this proves the mover's own identity, not adoption across a
    re-keyed consumer.
    """

    first = _shared_seal(CONSUMER_VARIABLES)
    second = _shared_seal({**CONSUMER_VARIABLES, "PATH": "/usr/bin:/bin",
                           "PRISMAQUANT_X": "2", "OMP_NUM_THREADS": "8"})
    assert first["action_key"] == second["action_key"]
