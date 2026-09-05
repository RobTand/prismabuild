"""A sealed snapshot is proved on the node whatever definition carries it.

Issue #57: "Non-pbrun definitions skip the clean-tree and ancestry proof
(`core.py:1856`); `fleet_submit.py:217` seals snapshots for `tessera/*`."

``_verify_pbrun_checkout_identity`` returned as its first statement whenever
``task.definition_id`` was not ``fleet/pbrun``.  Two different proofs sat
behind that one guard.  The closure-stamp proof does belong to ``fleet/pbrun``,
which is the only definition that writes a stamp.  The snapshot proof does
not: it belongs to ``params.checkout_snapshot``, which promises the worker a
clean tree at one sealed commit carrying its sealed parent.

``fleet_submit.seal_checkout_snapshot`` seals exactly that record for the
Tessera dispatchers on the SLURM lane, so a node running one of 120 export
shards checked nothing about the tree it was about to execute in.  A
materialized tree that had been written into, or was never the sealed commit
at all, was accepted.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import materialize  # noqa: E402

import fleet_submit  # noqa: E402


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True, text=True, check=True,
    ).stdout


def _producer_checkout(tmp_path: Path) -> Path:
    """A Git worktree with no pbrun stamp, which is what a producer has."""

    root = tmp_path / "producer-checkout"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.name", "PrismaBuild test")
    _git(root, "config", "user.email", "t@example.invalid")
    (root / "task.py").write_text(
        "import pathlib\n"
        "pathlib.Path('shard.json').write_text('sealed')\n",
        encoding="utf-8",
    )
    # A tracked file the closure does not pin, so a test that writes into
    # the materialized tree is refused by the snapshot proof rather than by
    # the closure check that runs before it.
    (root / "payload.txt").write_text("sealed by a producer\n", encoding="utf-8")
    _git(root, "add", "task.py", "payload.txt")
    _git(root, "commit", "-qm", "producer source")
    # A producer seals whatever the tree holds, dirt included, so the sealed
    # commit is not the submitter's HEAD. That is the case the proof is for.
    (root / "task.py").write_text(
        "import pathlib\n"
        "pathlib.Path('shard.json').write_text('sealed dirty')\n",
        encoding="utf-8",
    )
    return root


def _producer_action(checkout: Path) -> dict:
    """The shape both Tessera dispatchers build: no snapshot, no stamp."""

    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tessera/glm53-export-shard",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "tessera-shard",
            "argv": [sys.executable, "task.py"],
            "working_directory": ".",
            "result_path": "shard.json",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"shard": 1, "of_shards": 120},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    })


@pytest.fixture
def sealed(tmp_path: Path) -> tuple[dict, pb.PrismaBuildCAS, Path]:
    """One Tessera action re-sealed around a snapshot, as the lane does."""

    fleet_submit._SNAPSHOT_CACHE.clear()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    checkout = _producer_checkout(tmp_path)
    action, _ = fleet_submit.seal_checkout_into_action(
        _producer_action(checkout), cas=cas, checkout_root=checkout
    )
    assert action["task"]["definition_id"] != "fleet/pbrun"
    assert action["params"]["checkout_snapshot"]["schema"] == (
        pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2
    )
    return action, cas, checkout


def _preflight(action: dict, cas: pb.PrismaBuildCAS, root: Path) -> None:
    pb.preflight_action(action, cas_root=cas.root, checkout_root=root)


def test_a_faithful_materialized_tree_is_accepted(
    sealed: tuple[dict, pb.PrismaBuildCAS, Path], tmp_path: Path
) -> None:
    """The proof must pass the tree the node actually builds."""

    action, cas, _ = sealed
    item = {
        "action_key": str(action["action_key"]),
        "cas_root": str(cas.root),
        "checkout_snapshot": action["params"]["checkout_snapshot"],
    }

    with materialize._execution_checkout(
        item, local_checkout_root=tmp_path / "local"
    ) as checkout:
        _preflight(action, cas, checkout)
        assert (checkout / "task.py").read_text().endswith("'sealed dirty')\n")


def test_a_written_into_materialized_tree_is_refused(
    sealed: tuple[dict, pb.PrismaBuildCAS, Path], tmp_path: Path
) -> None:
    """A tree that no longer is the sealed commit must not run under its key."""

    action, cas, _ = sealed
    item = {
        "action_key": str(action["action_key"]),
        "cas_root": str(cas.root),
        "checkout_snapshot": action["params"]["checkout_snapshot"],
    }

    with materialize._execution_checkout(
        item, local_checkout_root=tmp_path / "local"
    ) as checkout:
        (checkout / "payload.txt").write_text("not what was sealed\n")

        with pytest.raises(
            pb.ActionContractError, match="differs from its sealed commit"
        ):
            _preflight(action, cas, checkout)


def test_the_submitters_own_tree_is_refused(
    sealed: tuple[dict, pb.PrismaBuildCAS, Path],
) -> None:
    """The submitter's tree is dirty and is not the sealed commit either."""

    action, cas, checkout = sealed

    with pytest.raises(
        pb.ActionContractError, match="differs from its sealed commit"
    ):
        _preflight(action, cas, checkout)


def test_a_tree_without_its_sealed_parent_is_refused(
    sealed: tuple[dict, pb.PrismaBuildCAS, Path], tmp_path: Path
) -> None:
    """Ancestry is what the v2 record sells, so it is part of the proof.

    A tree cut off at the sealed commit still checks out clean at it, so the
    identity half cannot see the difference. The action then dies inside its
    own ``BASE...HEAD`` gate on the node, after the queue said yes.
    """

    action, cas, _ = sealed
    item = {
        "action_key": str(action["action_key"]),
        "cas_root": str(cas.root),
        "checkout_snapshot": action["params"]["checkout_snapshot"],
    }

    with materialize._execution_checkout(
        item, local_checkout_root=tmp_path / "local"
    ) as checkout:
        commit = str(action["params"]["checkout_snapshot"]["commit"])
        shallow = Path(_git(checkout, "rev-parse", "--git-dir").strip())
        if not shallow.is_absolute():
            shallow = checkout / shallow
        (shallow / "shallow").write_text(f"{commit}\n", encoding="utf-8")
        assert pb.git_checkout_identity(checkout)["head"] == commit

        with pytest.raises(
            pb.ActionContractError, match="does not carry its sealed parent"
        ):
            _preflight(action, cas, checkout)
