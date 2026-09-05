"""What the Epilog gets told, and when, and whether it can read it.

The Epilog is the single owner of node-side cleanup, and everything it does it
does out of one flat ``key=value`` file the job leaves behind. Two things about
that file decide whether a killed job leaks:

*   **When the tree is named.** ``epilog.sh`` removes a checkout only when
    ``checkout_dir`` is non-empty. The launcher wrote the file with
    ``checkout_dir=`` and filled the real path in only after the materializer
    had finished fetching, which for a large snapshot is minutes of git. A job
    killed by a time limit or a ``scancel`` inside that window leaves its tree
    under ``/home/rob/tmp/prismabuild-checkouts`` forever, because nothing that
    runs afterwards knows the name ``mkdtemp`` chose.

*   **Whether the Epilog can read it.** The file is written with no explicit
    mode, so a submitter running under ``umask 077`` produces mode 0600. The
    Epilog reaches it as a root-squashed user over NFS, so its ``sed`` reads
    nothing, every field comes back empty, and the containers and the checkout
    leak while the state file itself is still deleted -- the failure that says
    nothing at all.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import materialize  # noqa: E402

import slurm_job  # noqa: E402

from test_slurm_lane import WORKER, _runnable_action  # noqa: E402


def _fields(path: Path) -> dict[str, str]:
    return dict(
        line.split("=", 1)
        for line in path.read_text(encoding="utf-8").splitlines()
        if "=" in line
    )


def test_the_tree_is_named_before_the_fetch_that_creates_it_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job killed during the fetch has to leave the Epilog a path."""

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    state_root = tmp_path / "jobs"
    checkouts = tmp_path / "materialized"
    state_path = state_root / "6001.job"

    at_fetch: list[str] = []
    real_git = materialize._run_materializer_git

    def watched(argv, **kwargs):
        if "fetch" in argv and state_path.exists():
            at_fetch.append(_fields(state_path).get("checkout_dir", ""))
        return real_git(argv, **kwargs)

    monkeypatch.setattr(materialize, "_run_materializer_git", watched)
    code = slurm_job.main([
        "--action", str(request), "--cas-root", str(cas_root),
        "--worker", str(WORKER), "--worker-python", sys.executable,
        "--job-state-root", str(state_root), "--job-id", "6001",
        "--checkout-root", str(checkouts),
    ])

    assert code == 0
    assert at_fetch, "the fetch never ran, so the window was never entered"
    recorded = at_fetch[0]
    assert recorded, "the Epilog was told nothing to remove during the fetch"
    assert Path(recorded).parent == checkouts


def test_the_recorded_tree_is_the_one_the_materializer_made(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Naming it early must name that directory, not a guess beside it.

    Read off ``git init``'s own argument, which is ``<temporary>/checkout``, so
    the assertion does not depend on how the launcher derives the path.
    """

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    state_root = tmp_path / "jobs"
    checkouts = tmp_path / "materialized"

    repositories: list[Path] = []
    real_git = materialize._run_materializer_git

    def watched(argv, **kwargs):
        if "init" in argv:
            repositories.append(Path(argv[-1]))
        return real_git(argv, **kwargs)

    monkeypatch.setattr(materialize, "_run_materializer_git", watched)
    assert slurm_job.main([
        "--action", str(request), "--cas-root", str(cas_root),
        "--worker", str(WORKER), "--worker-python", sys.executable,
        "--job-state-root", str(state_root), "--job-id", "6002",
        "--checkout-root", str(checkouts),
    ]) == 0

    recorded = _fields(state_root / "6002.job")["checkout_dir"]
    assert repositories and str(repositories[0].parent) == recorded
    # And the materializer still removed it on the normal ending, so nothing
    # is cleaned twice and nothing is left for the Epilog to find.
    assert not Path(recorded).exists()


def test_the_state_file_is_readable_under_a_restrictive_umask(
    tmp_path: Path
) -> None:
    """The Epilog reads this as ``nobody`` through a root-squashed export."""

    path = tmp_path / "jobs" / "6003.job"
    previous = os.umask(0o077)
    try:
        slurm_job._write_job_state(
            path,
            action_key="cd" * 32,
            container_owner="",
            container_marker="",
            container_job="6003",
            checkout_dir=None,
            local_checkout_root=tmp_path / "checkouts",
        )
    finally:
        os.umask(previous)

    assert stat.S_IMODE(path.stat().st_mode) == 0o644


def test_a_root_addressed_action_still_records_no_tree(
    tmp_path: Path
) -> None:
    """A live checkout root is not a materialized tree, and the Epilog must
    never be handed a path it would then delete."""

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    live = tmp_path / "live"
    live.mkdir()
    (live / "task.py").write_text(
        "open('result.txt', 'w').write('live\\n')\n", encoding="utf-8")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": [sys.executable, "task.py"],
            "working_directory": ".",
            "result_path": "result.txt",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(live, ["task.py"]),
        "params": {
            "command": [sys.executable, "task.py"],
            "cwd": ".",
            "demand": {},
            "checkout_root": str(live),
        },
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    })
    request = cas.publish_action_request(action)
    state_root = tmp_path / "jobs"

    slurm_job.main([
        "--action", str(request), "--cas-root", str(cas_root),
        "--worker", str(WORKER), "--worker-python", sys.executable,
        "--job-state-root", str(state_root), "--job-id", "6004",
        "--checkout-root", str(tmp_path / "materialized"),
    ])

    assert _fields(state_root / "6004.job")["checkout_dir"] == ""
    assert live.is_dir()
