"""An ordinary ``pbrun`` submission seals the body it sealed before.

#517 moves ``main()``'s action construction into a reusable builder so the
pre-execution decomposer can seal children through the same sealer rather than
clone its hashing rules.  Every ``pbrun`` action key is a content hash of the
body below, and every receipt in the CAS is addressed by that key, so a body
that shifts by one field orphans the fleet's entire measurement history and
turns every cache hit into a re-run.  The refactor is therefore correct exactly
when this file's golden still matches, and nothing else about it is
interesting.

The golden is the sealed body rather than a digest of it because a digest says
only *that* something moved.  One value in the body cannot be frozen, and
everything derived from it is masked by token rather than dropped, so the
shape around each one is still checked:

* ``PRISMABUILD_CONTAINER_OWNER`` hashes the queue's ``container-owners`` root
  (``pbrun.container_owner``), which is the temporary directory this test runs
  under.  Its marker path carries the same digest.
* That owner is in ``variables`` by the time ``result_and_stamp_names`` takes
  its fingerprint, so the 16-hex stamp fingerprint moves with it, and with the
  fingerprint go the result path, the ``tee`` target inside ``argv``, the
  closure's stamp filename and therefore ``closure_sha256``.
* The wrapper directory is the deployed checkout's, so it differs between a
  developer worktree and the tree PrismaBuild materializes.
* The action key follows from the body, so it inherits all of the above.  The
  second test is what keeps that mask honest.

Everything else is pinned literally.  The fixture commits at a fixed date so
its commit -- and therefore the bundle bytes ``pbrun.checkout-snapshot``
addresses -- is the same on every box and every run.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

GOLDEN = Path(__file__).with_suffix("").with_suffix(".golden.json")

#: Fixed so the fixture's commit object, and every digest downstream of it, is
#: the same bytes wherever this runs.
COMMIT_DATE = "2026-09-11T00:00:00+00:00"

MASK = "<derived-from-the-queue-root>"


def _checkout(root: Path) -> Path:
    work = root / "work"
    work.mkdir(parents=True)
    (work / "seed.txt").write_text("sealed\n", encoding="utf-8")
    environment = {
        "GIT_AUTHOR_DATE": COMMIT_DATE,
        "GIT_COMMITTER_DATE": COMMIT_DATE,
        "GIT_AUTHOR_NAME": "PrismaBuild test",
        "GIT_COMMITTER_NAME": "PrismaBuild test",
        "GIT_AUTHOR_EMAIL": "test@example.invalid",
        "GIT_COMMITTER_EMAIL": "test@example.invalid",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(root),
    }
    for args in (
        ("init", "-q", "-b", "main"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "seed.txt"),
        ("commit", "-qm", "sealed tree"),
    ):
        done = subprocess.run(
            ["git", "-C", str(work), *args], capture_output=True, text=True,
            env=environment,
        )
        assert done.returncode == 0, done.stderr
    return work


def _seal(root: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Submit one ordinary action and return the body ``pbrun`` sealed."""

    work = _checkout(root)
    queue = pool.PoolQueue(root / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    monkeypatch.setattr(pbrun, "SH", root)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--here", "--no-default-env", "--", "/bin/bash", "-lc", "printf ok",
    ])
    assert pbrun.main() == 0
    requests = sorted((root / "cas" / "requests").rglob("*.json"))
    assert len(requests) == 1, f"one submission sealed {len(requests)} requests"
    return json.loads(requests[0].read_text(encoding="utf-8"))


def _masked(body: dict[str, object], root: Path) -> dict[str, object]:
    """Replace the queue-root-derived values, leaving the shape around them."""

    result_path = str(body["task"]["result_path"])          # type: ignore[index]
    fingerprint = result_path.split(".")[1]
    assert len(fingerprint) == 16, f"unexpected result path {result_path!r}"
    text = json.dumps(body, sort_keys=True)
    for literal, token in (
        (str(root), "/PB517"),
        (str(pbrun.CONTAINER_WRAPPER_DIR), "/PB517/wrapper"),
        (fingerprint, "<stamp-fingerprint>"),
    ):
        text = text.replace(literal, token)
    masked = json.loads(text)
    variables = masked["environment"]["variables"]
    variables[pbrun.CONTAINER_OWNER_ENV] = MASK
    variables[pbrun.CONTAINER_MARKER_ENV] = MASK
    masked["code_closure"]["closure_sha256"] = MASK
    masked["action_key"] = MASK
    # The stamp is committed into the snapshot's private index under its
    # fingerprinted name, so the snapshot commit, and the bundle bytes that
    # carry it, move with the fingerprint as well.  What this golden pins
    # about the snapshot is where it appears in the body and the rest of its
    # fields; that its bytes are a function of the tree alone is
    # ``test_pbrun_bundle_determinism.py``'s subject, not this file's.
    snapshot = masked["params"]["checkout_snapshot"]
    snapshot["commit"] = MASK
    for entry in [*masked["inputs"], snapshot["input"]]:
        if entry["id"] == pbrun.pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID:
            entry["sha256"] = MASK
            entry["bytes"] = MASK
    return masked


def _canonical(value: object) -> str:
    return json.dumps(value, indent=1, sort_keys=True) + "\n"


def test_the_sealed_body_matches_the_recorded_golden(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole body, field by field -- argv template included."""

    observed = _canonical(_masked(_seal(tmp_path, monkeypatch), tmp_path))
    if not GOLDEN.exists():          # recording run; never silently passes
        GOLDEN.write_text(observed, encoding="utf-8")
        pytest.fail(f"recorded a new golden at {GOLDEN.name}; re-run to check it")
    assert observed == GOLDEN.read_text(encoding="utf-8"), (
        "the ordinary pbrun action body moved: every CAS receipt addressed by "
        "the old key is orphaned. Restore the body, or -- if the change is "
        "intended -- delete the golden, re-record it, and say in the commit "
        "message which receipts it retires."
    )


def test_the_key_is_the_hash_of_that_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The masked field is a consequence, not an escape hatch.

    The golden above masks ``action_key`` because two of the body's values
    hash the queue root.  That is only safe while the key remains a pure
    function of the body, so the seal is re-derived here from the body the
    golden recorded.
    """

    body = _seal(tmp_path, monkeypatch)
    key = body.pop("action_key")
    assert pbrun.pb.seal_action(body)["action_key"] == key
