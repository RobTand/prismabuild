"""An ordinary ``pbrun`` submission seals the body it sealed before.

#517 moves ``main()``'s action construction into a reusable builder so the
pre-execution decomposer can seal children through the same sealer rather than
clone its hashing rules.  Every ``pbrun`` action key is a content hash of the
body below, and every receipt in the CAS is addressed by that key, so a body
that shifts by one field orphans the fleet's entire measurement history and
turns every cache hit into a re-run.  The refactor is therefore correct exactly
when this file still passes, and nothing else about it is interesting.

Two rows, because most of the body is conditional.  The plain row seals none
of the optional ``params``; the rich row seals a data manifest, the two GPU
fields, an execution deadline, a progress policy and a profile mode, so a move
that only touches one of the ``if`` branches cannot pass unseen.

The golden is the sealed body rather than a digest of it because a digest says
only *that* something moved.  Three values in it hash the queue root -- this
test's temporary directory -- and are masked by token rather than dropped, so
the shape around each one is still checked:

* ``PRISMABUILD_CONTAINER_OWNER`` hashes the queue's ``container-owners`` root
  (``pbrun.container_owner``).  Its marker path carries the same digest.
* That owner is in ``variables`` by the time ``result_and_stamp_names`` takes
  its fingerprint, so the 16-hex stamp fingerprint moves with it, and with the
  fingerprint go the result path, the ``tee`` target inside ``argv``, the
  closure's stamp filename and therefore ``closure_sha256`` -- and, because
  the stamp is committed into the snapshot's private index under that name,
  the snapshot commit and the bundle bytes it addresses.
* The action key follows from the body, so it inherits all of the above.

Masking a value would hide a moved value, so neither mask is left to stand on
its own.  ``test_every_masked_value_is_re_derived_from_the_body`` recomputes
the owner and the fingerprint *from the sealed body*, which is what catches
the one drift the golden structurally cannot see: an extraction that hashes
the environment a line early still writes the same ``variables`` into the body
and moves every ordinary action key.  ``test_the_key_is_the_hash_of_that_body``
does the same for the key.  Only the snapshot's bytes remain unre-derived;
that they are a function of the tree alone is
``test_pbrun_bundle_determinism.py``'s subject, not this file's.
"""
from __future__ import annotations

import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

#: Fixed so the fixture's commit object, and every digest downstream of it, is
#: the same bytes wherever this runs.
COMMIT_DATE = "2026-09-11T00:00:00+00:00"

MASK = "<derived-from-the-queue-root>"

#: The two shapes of ordinary submission: no optional ``params`` at all, and
#: every one of them that does not need a second box to be true.
ROWS: dict[str, list[str]] = {
    "plain": [],
    "rich": [
        # ``--gpu-capacity`` explicitly, so the demand this row seals is a
        # property of the arguments rather than of whatever a live box last
        # announced.
        "--gpu", "--exclusive", "--gpu-capacity", "1",
        "--gpu-memory-gb", "8", "--timeout-s", "60",
        "--progress-phase", "load=120", "--progress-phase", "measure=600",
        "--profile", "sample",
    ],
}


def _golden(row: str) -> Path:
    return Path(__file__).with_name(f"{Path(__file__).stem}.{row}.golden.json")


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


def _data_manifest(root: Path) -> Path:
    """A manifest whose bytes are fixed; the files it names are never read.

    The manifest is content-addressed like any other input, so what the action
    key covers is this file, not the residency it describes.
    """

    path = root / "inputs" / "data-manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = [
        {"path": f"/mnt/shared/pb517/shard-{index:02d}.bin",
         "offset": 0, "bytes": 4096 * (index + 1), "sha256": None}
        for index in range(3)
    ]
    path.write_text(json.dumps({
        "schema": pbrun.pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "pb517-test"},
        "annotations": {},
        "mount_prefix": "/mnt/shared",
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(entry["bytes"] for entry in entries),
    }, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _seal(
    root: Path, monkeypatch: pytest.MonkeyPatch, row: str,
) -> dict[str, Any]:
    """Submit one ordinary action and return the body ``pbrun`` sealed."""

    work = _checkout(root)
    queue = pool.PoolQueue(root / "pb-queue")
    # Progress-capable, because ``pbrun`` refuses a progress policy no live
    # worker can honour.  The offer is a diagnostic, not an input to the
    # body: it decides whether the submission is allowed, never what it
    # seals, which is why the plain row's golden does not move with it.
    queue.announce(
        host="sparky",
        tags=["sparky", "gb10", pbrun.pb.PROGRESS_TAG,
              pbrun.pb.PROGRESS_HELPER_TAG],
        has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
        timeout_ceiling_s=7200,
        progress_contracts=[pbrun.pb.PROGRESS_RECORD_SCHEMA_V1],
    )
    extra = list(ROWS[row])
    if row == "rich":
        extra += ["--data-manifest", str(_data_manifest(root))]
    monkeypatch.setattr(pbrun, "SH", root)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        "--here", "--no-default-env", *extra,
        "--", "/bin/bash", "-lc", "printf ok",
    ])
    assert pbrun.main() == 0
    requests = sorted((root / "cas" / "requests").rglob("*.json"))
    assert len(requests) == 1, f"one submission sealed {len(requests)} requests"
    return json.loads(requests[0].read_text(encoding="utf-8"))


def _masked(body: dict[str, Any], root: Path) -> dict[str, Any]:
    """Replace the queue-root-derived values, leaving the shape around them."""

    result_path = str(body["task"]["result_path"])
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
    snapshot = masked["params"]["checkout_snapshot"]
    snapshot["commit"] = MASK
    for entry in [*masked["inputs"], snapshot["input"]]:
        if entry["id"] == pbrun.pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID:
            entry["sha256"] = MASK
            entry["bytes"] = MASK
    return masked


def _canonical(value: object) -> str:
    return json.dumps(value, indent=1, sort_keys=True) + "\n"


@pytest.mark.parametrize("row", sorted(ROWS))
def test_the_sealed_body_matches_the_recorded_golden(
    row: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole body, field by field -- argv template included."""

    golden = _golden(row)
    observed = _canonical(_masked(_seal(tmp_path, monkeypatch, row), tmp_path))
    if not golden.exists():          # recording run; never silently passes
        golden.write_text(observed, encoding="utf-8")
        pytest.fail(f"recorded a new golden at {golden.name}; re-run to check it")
    assert observed == golden.read_text(encoding="utf-8"), (
        "the ordinary pbrun action body moved: every CAS receipt addressed by "
        "the old key is orphaned. Restore the body, or -- if the change is "
        "intended -- delete the golden, re-record it, and say in the commit "
        "message which receipts it retires."
    )


@pytest.mark.parametrize("row", sorted(ROWS))
def test_every_masked_value_is_re_derived_from_the_body(
    row: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The masks are consequences of the queue root, not a hole in the golden.

    The golden pins ``variables`` literally but masks the owner hashed *from*
    them, so an extraction that took its copy of the environment one line
    early -- before the wrapper joins ``PATH``, say -- would move every
    ordinary action key while the golden still matched.  So the owner and the
    stamp fingerprint are recomputed here from the sealed body itself, with
    only the checkout identity recorded as an input, since ``main()`` reads it
    before it migrates the local exclude file.
    """

    seen: dict[str, object] = {}
    identify = pbrun._git_identity
    monkeypatch.setattr(
        pbrun, "_git_identity",
        lambda cwd: seen.setdefault("identity", identify(cwd)),
    )
    body = _seal(tmp_path, monkeypatch, row)
    work = tmp_path / "work"
    params = body["params"]

    variables = dict(body["environment"]["variables"])
    owner = variables.pop(pbrun.CONTAINER_OWNER_ENV)
    marker = variables.pop(pbrun.CONTAINER_MARKER_ENV)
    marker_root = tmp_path / "pb-queue" / pool.CONTAINER_OWNERS
    assert owner == pbrun.container_owner(
        params["command"], work, params["demand"], variables,
        determinism=body["task"]["determinism"],
        retry_policy=params["retry_policy"],
        marker_root=marker_root,
        identity=seen["identity"],
        logical_cwd=params["cwd"],
        placement=params["placement"],
    ), "the container owner is not the hash of the environment that was sealed"
    assert marker == str(marker_root / f"{owner}.used")

    log_name, stamp_name = pbrun.result_and_stamp_names(
        params["command"], work, params["demand"],
        body["environment"]["variables"],
        identity=seen["identity"],
        logical_cwd=params["cwd"],
        placement=params["placement"],
    )
    assert body["task"]["result_path"] == log_name
    assert [entry["path"] for entry in body["code_closure"]["files"]] == [
        stamp_name
    ], "the closure stamp is not the one this submission's fingerprint names"
    assert f"tee {shlex.quote(log_name)}" in body["task"]["argv"][-1], (
        "the action would tee its output somewhere other than its declared "
        "result path"
    )


@pytest.mark.parametrize("row", sorted(ROWS))
def test_the_key_is_the_hash_of_that_body(
    row: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The golden masks ``action_key``; this is what keeps that honest."""

    body = _seal(tmp_path, monkeypatch, row)
    key = body.pop("action_key")
    assert pbrun.pb.seal_action(body)["action_key"] == key
