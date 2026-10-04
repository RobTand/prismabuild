"""#1403 item 2 (#1500): docker's "removal ... already in progress" is not an error.

On 2026-10-01 a withdrawn claim's cleanup recorded ``PoolContractError: docker
cleanup failed (1): Error response from daemon: removal of container
fae18385af61 is already in progress`` although the daemon was removing the
container and it was gone moments later. That is a transient state: cleanup is
still pending, the next sweep's ownership query proves absence, and the claim
keeps its tokens until then. Any other ``docker rm`` failure still raises.

Only the docker CLI boundary (``subprocess.run`` for ``pool.DOCKER``) is
stubbed; the queue, claim, finish, ledger and container marker are real.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from prismabuild import pool

KEY = "a" * 64
OWNER = "e" * 64
BUSY = "fae18385af61"
OTHER = "0123456789ab"


def _in_progress(cid: str) -> str:
    return f"Error response from daemon: removal of container {cid} is already in progress\n"


class FakeDocker:
    """``docker ps``/``docker rm`` answers, scripted per call; never a real daemon."""

    def __init__(self, monkeypatch):
        self.present: set[str] = set()
        self.rm_answers: list[tuple[int, str, str]] = []
        self.calls: list[list[str]] = []
        real = subprocess.run

        def run(argv, *args, **kwargs):
            if not argv or argv[0] != pool.DOCKER:
                return real(argv, *args, **kwargs)
            self.calls.append(list(argv))
            if argv[1] == "ps":
                out = "".join(f"{cid}\n" for cid in sorted(self.present))
                return subprocess.CompletedProcess(argv, 0, out, "")
            assert argv[1:3] == ["rm", "-f"]
            code, out, err = self.rm_answers.pop(0)
            return subprocess.CompletedProcess(argv, code, out, err)

        monkeypatch.setattr(pool.subprocess, "run", run)


@pytest.fixture()
def docker(monkeypatch):
    return FakeDocker(monkeypatch)


def test_in_progress_removal_of_a_requested_container_is_not_a_failure(docker):
    docker.rm_answers.append((1, f"{OTHER}\n", _in_progress(BUSY)))
    assert pool._docker_remove_containers([BUSY, OTHER]) == [OTHER]


def test_in_progress_matches_the_full_id_of_a_requested_short_id(docker):
    docker.rm_answers.append((1, "", _in_progress(BUSY + "c" * 52)))
    assert pool._docker_remove_containers([BUSY]) == []


@pytest.mark.parametrize("stderr", [
    "Error response from daemon: cannot remove container: permission denied\n",
    _in_progress("ffffffffffff"),  # not a container this cleanup asked to remove
    _in_progress(BUSY) + "Error response from daemon: device or resource busy\n",
    "",
])
def test_any_other_docker_rm_failure_still_raises(docker, stderr):
    docker.rm_answers.append((1, "", stderr))
    with pytest.raises(pool.PoolContractError, match="docker cleanup failed"):
        pool._docker_remove_containers([BUSY])


@pytest.fixture()
def claimed(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"gpu": 1, "mem_gb": 8})
    queued = pool._read_json(queue.item_path(pool.READY, KEY))
    queued["container_owner"] = OWNER
    pool._write_json_atomic(queue.item_path(pool.READY, KEY), queued)
    record = queue.claim(capacity={"gpu": 1, "mem_gb": 8})
    assert record is not None and record["action_key"] == KEY
    marker = queue.root / "container-owners" / f"{OWNER}.used"
    marker.parent.mkdir(parents=True)
    marker.write_text(OWNER)
    return queue, record, marker


def test_withdrawn_finish_records_in_progress_cleanup_as_pending_then_concludes(claimed, docker):
    queue, record, marker = claimed
    docker.present = {BUSY}
    # The daemon is already removing it; the follow-up ownership query still
    # lists it, so cleanup is honestly incomplete -- but not a failure.
    docker.rm_answers.append((1, "", _in_progress(BUSY)))
    path = queue.finish(KEY, status="withdrawn",
                        detail={"returncode": -15, "termination_reason": "withdrawn"},
                        claim_snapshot=record)
    assert path == queue.item_path(pool.CLAIMED, KEY)
    pending = pool._read_json(path)
    cleanup = pending["container_cleanup_pending"]
    assert cleanup["complete"] is False
    assert "error" not in cleanup, cleanup
    assert cleanup["remaining"] == [BUSY]
    assert pending["finish_pending"]["status"] == "withdrawn"
    assert queue.ledger().held_keys() == [KEY]
    assert marker.exists()

    # The removal finishes; the next retry proves absence and concludes.
    docker.present = set()
    snapshot = pool._read_json(queue.item_path(pool.CLAIMED, KEY))
    concluded = queue._retry_own_pending_finish(KEY, snapshot)
    assert concluded == queue.item_path(pool.WITHDRAWN, KEY)
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.ledger().held_keys() == []
    assert not marker.exists()


def test_a_real_removal_failure_is_still_recorded_as_an_error(claimed, docker):
    queue, record, marker = claimed
    docker.present = {BUSY}
    docker.rm_answers.append((1, "", "Error response from daemon: driver failed\n"))
    path = queue.finish(KEY, status="withdrawn", claim_snapshot=record)
    cleanup = pool._read_json(path)["container_cleanup_pending"]
    assert cleanup["complete"] is False
    assert "docker cleanup failed (1)" in cleanup["error"]
    assert queue.ledger().held_keys() == [KEY]
