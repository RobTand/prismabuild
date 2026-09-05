"""A killed ingest leaves an attributable directory, not an orphan payload.

Issue #57: "Root `.staging` (`core.py:3313`) keeps an aborted bundle ingest (up
to 512 MiB) forever."

The wording needed a correction, recorded here because the correction is the
reason the fix looks the way it does.  An ingest that is *refused* already
leaves nothing: ``_copy_to_staging`` unlinks its payload through the staging
directory descriptor on any exception, and ``ingest_input`` unlinks again in a
``finally``.  A digest mismatch, a byte-count mismatch and an unsupported source
all leave the store clean.

What leaks is a *killed* ingest, which no ``finally`` can reach.  ``pbrun``
ingests the checkout bundle, capped at 512 MiB, and a SIGTERM at a job time
limit or an OOM during that copy leaves the payload behind.  In one shared
``.staging`` that leftover is a bare ``.payload.<random>.tmp`` sitting next to
the in-flight payloads of every other process using the store, so a sweeper
cannot tell an abandoned bundle from a live one and the bytes stay forever.

One private directory per ingest makes the leftover attributable, which is what
``pb_gc`` needs to be able to act on it, and the ingest removes its own
directory on both the success and the failure path.
"""

from __future__ import annotations

import fcntl
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import core as pb  # noqa: E402

PAYLOAD_BYTES = 1 << 20

#: The child ingests for real and kills itself once the payload is staged and
#: before the blob is published, which is where a job time limit lands.
KILLED_INGEST = """
import os
import signal
import sys

sys.path.insert(0, {source!r})

from prismabuild import core as pb


def die(value):
    os.kill(os.getpid(), signal.SIGKILL)


pb.validate_input_contract = die
pb.PrismaBuildCAS({cas!r}).ingest_input(
    {payload!r}, input_id="pbrun/checkout-snapshot"
)
"""


def _payload(tmp_path: Path) -> Path:
    path = tmp_path / "checkout.bundle"
    path.write_bytes(b"b" * PAYLOAD_BYTES)
    return path


def _staging(cas: pb.PrismaBuildCAS) -> list[Path]:
    directory = cas.root / ".staging"
    return sorted(directory.iterdir()) if directory.is_dir() else []


def test_a_killed_ingest_leaves_one_attributable_directory(
    tmp_path: Path,
) -> None:
    """The leftover a sweeper has to decide about must name an owner."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    payload = _payload(tmp_path)
    script = KILLED_INGEST.format(
        source=str(REPOSITORY / "src"),
        cas=str(cas.root),
        payload=str(payload),
    )

    completed = subprocess.run([sys.executable, "-c", script])

    assert completed.returncode == -9
    entries = _staging(cas)
    assert len(entries) == 1, entries
    leftover = entries[0]
    assert leftover.is_dir(), f"leftover is loose in the shared root: {leftover}"
    assert leftover.name.startswith(pb.PRIVATE_STAGING_PREFIX)
    owner = leftover / pb.PRIVATE_STAGING_OWNER
    with owner.open("r+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    staged = sorted(path for path in leftover.iterdir() if path != owner)
    assert len(staged) == 1
    assert staged[0].stat().st_size == PAYLOAD_BYTES


def test_a_refused_ingest_leaves_nothing_behind(tmp_path: Path) -> None:
    """The property the private directory must not cost: a clean refusal."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    payload = _payload(tmp_path)

    with pytest.raises(pb.ActionContractError, match="differs from the expected"):
        cas.ingest_input(
            payload, input_id="pbrun/checkout-snapshot", expected_sha256="0" * 64
        )

    assert _staging(cas) == []


def test_a_successful_ingest_leaves_nothing_behind(tmp_path: Path) -> None:
    """A store that ingests all day must not accrue one directory per input."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    for index in range(4):
        payload = tmp_path / f"input-{index}.bin"
        payload.write_bytes(f"payload {index}\n".encode())
        cas.ingest_input(payload, input_id=f"test/input-{index}")

    assert _staging(cas) == []


def test_a_failing_ingest_does_not_disturb_one_in_flight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup on failure must reach only the failing ingest's own directory.

    The second ingest runs from inside the first one's publish step, so the
    first is holding a staged payload at the moment the second is refused.
    """

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    payload = _payload(tmp_path)
    other = tmp_path / "other.bundle"
    other.write_bytes(b"other\n")
    real = pb.PrismaBuildCAS._publish_staged_input_blob
    observed: dict[str, object] = {}

    def publish(self, staging, contract):
        with pytest.raises(pb.ActionContractError):
            cas.ingest_input(
                other, input_id="test/other", expected_sha256="0" * 64
            )
        observed["survived"] = staging.is_file()
        observed["size"] = staging.stat().st_size
        return real(self, staging, contract)

    monkeypatch.setattr(pb.PrismaBuildCAS, "_publish_staged_input_blob", publish)

    entry, won = cas.ingest_input(payload, input_id="pbrun/checkout-snapshot")

    assert observed == {"survived": True, "size": PAYLOAD_BYTES}
    assert entry["bytes"] == PAYLOAD_BYTES
    assert won is True
    assert _staging(cas) == []


def test_live_ingest_holds_owner_lock_until_cleanup(tmp_path: Path, monkeypatch) -> None:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    payload = _payload(tmp_path)
    real = pb.PrismaBuildCAS._publish_staged_input_blob

    def publish(self, staging, contract):
        assert staging.parent.name.startswith(pb.PRIVATE_STAGING_PREFIX)
        assert staging.parent.stat().st_mode & 0o777 == 0o700
        with (staging.parent / pb.PRIVATE_STAGING_OWNER).open("r+b") as owner:
            with pytest.raises(BlockingIOError):
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return real(self, staging, contract)

    monkeypatch.setattr(pb.PrismaBuildCAS, "_publish_staged_input_blob", publish)
    cas.ingest_input(payload, input_id="test/input")
    assert _staging(cas) == []


def test_ingest_staging_refuses_symlinked_parent(tmp_path: Path) -> None:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.root.mkdir()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (cas.root / ".staging").symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(pb.CASTamperError):
        cas.ingest_input(_payload(tmp_path), input_id="test/input")
    assert list(elsewhere.iterdir()) == []
