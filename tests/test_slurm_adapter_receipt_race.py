"""A receipt that lands during the terminal query wins, in the retained adapter.

``SlurmAdapter.resolve`` looked the CAS up once at entry and never again.
Ordinary job completion publishes the receipt while the ``squeue`` or ``sacct``
query is in flight, so a COMPLETED job whose verified receipt landed inside
that window was reported ``failed`` with "no valid CAS receipt exists" while
``cas.lookup`` already answered it.  ``DagsterActionRunner`` raised on that
resolution, and an immediate second ``resolve`` said ``succeeded`` from the
same CAS.

Issue #91.  This is the retained Dagster adapter, not the thin SLURM lane, so
the change is only the second read at the terminal decision boundary; the
durable journal audit and the receipt validation are unchanged.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm import (  # noqa: E402
    _action, _adapter, _completed, _record_submission, _state_row,
)

PAYLOAD = b"canonical result"


def _prepared(tmp_path: Path):
    """One recorded submission, and everything needed to publish its result."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout)
    adapter, _worker = _adapter(tmp_path)
    intent = _record_submission(adapter, action, checkout, "123")
    cas = pb.PrismaBuildCAS(adapter.cas_root)
    output = tmp_path / "output"
    output.write_bytes(PAYLOAD)
    attestation = pb.preflight_action(
        action, cas_root=adapter.cas_root, checkout_root=checkout)
    return adapter, action, intent, cas, output, attestation


def test_a_receipt_published_during_the_query_resolves_succeeded(
    tmp_path: Path
) -> None:
    """Pre-fix: ``resolution.status == "succeeded"`` failed with ``'failed'``
    and the reason ``SLURM reported COMPLETED but no valid CAS receipt
    exists``, with ``cas.lookup(action)`` answering at the same moment."""

    adapter, action, intent, cas, output, attestation = _prepared(tmp_path)

    def fake_run(argv):
        if Path(argv[0]).name == "squeue":
            # The state at entry: no receipt yet.  The worker finishes and
            # publishes while the scheduler is being asked.
            assert cas.lookup(action) is None
            cas.publish_result(action, output, attestation=attestation)
            return _completed(argv, "")
        assert Path(argv[0]).name == "sacct"
        return _completed(argv, _state_row(intent, 123, "COMPLETED"))

    adapter._run = fake_run

    resolution = adapter.resolve(action, "123")

    assert resolution.status == "succeeded"
    assert resolution.reason == "verified CAS receipt exists"
    assert resolution.receipt is not None
    assert resolution.payload_path is not None
    assert resolution.payload_path.read_bytes() == PAYLOAD
    # The scheduler's answer is kept rather than dropped: it is what the job
    # ended as, and the receipt is why the action succeeded.
    assert resolution.slurm_state == "COMPLETED"
    # Idempotent, as it was before: the same answer from the entry lookup.
    assert adapter.resolve(action, "123").status == "succeeded"


def test_a_completed_job_with_no_receipt_still_fails(tmp_path: Path) -> None:
    """The rule the second read must not weaken: a receipt is the only
    success, and a job that exited zero without publishing one has not done
    the work."""

    adapter, action, intent, _cas, _output, _attestation = _prepared(tmp_path)

    def fake_run(argv):
        if Path(argv[0]).name == "squeue":
            return _completed(argv, "")
        return _completed(argv, _state_row(intent, 123, "COMPLETED"))

    adapter._run = fake_run

    resolution = adapter.resolve(action, "123")

    assert resolution.status == "failed"
    assert resolution.reason == (
        "SLURM reported COMPLETED but no valid CAS receipt exists")
    assert resolution.receipt is None


def test_a_receipt_published_during_a_cancellation_query_also_wins(
    tmp_path: Path
) -> None:
    """The boundary is the terminal decision, not the COMPLETED branch: work
    the CAS holds is done however the allocation ended."""

    adapter, action, intent, cas, output, attestation = _prepared(tmp_path)

    def fake_run(argv):
        if Path(argv[0]).name == "squeue":
            cas.publish_result(action, output, attestation=attestation)
            return _completed(argv, "")
        return _completed(argv, _state_row(intent, 123, "CANCELLED"))

    adapter._run = fake_run

    resolution = adapter.resolve(action, "123")

    assert resolution.status == "succeeded"
    assert resolution.receipt is not None
