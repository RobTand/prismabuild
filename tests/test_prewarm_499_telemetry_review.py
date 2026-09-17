"""Review regressions: missing disk feedback must not authorize data reads."""
import threading

import pytest

from prewarm_fixture import prewarm_loop


@pytest.mark.parametrize("readable_disk", [False, True], ids=["all-missing", "one-member-missing"])
def test_required_disk_feedback_loss_prevents_payload_reads(tmp_path, readable_disk):
    stop = threading.Event()
    clock = [10.0]

    def now():
        # Supply distinct intervals even when wait returns without sleeping.
        clock[0] += 1.0
        return clock[0]

    def sleep(seconds):
        clock[0] += seconds
        stop.set()  # An interruptible hold is sufficient for this check.

    def stats(device):
        return [0] * 11 if readable_disk and device == "sdc" else None

    pacer = prewarm_loop.DiskPacer(
        ["sdb", "sdc"], max_util_pct=40, max_read_await_ms=15,
        max_backlog_ms=4000, sample_s=0, stat_source=stats,
        clock=now, sleep=sleep,
    )
    # A healthy member must not cover for another member with missing stats.
    pacer.wait(stop)
    payload = tmp_path / "input"
    payload.write_bytes(b"x" * 4096)
    result = prewarm_loop.Reader(
        1, prewarm_loop.MountMap([f"{tmp_path}={tmp_path}"]), pacer=pacer,
    ).read(
        [{"path": str(payload), "offset": 0, "bytes": 4096}],
        budget_bytes=4096, stop=stop,
    )
    assert result["bytes_warmed"] == 0, result
