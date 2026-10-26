"""Publication refuses an impossible canary before changing the runtime."""
from pathlib import Path
import shutil

import pytest

from prismabuild import capability_eligibility, contracts, pool, publication_canary
from test_publish_runtime import ROOT, _checkout, _run_publish, publish_runtime

IMAGE = "example.invalid/canary@sha256:" + "ab" * 32


def _publisher(tmp_path, monkeypatch, *, worker_host="sparky", gpu=True, memory=16):
    checkout = _checkout(tmp_path / "box-local-checkout", "new")
    shutil.copyfile(ROOT / "tools/fleet/pbcanary.py", checkout / "tools/fleet/pbcanary.py")
    assert pool.is_box_local_path(checkout.resolve())
    monkeypatch.setattr(publish_runtime.socket, "gethostname", lambda: "sparky")
    monkeypatch.setenv("PBCANARY_GPU_IMAGE", IMAGE)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host=worker_host,
        tags=[worker_host, publication_canary.CAPABILITY, contracts.CONTAINER_IMAGE_TAG,
              "runtime-generation:previous"],
        has_gpu=gpu, capacity={"cpu": 4, "gpu": int(gpu), "mem_gb": memory},
        capabilities=capability_eligibility.OfferCapabilities(
            images=capability_eligibility.ImageAnswers.from_observed([IMAGE], None)),
    )
    monkeypatch.setattr(publish_runtime, "_invoke_canary_driver", lambda _: (0, "fixture verdict"))
    return checkout


def test_box_local_publisher_without_a_worker_refuses_before_publication(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch, worker_host="another-worker")
    with pytest.raises(SystemExit, match="canary.*sparky"):
        _run_publish(tmp_path, monkeypatch, checkout, [])
    assert not (tmp_path / "mirror").exists()
    assert not (tmp_path / "runtime-generations").exists()
