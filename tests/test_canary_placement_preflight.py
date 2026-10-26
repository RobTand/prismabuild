"""Publication refuses an impossible canary before changing the runtime."""
import shutil

import pytest

from prismabuild import capability_eligibility, contracts, pool, publication_canary, submission
from test_publish_runtime import ROOT, _checkout, _run_publish, publish_runtime

IMAGE = "example.invalid/canary@sha256:" + "ab" * 32


def _publisher(tmp_path, monkeypatch, *, worker_host="sparky", gpu=True,
               memory=16, image=True, capability=True):
    checkout = _checkout(tmp_path / "box-local-checkout", "new")
    shutil.copyfile(ROOT / "tools/fleet/pbcanary.py", checkout / "tools/fleet/pbcanary.py")
    assert pool.is_box_local_path(checkout.resolve())
    monkeypatch.setattr(publish_runtime.socket, "gethostname", lambda: "sparky")
    monkeypatch.setenv("PBCANARY_GPU_IMAGE", IMAGE)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host=worker_host,
        tags=[worker_host, *([publication_canary.CAPABILITY] if capability else []),
              contracts.CONTAINER_IMAGE_TAG,
              "runtime-generation:previous"],
        has_gpu=gpu, observed_capacity={"cpu": 0, "gpu": 0, "mem_gb": 0},
        capacity={"cpu": 4, "gpu": int(gpu), "mem_gb": memory},
        capabilities=capability_eligibility.OfferCapabilities(
            images=capability_eligibility.ImageAnswers.from_observed([IMAGE] if image else [], None)),
    )
    monkeypatch.setattr(publish_runtime, "_invoke_canary_driver", lambda _: (0, "fixture verdict"))
    return checkout


def test_box_local_publisher_without_a_worker_refuses_before_publication(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch, worker_host="another-worker")
    with pytest.raises(SystemExit, match="canary.*sparky"):
        _run_publish(tmp_path, monkeypatch, checkout, [])
    assert not (tmp_path / "mirror").exists()
    assert not (tmp_path / "runtime-generations").exists()


@pytest.mark.parametrize("offer", [
    {"gpu": False}, {"memory": 8}, {"image": False}, {"capability": False},
])
def test_ineligible_publisher_refuses_before_publication(tmp_path, monkeypatch, offer):
    checkout = _publisher(tmp_path, monkeypatch, **offer)
    with pytest.raises(SystemExit, match="canary.*sparky"):
        _run_publish(tmp_path, monkeypatch, checkout, ["--canary"])
    assert not (tmp_path / "mirror").exists()
    assert not (tmp_path / "runtime-generations").exists()


def test_old_generation_and_busy_worker_allow_publication(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch)
    code, mirror = _run_publish(tmp_path, monkeypatch, checkout, [])
    assert code == 0
    assert (mirror / "RUNTIME_VERSION.json").is_file()
    assert not (tmp_path / "pb-queue/publication-canaries").exists()


def test_disabled_canary_does_not_require_a_publisher_worker(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch, worker_host="another-worker")
    code, mirror = _run_publish(tmp_path, monkeypatch, checkout, ["--no-canary"])
    assert code == 0
    assert (mirror / "RUNTIME_VERSION.json").is_file()


def test_staging_does_not_require_a_publisher_worker(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch, worker_host="another-worker")
    code, mirror = _run_publish(tmp_path, monkeypatch, checkout, ["--stage-only"])
    assert code == 0
    assert not mirror.exists()
    assert len(list((tmp_path / "runtime-generations").glob("*/RUNTIME_VERSION.json"))) == 1


def test_unknown_placement_refuses_before_publication(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch)
    shutil.rmtree(tmp_path / "pb-queue")
    with pytest.raises(SystemExit, match="no worker offers"):
        _run_publish(tmp_path, monkeypatch, checkout, [])
    assert not (tmp_path / "mirror").exists()
    assert not (tmp_path / "runtime-generations").exists()


def test_missing_image_refuses_before_publication(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch)
    monkeypatch.delenv("PBCANARY_GPU_IMAGE")
    with pytest.raises(SystemExit, match="PBCANARY_GPU_IMAGE"):
        _run_publish(tmp_path, monkeypatch, checkout, [])
    assert not (tmp_path / "runtime-generations").exists()


def test_dry_run_refuses_impossible_placement_and_preserves_the_active_runtime(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch, worker_host="another-worker")
    previous = tmp_path / "previous-runtime"
    previous.mkdir()
    mirror = tmp_path / "mirror"
    mirror.symlink_to(previous, target_is_directory=True)
    with pytest.raises(SystemExit, match="canary.*sparky"):
        _run_publish(tmp_path, monkeypatch, checkout, ["--dry-run"])
    assert mirror.resolve() == previous
    assert not (tmp_path / "runtime-generations").exists()


def test_offer_read_failure_refuses_before_publication(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch)

    def unavailable(_queue):
        raise submission.SubmissionRefused("worker-offer discovery unavailable")

    monkeypatch.setattr(submission, "bounded_offer_snapshot", unavailable)
    with pytest.raises(SystemExit, match="worker-offer discovery unavailable"):
        _run_publish(tmp_path, monkeypatch, checkout, [])
    assert not (tmp_path / "runtime-generations").exists()


def test_driver_without_placement_preflight_refuses_before_publication(tmp_path, monkeypatch):
    checkout = _publisher(tmp_path, monkeypatch)
    (checkout / "tools/fleet/pbcanary.py").write_text(
        "def run_canary(*, generation):\n    return 0\n")
    with pytest.raises(SystemExit, match="no preflight_publication"):
        _run_publish(tmp_path, monkeypatch, checkout, [])
    assert not (tmp_path / "runtime-generations").exists()
