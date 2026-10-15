"""A renamed box announces under its new name, not the one it booted with.

``host`` was read once, before the poll loop, and held for the life of the
process.  A box renamed under a running loop therefore kept announcing the
name it started with forever.

Measured on 2026-09-06: sparklina rebooted at 14:18, twenty loops started at
14:22, and the hostname fix landed after that.  Those twenty went on offering
``gx10-6b77`` while three later ones offered ``sparklina``, so ``pbstatus``
showed one physical box as two live nodes -- and the phantom's ADMISSION column
read ``cpu: unavailable, gpu: unavailable`` permanently, because nothing
published adaptive counters under a name no box answered to.  An action pinned
to that name would have waited for a worker that could never be admitted.  The
only cure was to retire every pre-rename loop by hand.

Nothing here touches a real queue, a real box or a real name: the hostname is
this test's own and the queue is a stub.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from unittest import mock

import pytest

from prismabuild import contracts, pool

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


from prismabuild import role_session


def _worker_loop():
    """The real entry point, loaded under a private name."""

    spec = importlib.util.spec_from_file_location("worker_loop_rename",
                                                  WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # This unit observes the announced arguments in the parent. The actual
    # child boundary has separate process/FIFO regression coverage.
    def publish_inline(announce, **_kwargs):
        announce()
        return module.PublicationResult("published", 0, None, "")
    module.publish_offer = publish_inline
    return module


class _Ledger:
    def capacity(self) -> dict[str, int]:
        return {"cpu": 2}

    def held(self) -> dict[str, int]:
        return {}

    def retire_free_capacity(self, capacity: dict[str, int]) -> None:
        pass

    def available(self) -> dict[str, int]:
        return {"cpu": 2}


def _announced(module, names: list[str], *, gang=False, generation="", gpu=False,
               extra_tags=()) -> list[dict]:
    """One poll of the real loop, with ``gethostname`` answering ``names``.

    The first answer is the startup read and the second is the re-read at the
    top of the poll, so a two-element list is exactly the window a rename lands
    in.  The list's last value repeats, because a real box does not change its
    name once per call.
    """

    calls: list[dict] = []

    class _Queue:
        #: The loop sweeps its own dead offer temporaries under ``queue.root``
        #: (#1040).  A path that cannot exist makes that sweep a skip, so a double
        #: never reads or removes anything on a real queue.
        root = Path("/nonexistent/prismabuild-test-queue")

        def __init__(self, _root) -> None:
            pass

        def ledger(self) -> _Ledger:
            return _Ledger()

        def announce(self, **kwargs) -> None:
            calls.append(kwargs)

        def ready_items(self) -> list:
            # The loop discovers the candidate list before it publishes or
            # admits; this double stands in for an empty queue.
            return []

        def serve_once(self, **_kwargs):
            return None

        def placement_census(self) -> dict:
            return {}

    answers = list(names)

    def gethostname() -> str:
        return answers.pop(0) if len(answers) > 1 else answers[0]

    argv = ["worker_loop.py", "--once", "--assume-idle",
            "--poll-s", "0", "--all-cores"]
    if gpu:
        argv.append("--gpu")
    else:
        argv.extend(("--gpu-slots", "0"))
    if gang:
        argv.append("--gang-admission")
    for tag in extra_tags:
        argv.extend(("--tag", tag))
    with tempfile.TemporaryDirectory(prefix="pb-worker-offer-") as private:
        private_root = Path(private)
        _Queue.root = private_root / "queue"
        gate = private_root / "maintenance.json"
        gate.write_text(json.dumps({"draining": False}))
        receipt = private_root / "RUNTIME_VERSION.json"
        receipt.write_text(json.dumps({"commit": "audit", "generation": generation}))
        inventory = module.container_images.InventoryCache
        trusted_sample = module.gpu_admission.trusted_sample
        with mock.patch.object(module.pool, "PoolQueue", _Queue), \
         mock.patch.dict(module.os.environ, {"PRISMABUILD_GANG_ADMISSION": "0"}), \
         mock.patch.object(module, "SH", private_root / "fleet"), \
         mock.patch.object(role_session, "MAINTENANCE_GATE", gate), \
         mock.patch.object(role_session, "PARKED_ROOT", private_root / "parked"), \
         mock.patch.object(module, "PUBLICATION_LOCK_ROOT", private_root / "offer-locks"), \
         mock.patch.object(role_session, "ROLE_LOCK_ROOT", private_root / "role-locks"), \
         mock.patch.object(role_session, "RUNTIME_VERSION", receipt), \
         mock.patch.object(role_session, "GENERATION_VERSION", receipt), \
         mock.patch.object(module.container_images, "InventoryCache", side_effect=lambda: inventory(
             root=private_root / "images", docker=str(private_root / "docker"),
             probe=lambda *_args, **_kwargs: None)), \
         mock.patch.object(module.gpu_admission, "trusted_sample", side_effect=lambda: trusted_sample(
             private_root / "gpu-capacity.json")), \
         mock.patch.object(role_session, "published_commit", return_value="audit"), \
         mock.patch.object(role_session, "loaded_runtime_commit", return_value="audit"), \
         mock.patch.object(module.os, "sched_getaffinity",
                           lambda _pid: {2, 3}), \
         mock.patch.object(module.socket, "gethostname", gethostname), \
         mock.patch.object(sys, "argv", argv):
            assert module.main() == 0
    return calls


def test_a_loop_announces_the_name_the_box_has_now() -> None:
    """The rename lands between the startup read and the poll's re-read."""

    module = _worker_loop()

    calls = _announced(module, ["gx10-6b77", "sparklina"])

    assert calls, "the loop announced nothing"
    offer = calls[-1]
    assert offer["host"] == "sparklina", (
        "the loop announced the name it booted with, so this box is two nodes")
    assert "sparklina" in offer["tags"], offer["tags"]
    assert "gx10-6b77" not in offer["tags"], (
        "the old name is still offered, so work pinned to it still matches a "
        "worker that no longer answers to it")


def test_a_box_that_was_not_renamed_announces_its_current_name() -> None:
    module = _worker_loop()

    calls = _announced(module, ["sparky"])

    offer = calls[-1]
    assert offer["host"] == "sparky"
    assert "sparky" in offer["tags"]


@pytest.mark.parametrize("gang", [False, True])
@pytest.mark.parametrize("generation", ["", "g1"])
@pytest.mark.parametrize("gpu", [False, True])
def test_worker_enablement_reaches_the_published_offer_and_claim_matcher(
        tmp_path, gang, generation, gpu):
    module = _worker_loop()
    announcement = _announced(module, ["testbox"], gang=gang,
                              generation=generation, gpu=gpu)[-1]
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(**announcement)
    offer = queue.offers()[0]
    tags = frozenset(offer["tags"])

    assert ("cpu" in tags) is (not gpu)
    assert contracts.CONTAINER_IMAGE_TAG in tags
    assert contracts.SCRATCH_IO_TAG in tags
    assert contracts.SCRATCH_LIFETIME_TAG in tags
    assert (contracts.GANG_TAG in tags) is gang
    assert (contracts.PUBLICATION_CANARY_TAG in tags) is bool(generation)
    assert {tag for tag in tags if tag.startswith("runtime-generation:")} == (
        {f"runtime-generation:{generation}"} if generation else set())
    assert queue._placement_matches(
        {"tags": [contracts.GANG_TAG]}, tags=tags, has_gpu=gpu) is gang
    assert queue._placement_matches(
        {"tags": [contracts.PUBLICATION_CANARY_TAG]}, tags=tags,
        has_gpu=gpu) is bool(generation)


@pytest.mark.parametrize("tag,generation", [
    (contracts.GANG_TAG, ""),
    (contracts.PUBLICATION_CANARY_TAG, ""),
    ("runtime-generation:forged", ""),
    ("runtime-generation:forged", "g1"),
])
def test_manual_contract_tags_refuse_before_worker_main_announces(tag, generation, capsys):
    module = _worker_loop()
    published = []
    original = module.publish_offer

    def record_publication(*args, **kwargs):
        published.append(True)
        return original(*args, **kwargs)

    with mock.patch.object(module, "publish_offer", side_effect=record_publication):
        with pytest.raises(SystemExit) as exc:
            _announced(module, ["testbox"], generation=generation, extra_tags=[tag])
    assert exc.value.code == 2
    assert tag in capsys.readouterr().err
    assert published == []


def test_manual_placement_tags_keep_enabled_code_contracts():
    module = _worker_loop()
    tags = _announced(module, ["testbox"], gang=True, generation="g1",
                      extra_tags=["custom-placement", "cpu"])[-1]["tags"]
    assert {"custom-placement", "cpu", contracts.GANG_TAG,
            contracts.PUBLICATION_CANARY_TAG, "runtime-generation:g1"} <= set(tags)
