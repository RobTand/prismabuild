"""``cpu: stale`` in the status view is downstream of placement, not upstream.

Issue #205 recorded a contradiction on the live fleet: ``dl380g10`` was
``live`` with a 64-second offer and ``{'cpu': 80, 'mem_gb': 192}`` free, three
READY items of ``cpu: 1 / mem_gb: 8`` were unplaceable on it, and its
``ADMISSION`` column read ``cpu: stale`` while its ``reservations/dl380g10/
adaptive`` directory had just been written.  The natural reading -- a stale
admission verdict excluding a box that could plainly fit the work -- is not
what the code does, and these tests pin the two halves of why.

**Nothing reads the admission sample when deciding placement.**  ``offers()``,
``_matching_offers()``, ``placeable()`` and ``placeable_hosts()`` never open
``adaptive/cpu-sample.json``.  The only freshness that gates placement is the
offer's own ``announced_unix`` against ``OFFER_TIMEOUT_S``.

**The sample is written on the placement-success path.**  The writer is
``adaptive_cpu.Controller.sample()``, reached only through ``decision()``,
reached only from ``claim()`` -- and ``claim()`` skips every item that fails
``_placement_matches`` *before* it constructs a demand or asks the controller.
A box that matches nothing therefore never samples, so its record never
advances, so the status view reports it stale for as long as the starvation
lasts.  The staleness is the *shadow* of the exclusion; reading it as the
cause points an operator at the admission controller, which is where #205's
investigation went.

Measured on the live queue, 2026-09-06 16:03-16:12 UTC: ``dl380g10`` re-wrote
``workers/dl380g10.json`` at 16:07:26 and 16:11:50 while every file under
``reservations/dl380g10/adaptive/`` stayed frozen at 15:48:55 / 15:50:23, and
all three READY items carried ``tags: ["sparky"]``.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))

from prismabuild import pool  # noqa: E402
import pbstatus  # noqa: E402

#: The live fleet's shape at the incident (``tools/fleet/fleet_boxes.json``).
GB10 = {"cpu": 20, "gpu": 1, "mem_gb": 72}
X86 = {"cpu": 80, "gpu": 0, "mem_gb": 192}

#: An item of this size "cannot fail to fit" ``X86`` -- the issue's words.
SMALL_CPU_WORK = {"cpu": 1, "mem_gb": 8}


def _fleet(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    queue.announce(host="sparky", tags=["gb10", "sparky"], has_gpu=True,
                   capacity=GB10, observed_capacity=GB10)
    queue.announce(host="dl380g10", tags=["cpu", "dl380g10", "x86"],
                   has_gpu=False, capacity=X86, observed_capacity=X86)
    return queue


def _publish(queue: pool.PoolQueue, seed: str, *, tags: list[str],
             resources: dict | None = None) -> str:
    key = seed * 64
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=str(queue.root),
                  worker_script=ROOT / "tools/prismabuild_worker.py",
                  resources=dict(resources or SMALL_CPU_WORK),
                  tags=tags, needs_gpu=False)
    return key


def _stale_admission(queue: pool.PoolQueue, host: str) -> Path:
    """Give ``host`` an admission sample far older than ``MAX_SAMPLE_AGE_S``.

    Written the way the controller writes it, so the reader under test sees a
    real record rather than a shape invented for the assertion.
    """

    base = queue.ledger(host).base / "adaptive"
    base.mkdir(parents=True, exist_ok=True)
    path = base / "cpu-sample.json"
    stale = time.time() - 10 * pool.cpu_admission.MAX_SAMPLE_AGE_S
    path.write_text(json.dumps({"cpus": {}, "psi_total": 0,
                                "sampled_unix": stale, "observation": {}}))
    return path


def test_a_stale_admission_sample_does_not_remove_a_box_from_placement(
        tmp_path: Path) -> None:
    """The mechanism #205 proposed, stated as an assertion.

    ``dl380g10`` has the capacity, a fresh offer, and an admission sample ten
    times past ``MAX_SAMPLE_AGE_S``.  If admission staleness gated placement,
    the box would drop out of ``placeable_hosts``.  It does not: the status
    view will call it ``cpu: stale`` in the same breath as the queue calls it
    placeable.
    """

    queue = _fleet(tmp_path)
    _stale_admission(queue, "dl380g10")
    item = {"tags": [], "needs_gpu": False, "resources": SMALL_CPU_WORK}

    assert queue.placeable_hosts(item) == ["dl380g10", "sparky"]
    assert queue.placeable(item) is True

    census = pbstatus.read_pool(queue.root)
    box = next(n for n in census["nodes"] if n["node"] == "dl380g10")
    assert box["state"] == "live" and box["healthy"] is True
    assert box["admission"]["cpu"]["state"] == "stale"


def test_the_offer_clock_is_the_only_freshness_that_gates_placement(
        tmp_path: Path) -> None:
    """Offer age excludes; sample age does not.  One clock, not two.

    ``OFFER_TIMEOUT_S`` is 120 s and ``MAX_SAMPLE_AGE_S`` is 5 s, and only the
    first is consulted by ``offers()``.  This is the half of #205 that is a
    real placement gate -- ``sparklina`` sat at a 563-second offer age at the
    observation and was excluded from ``live`` for exactly this reason.
    """

    queue = _fleet(tmp_path)
    item = {"tags": [], "needs_gpu": False, "resources": SMALL_CPU_WORK}
    assert pool.cpu_admission.MAX_SAMPLE_AGE_S < pool.OFFER_TIMEOUT_S

    # Older than the sample window, younger than the offer window: still placeable.
    offer = queue.root / pool.WORKERS / "dl380g10.json"
    record = json.loads(offer.read_text())
    record["announced_unix"] = time.time() - 30.0
    offer.write_text(json.dumps(record))
    assert "dl380g10" in (queue.placeable_hosts(item) or [])

    # Past the offer window: excluded, and now the reason is legible.
    record["announced_unix"] = time.time() - (pool.OFFER_TIMEOUT_S + 30.0)
    offer.write_text(json.dumps(record))
    assert queue.placeable_hosts(item) == ["sparky"]


def test_a_box_that_matches_nothing_never_advances_its_admission_sample(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The reversed arrow, end to end, with a positive control.

    Every READY item carries ``tags: ["sparky"]`` -- the shape the live queue
    held at the observation, where the pin came from a submitter-local
    interpreter (``/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python``) and
    ``pbrun.placement_tags`` kept the source-host pin.

    ``claim()`` is entered with ``adaptive_cpu=True``, so the CPU controller is
    really constructed and really holds its lock.  It is still never *asked*:
    ``_claim`` skips each item at ``_placement_matches`` before it reaches
    ``controller.decision()``, so ``sample()`` never runs and the record the
    status view reads cannot advance -- however healthy and idle the box is.
    The same rig on the box the tags DO name advances it on the first poll.
    That contrast is the claim: the sample tracks *matching*, not health, so
    reading it as a health signal inverts cause and effect.  It is the exact
    "fresh file behind a stale verdict" the issue could not explain.
    """

    queue = _fleet(tmp_path)
    _publish(queue, "a", tags=["sparky"])
    _publish(queue, "b", tags=["sparky"])
    tiers = {"preferred": [0], "fallback": [1]}

    monkeypatch.setattr(pool.socket, "gethostname", lambda: "dl380g10")
    sample = _stale_admission(queue, "dl380g10")
    before = json.loads(sample.read_text())["sampled_unix"]
    for _ in range(3):
        assert queue.claim(tags=["cpu", "dl380g10", "x86"], has_gpu=False,
                           owner="dl380g10", capacity=X86, cpu_tiers=tiers,
                           adaptive_cpu=True) is None

    assert json.loads(sample.read_text())["sampled_unix"] == before
    # ...and no pass was recorded either: the item was skipped before the
    # controller was ever asked, so even the denial counter stays silent.  A
    # box starved this way leaves no trace anywhere but a verdict that names
    # the one subsystem which never ran.
    assert queue.passes("a" * 64) == 0

    census = pbstatus.read_pool(queue.root)
    box = next(n for n in census["nodes"] if n["node"] == "dl380g10")
    assert box["healthy"] is True
    assert box["admission"]["cpu"]["state"] == "stale"
    waiting = [j for j in census["jobs"] if j["state"] == "READY"]
    assert waiting and all(j["placeable_hosts"] == ["sparky"] for j in waiting)

    # Positive control: the box the tags DO name reaches the controller on its
    # first poll, and its sample advances whether or not the claim succeeds.
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    matched = _stale_admission(queue, "sparky")
    stale_on_sparky = json.loads(matched.read_text())["sampled_unix"]
    queue.claim(tags=["gb10", "sparky"], has_gpu=False, owner="sparky",
                capacity=GB10, cpu_tiers=tiers, adaptive_cpu=True)
    assert json.loads(matched.read_text())["sampled_unix"] > stale_on_sparky
