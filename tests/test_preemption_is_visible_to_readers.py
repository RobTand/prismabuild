"""What a preemption looks like to the people and tools waiting on it.

#364 stops an admitted background holder and re-publishes it.  That requeue is
necessarily a NEW generation -- the cancellation it revives is generation-scoped
and would otherwise cover its own retry -- and both readers of the queue are
generation-aware:

* ``pbrun`` waits on the generation it submitted, so without this it reads the
  cancellation, exits 143, and reports work the queue is in the middle of
  running again as decided against.  "Retried, not lost" would then be true of
  the queue and false of everyone waiting on it.
* ``pbstatus`` builds fixed rows and drops fields it does not name, so
  ``preempted_by`` sitting in the record is not the same thing as an operator
  being able to see it -- which is what the issue asks for.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbstatus  # noqa: E402



@pytest.fixture(autouse=True)
def known_generation_action(monkeypatch):
    # These private logical-token fixtures stand for a verified generation
    # request; individual tests override this to exercise unknown/measurement.
    monkeypatch.setattr(pool.cpu_admission, "action_identity", lambda item: ("shape", False))


FOREGROUND = uuid.uuid4().hex + uuid.uuid4().hex
BACKGROUND = uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def preempted(tmp_path: Path):
    """A queue where a foreground denial has just preempted the holder."""

    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.ledger().ensure_capacity({"gpu": 1})

    q.publish(action_key=BACKGROUND, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", priority=-10, resources={"gpu": 1},
              retry_safe=True, max_attempts=3)
    holder = q.claim(capacity={"gpu": 1})
    assert holder is not None and holder["action_key"] == BACKGROUND

    q.publish(action_key=FOREGROUND, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", priority=0, resources={"gpu": 1})
    assert q.claim(capacity={"gpu": 1}) is None

    stopped = float(holder["published_unix"])
    requeued = json.loads(q.item_path(pool.READY, BACKGROUND).read_text())
    assert float(requeued["published_unix"]) != stopped
    return q, holder, stopped


def test_a_waiter_follows_the_preemption_to_the_generation_it_requeued(
    preempted,
) -> None:
    """The stopped generation is not an ending while its retry is queued."""

    q, holder, stopped = preempted

    # Nothing has ended: the retry is waiting to run.
    assert pbrun.landed_outcome(q, BACKGROUND, wait_s=0.0,
                                generation=stopped) is None

    # The holder concludes the stop; the foreground item it yielded to runs.
    q.finish(BACKGROUND, status="withdrawn", detail={"returncode": -15},
             claim_snapshot=holder)
    first = q.claim(capacity={"gpu": 1})
    assert first is not None and first["action_key"] == FOREGROUND
    q.finish(FOREGROUND, status="executed", detail={"returncode": 0},
             claim_snapshot=first)

    # And then the retry runs and succeeds.  A waiter that named the stopped
    # generation is told about that ending, not about the stop.
    retry = q.claim(capacity={"gpu": 1})
    assert retry is not None and retry["action_key"] == BACKGROUND
    q.finish(BACKGROUND, status="executed", detail={"returncode": 0},
             claim_snapshot=retry)

    landed = pbrun.landed_outcome(q, BACKGROUND, wait_s=0.0,
                                  generation=stopped)
    assert landed is not None
    assert landed[1]["status"] == "executed"
    assert float(landed[1]["published_unix"]) == float(retry["published_unix"])


def test_an_operator_withdrawal_still_ends_the_wait(tmp_path: Path) -> None:
    """Only a preemption is followed.  A decision is still a decision."""

    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.publish(action_key=BACKGROUND, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", priority=-10)
    generation = float(json.loads(
        q.item_path(pool.READY, BACKGROUND).read_text())["published_unix"])
    q.withdraw(BACKGROUND, reason="an operator changed their mind",
               by="an operator")

    landed = pbrun.landed_outcome(q, BACKGROUND, wait_s=0.0,
                                  generation=generation)
    assert landed is not None
    assert landed[1]["status"] == "withdrawn"
    assert landed[1].get("preempted_by") is None


def test_pbstatus_names_the_preemption_on_the_ending_and_on_the_requeue(
    preempted,
) -> None:
    """The cost is readable in the tables, not only in the record."""

    q, _, _ = preempted

    endings = pbstatus.read_endings(q.root)
    withdrawn = [row for row in endings if row["status"] == "withdrawn"]
    assert len(withdrawn) == 1
    assert withdrawn[0]["preempted_by"] == FOREGROUND
    assert f"preempted by {FOREGROUND[:12]}" in "\n".join(
        pbstatus.ending_lines(endings))

    pool_state = pbstatus.read_pool(q.root)
    rows = [row for row in pool_state["jobs"]
            if row["action_key"] == BACKGROUND and row["state"] == "READY"]
    assert len(rows) == 1
    assert rows[0]["preempted_by"] == FOREGROUND
    assert f"preemption by {FOREGROUND[:12]}" in str(rows[0]["reason"])


def test_waiter_cannot_finish_between_withdrawal_and_requeue(tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from test_preemption_review_boundaries import setup_holder

    q, bg, fg, holder = setup_holder(tmp_path)
    stopped = threading.Event()
    allow_publish = threading.Event()
    reader_checkpoint = threading.Event()
    publish = q.publish
    transition = q._transition_locked
    reader_ident = []

    def pause_publish(**kw):
        if kw.get('preempted_claim') is not None:
            stopped.set()
            assert allow_publish.wait(10)
        return publish(**kw)

    def watch_transition(key, **kw):
        if reader_ident and threading.get_ident() == reader_ident[0] and key == bg:
            reader_checkpoint.set()
        return transition(key, **kw)

    def wait_for_outcome():
        reader_ident.append(threading.get_ident())
        try:
            return pbrun.landed_outcome(q, bg, wait_s=0,
                                       generation=holder['published_unix'])
        finally:
            reader_checkpoint.set()

    monkeypatch.setattr(q, 'publish', pause_publish)
    monkeypatch.setattr(q, '_transition_locked', watch_transition)
    with ThreadPoolExecutor(max_workers=2) as workers:
        writer = workers.submit(q.claim, capacity={'gpu': 1})
        try:
            assert stopped.wait(10)
            reader = workers.submit(wait_for_outcome)
            # The reader either reaches the handoff lock or prematurely returns
            # a verdict. No timing sleep decides which interleaving was tested.
            assert reader_checkpoint.wait(10)
        finally:
            allow_publish.set()
        assert writer.result(timeout=10) is None
        assert reader.result(timeout=10) is None


def test_waiter_reports_its_requeue_not_an_unrelated_later_generation(tmp_path):
    from test_preemption_review_boundaries import setup_holder
    q, bg, fg, holder = setup_holder(tmp_path, max_attempts=2)
    assert q.claim(capacity={'gpu': 1}) is None
    q.finish(bg, status='withdrawn', claim_snapshot=holder)
    foreground = q.claim(capacity={'gpu': 1})
    q.finish(fg, status='executed', claim_snapshot=foreground)
    retry = q.claim(capacity={'gpu': 1})
    q.finish(bg, status='failed', detail={'returncode': 1}, claim_snapshot=retry)
    assert q.item_path(pool.FAILED, bg).exists()
    q.publish(action_key=bg, cas_root=tmp_path / 'cas', checkout_root=tmp_path,
              worker_script=tmp_path / 'worker.py', priority=0, resources={'gpu': 1})
    unrelated = q.claim(capacity={'gpu': 1})
    q.finish(bg, status='executed', claim_snapshot=unrelated)
    landed = pbrun.landed_outcome(q, bg, wait_s=0, generation=holder['published_unix'])
    assert landed is not None
    assert landed[1]['published_unix'] == retry['published_unix']
    assert landed[1]['status'] == 'failed'


@pytest.mark.parametrize('status', ['failed', 'executed'])
@pytest.mark.parametrize('interruptions', [1, 2])
def test_late_waiter_recovers_overwritten_retry_from_immutable_attempts(
        tmp_path, status, interruptions):
    from test_preemption_review_boundaries import setup_holder
    q, bg, fg, holder = setup_holder(tmp_path, max_attempts=interruptions + 1)
    original_generation = holder['published_unix']
    for index in range(interruptions):
        assert q.claim(capacity={'gpu': 1}) is None
        q.finish(bg, status='withdrawn', claim_snapshot=holder)
        foreground = q.claim(capacity={'gpu': 1})
        q.finish(fg, status='executed', claim_snapshot=foreground)
        holder = q.claim(capacity={'gpu': 1})
        if index + 1 < interruptions:
            q.publish(action_key=fg, cas_root=tmp_path / 'cas', checkout_root=tmp_path,
                      worker_script=tmp_path / 'worker.py', resources={'gpu': 1})
    retry = holder
    retry_code = 7 if status == 'failed' else 0
    q.finish(bg, status=status, detail={'returncode': retry_code, 'stdout': 'causal retry'},
             claim_snapshot=retry)
    q.publish(action_key=bg, cas_root=tmp_path / 'cas', checkout_root=tmp_path,
              worker_script=tmp_path / 'worker.py', priority=0, resources={'gpu': 1},
              max_attempts=1, retry_safe=False)
    unrelated = q.claim(capacity={'gpu': 1})
    q.finish(bg, status=status, detail={'returncode': 9 if status == 'failed' else 0,
                                      'stdout': 'unrelated later run'},
             claim_snapshot=unrelated)
    terminal = q.item_path(pool.FAILED if status == 'failed' else pool.DONE, bg)
    assert json.loads(terminal.read_text())['published_unix'] == unrelated['published_unix']
    landed = pbrun.landed_outcome(q, bg, wait_s=0, generation=original_generation)
    assert landed is not None
    assert landed[1]['published_unix'] == retry['published_unix']
    summary = pbrun.outcome_summary(q, *landed)
    assert summary['status'] == status
    assert summary['returncode'] == retry_code
    assert summary['detail']['stdout'] == 'causal retry'
    assert landed[0] == q.attempt_path(landed[1], landed[1]['attempts'])
    assert landed[0].exists()
    # Recovery is read-only: it must not replace G3's current summary with G2.
    assert json.loads(terminal.read_text())['published_unix'] == unrelated['published_unix']


@pytest.mark.parametrize('damage', ['lineage', 'log'])
def test_archived_retry_recovery_revalidates_immutable_evidence(preempted, damage):
    q, holder, stopped = preempted
    q.finish(BACKGROUND, status='withdrawn', claim_snapshot=holder)
    foreground = q.claim(capacity={'gpu': 1})
    q.finish(FOREGROUND, status='executed', claim_snapshot=foreground)
    retry = q.claim(capacity={'gpu': 1})
    q.finish(BACKGROUND, status='executed', detail={'stdout': 'verified original'},
             claim_snapshot=retry)
    recovered = q.archived_preemption_outcomes(BACKGROUND)
    assert len(recovered) == 1
    path, record = recovered[0]
    outcome = json.loads(path.read_text())
    if damage == 'lineage':
        outcome['preemption_context']['supersedes_withdrawal']['published_unix'] += 1
        raw = json.dumps(outcome).encode()
    else:
        path = q.root / outcome['logs']['stdout']['path']
        raw = b'changed after publication'
    path.chmod(0o644)
    path.write_bytes(raw)
    path.chmod(0o444)
    with pytest.raises(pool.PoolContractError):
        q.archived_preemption_outcomes(BACKGROUND)
