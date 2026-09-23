"""A released consumer's row is refused where live code runs (#954).

#945 lets an operator release an ``unpublished`` declaration once no live code
can publish its key, and from then on ``declare_origin_consumer`` refuses the
key. A ``pbrun`` older than #945 is the one submitter left that can still
publish it: it declares without the transition lock and without the release
check, so a release can land between its declaration and its row. That row
reads a batch the retirement tick may already be deleting.

The release is therefore enforced where live code always runs:

*   **The claim fails the row.** ``PoolQueue._claim`` lists the queue-wide
    release index once per scan, confirms a listed key against its release
    record under the key's transition lock, and files the ready row as failed
    (``origin_consumer_released``, refusal ``origin-consumer-released``).
*   **The tier loop stages nothing for it.** ``tier_loop.live_consumers``
    leaves a released ready consumer out, so no phase of its frozen plan is
    published, not even its lead.
*   **The index precedes the record.** A record the claim's listing could not
    see would let a row run while the tick deletes its batch. An entry with no
    record is a release that stopped, and refuses nothing; running the release
    again completes it, and gives a #945-era record its entry.
*   **Unknown is not a verdict.** A release that cannot be read denies the
    claim and leaves the row ready.
*   **A crash while filing it recovers.** The row is captured before its
    ending is written; the transition sweep restores a capture with no ending,
    and the next claim files it.

Fixture concessions: owners and consumers are published, claimed and finished
through the real ``PoolQueue``; submissions and operator releases go through
the real ``pbrun.main``; the tier loop is one real ``tier_loop.cycle``. The
older ``pbrun`` is today's with ``submission_window`` a no-op, paused where it
writes its row and resumed with the same publication; a publish is the
fleet's ``repo`` link moving to another generation.
"""
from __future__ import annotations

import contextlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import adaptive_cpu, pool, produced_output as po  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
import pbrun  # noqa: E402
from test_consumed_origin_retirement import (  # noqa: E402
    _commit, _queue, _template,
)
from test_an_unpublished_declaration_is_released_once_nothing_can_publish_it import (  # noqa: E402
    _consumer_argv, _publish, _release, _release_argv,
)
from test_pbrun_residency_stage_submission import (  # noqa: E402
    _announce_tier, _tier_cycle,
)
from test_superseded_origin_consumers import (  # noqa: E402
    TAGS, _events, _manifest, _pbrun_env, _refused,
)

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

RETIRED = po.ORIGIN_RETIRED_EVENT


# -- fixtures ------------------------------------------------------------------


def _old_submitter(monkeypatch, *argv: str):
    """A ``pbrun`` from before #945, paused between its declaration and its row.

    It declares with no lock, as that code did. Returns its key and the call
    that writes the row it was about to write.
    """

    paused: list[tuple[str, object, dict, dict]] = []

    def pause(name):
        def publish(q, publication, **kwargs):
            paused.append((name, q, dict(publication), kwargs))
            raise SystemExit("old submitter paused before its row")
        return publish

    with monkeypatch.context() as patch:
        patch.setattr(pbrun, "submission_window",
                      lambda q, key, refs: contextlib.nullcontext(True))
        patch.setattr(pbrun, "publish_or_refuse", pause("publish_or_refuse"))
        patch.setattr(pbrun, "publish_or_attach", pause("publish_or_attach"))
        assert "paused before its row" in _refused(monkeypatch, *argv)
    [(name, q, publication, kwargs)] = paused
    key = str(publication["action_key"])
    assert po._key_generation(q, key)[0] == "absent"

    def resume() -> None:
        getattr(pbrun, name)(q, publication, **kwargs)
        assert q.item_path(pool.READY, key).exists()

    return key, resume


def _declared_by_old_code(tmp_path: Path, monkeypatch, *options: str,
                          stage: Path | None = None):
    """A committed batch, and an old submitter paused after declaring it."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    if stage is not None:
        _announce_tier(queue, mountpoint=stage)
    instance, path, committed = _commit(queue, template, "band-l")
    queue.finish(instance["owner_action_key"], status="executed")
    work = _pbrun_env(tmp_path, queue, monkeypatch)
    (tmp_path / "repo").symlink_to(pbrun.RUNTIME_ROOT)
    manifest = _manifest(tmp_path, queue, committed)
    argv = _consumer_argv(work, manifest, "true")
    argv = (*argv[:argv.index("--")], *options, *argv[argv.index("--"):])
    key, resume = _old_submitter(monkeypatch, *argv)
    assert (po._consumers_dir(queue.root, instance, "b1")
            / f"{key}.json").exists(), "declared before it paused"
    return queue, instance, path, committed, key, resume


def _released_while_paused(tmp_path: Path, monkeypatch, capsys, *options: str,
                           stage: Path | None = None):
    """The same, with the declaration released after a publish."""

    queue, instance, path, committed, key, resume = _declared_by_old_code(
        tmp_path, monkeypatch, *options, stage=stage)
    _publish(tmp_path, "g-next")
    answer = _release(monkeypatch, capsys, committed, key)
    assert (answer["released"], answer["state"]) == (True, "unpublished")
    return queue, instance, path, committed, key, resume


def _failed(queue, key: str) -> dict:
    record = pool._read_json(queue.item_path(pool.FAILED, key))
    assert record is not None, "the row was not filed as failed"
    return record


def _assert_refused(queue, key: str, committed: dict) -> None:
    record = _failed(queue, key)
    assert record["status"] == "origin_consumer_released"
    assert record["detail"]["refusal"] == "origin-consumer-released"
    assert record["detail"]["ref"] == committed["ref"]
    assert record["detail"]["released_state"] == "unpublished"
    assert isinstance(record["published_unix"], (int, float))
    assert not queue.item_path(pool.READY, key).exists()
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert list(queue.superseded_dir().glob(f"{key}.*.origin-released.json"
                                            ".ready-source")), (
        "the ready bytes are kept as evidence")


def _denial(queue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values()
                if value["action_key"] == key)


def _index_entries(queue, key: str) -> list[Path]:
    return sorted(queue.released_origin_consumers_dir().glob(f"{key}.*.json"))


def _movers_published(queue, plan: dict) -> list[str]:
    return [mover for mover in residency_plan.mover_keys(plan)
            if any(queue.item_path(state, mover).exists()
                   for state in (pool.READY, pool.CLAIMED, pool.DONE,
                                 pool.FAILED))]


# -- acceptance ----------------------------------------------------------------


def test_the_claim_fails_a_row_published_after_its_release(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Declared with no lock, released, then queued: failed at claim, batch retires."""

    queue, instance, path, committed, key, resume = _released_while_paused(
        tmp_path, monkeypatch, capsys)
    assert [entry.name[:64] for entry in _index_entries(queue, key)] == [key]

    resume()
    # Queued, the key reads as live and holds the batch, release or not.
    assert _events(queue, RETIRED) == []
    assert path.exists()

    assert queue.claim(owner="w-consumer", tags=list(TAGS)) is None
    _assert_refused(queue, key, committed)

    [retired] = _events(queue, RETIRED)
    assert retired["consumers"] == [{"action_key": key, "state": "released"}]
    assert not path.exists()


def test_the_tier_loop_stages_nothing_for_a_released_consumer(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """A staged row published after its release: no phase is ever published."""

    stage = tmp_path / "stage"
    queue, instance, path, committed, key, resume = _released_while_paused(
        tmp_path, monkeypatch, capsys, "--residency", "stage", stage=stage)
    plan = residency_plan.read(queue, key)
    assert plan is not None and residency_plan.mover_keys(plan), (
        "the old submitter froze its window before its row")

    resume()
    assert queue.item_path(pool.READY, key).exists()
    _tier_cycle(queue, stage)
    assert _movers_published(queue, plan) == [], (
        "a released consumer's lead was published")
    assert not any(stage.rglob(path.name)), "bytes were staged from the batch"

    assert queue.claim(owner="w-consumer", tags=list(TAGS)) is None
    _assert_refused(queue, key, committed)
    _tier_cycle(queue, stage)
    assert _movers_published(queue, plan) == []
    assert not any(stage.rglob(path.name))
    assert residency_plan.read(queue, key) is None, (
        "the dead-consumer sweep archives the plan")
    assert not path.exists(), "the tier cycle's tick retired the batch"


def test_the_index_entry_precedes_the_record_and_a_rerun_completes_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """No record without an entry; an entry alone refuses nothing; a rerun fills in."""

    queue, instance, path, committed, key, resume = _declared_by_old_code(
        tmp_path, monkeypatch)
    _publish(tmp_path, "g-next")
    released_dir = po._released_consumers_dir(queue.root, instance, "b1")

    # The entry cannot be filed: nothing is released.
    real = pool._publish_immutable

    def no_index(path_, raw, *, where):
        if Path(path_).parent == queue.released_origin_consumers_dir():
            raise pool.PoolContractError("index write refused")
        return real(path_, raw, where=where)

    with monkeypatch.context() as patch:
        patch.setattr(pool, "_publish_immutable", no_index)
        assert "origin-consumer-release-conflict" in _refused(
            monkeypatch, *_release_argv(committed, key))
    assert not (released_dir / f"{key}.json").exists(), (
        "a release record was filed that the claim's listing cannot see")

    # The record cannot be filed: the entry stands alone, which is no release.
    def no_record(path_, raw, *, where):
        if Path(path_).parent == released_dir:
            raise pool.PoolContractError("record write refused")
        return real(path_, raw, where=where)

    with monkeypatch.context() as patch:
        patch.setattr(pool, "_publish_immutable", no_record)
        assert "origin-consumer-release-conflict" in _refused(
            monkeypatch, *_release_argv(committed, key))
    assert len(_index_entries(queue, key)) == 1
    assert not (released_dir / f"{key}.json").exists()
    assert po.origin_consumer_release(queue, key) is None

    # Run again, it completes; a #945-era record without its entry gets one.
    for entry in _index_entries(queue, key):
        entry.unlink()
    with monkeypatch.context() as patch:
        patch.setattr(po, "_index_release", lambda *_args: None)
        assert _release(monkeypatch, capsys, committed, key)["released"] is True
    assert _index_entries(queue, key) == []
    assert _release(monkeypatch, capsys, committed, key)["released"] is False
    assert len(_index_entries(queue, key)) == 1
    assert po.origin_consumer_release(queue, key)["state"] == "unpublished"

    resume()
    assert queue.claim(owner="w-consumer", tags=list(TAGS)) is None
    _assert_refused(queue, key, committed)


def test_an_entry_without_its_record_leaves_the_row_to_run(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """A release that stopped before its record: the consumer runs and holds the batch."""

    queue, instance, path, committed, key, resume = _declared_by_old_code(
        tmp_path, monkeypatch)
    po._index_release(queue, key, po._checked_origin_ref(committed["ref"]))
    assert len(_index_entries(queue, key)) == 1

    resume()
    claimed = queue.claim(owner="w-consumer", tags=list(TAGS))
    assert claimed is not None and claimed["action_key"] == key
    assert not queue.item_path(pool.FAILED, key).exists()
    assert _events(queue, RETIRED) == [] and path.exists()
    queue.finish(key, status="executed")
    [retired] = _events(queue, RETIRED)
    assert retired["consumers"] == [{"action_key": key, "state": "succeeded"}]


def test_an_unreadable_release_denies_the_claim_and_fails_nothing(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Unknown is neither a release nor its absence: the row stays ready."""

    queue, instance, path, committed, key, resume = _declared_by_old_code(
        tmp_path, monkeypatch)
    queue.ensure_layout()
    (queue.released_origin_consumers_dir() / f"{key}.{'0' * 64}.json"
     ).write_text("{not json")

    resume()
    assert queue.claim(owner="w-consumer", tags=list(TAGS)) is None
    assert queue.item_path(pool.READY, key).exists()
    assert not queue.item_path(pool.FAILED, key).exists()
    assert _denial(queue, key)["reason"] == "origin_consumer_release_unreadable"


def test_a_crash_while_filing_the_refusal_is_recovered_and_filed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The captured row comes back to ready, and the next claim files it."""

    queue, instance, path, committed, key, resume = _released_while_paused(
        tmp_path, monkeypatch, capsys)
    resume()

    def crash(*_args, **_kwargs):
        raise OSError("worker died filing the ending")

    with monkeypatch.context() as patch:
        patch.setattr(pool, "_write_json_atomic", crash)
        with pytest.raises(OSError, match="worker died"):
            queue.claim(owner="w-consumer", tags=list(TAGS))
    assert not queue.item_path(pool.READY, key).exists()
    assert not queue.item_path(pool.FAILED, key).exists()

    assert key in queue.sweep_ready_transitions(grace_s=-1.0)
    assert queue.item_path(pool.READY, key).exists()
    assert queue.claim(owner="w-consumer", tags=list(TAGS)) is None
    _assert_refused(queue, key, committed)
