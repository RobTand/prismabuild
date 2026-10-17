"""A listed historical terminal cannot disappear on an unavailable read."""
import pytest

from prismabuild import pool, record_store
from test_lifecycle_recovery_crashes import KEY, _history, _interrupt_after


@pytest.mark.parametrize("observation", ["unknown", "listed-empty"])
def test_historical_terminal_retirement_keeps_unknown_proof(tmp_path, monkeypatch, observation):
    queue, item, _ = _history(tmp_path, False)
    _interrupt_after(queue, item, tmp_path, "publication", False)
    tombstones = list(queue.records.finishing_paths())
    assert len(tombstones) == 1
    path = queue.records.item_path(pool.record_store.FAILED, KEY)
    original = path.read_bytes()
    read = queue.records.confirmed_item

    def unavailable(state, key, **kwargs):
        if state == pool.record_store.FAILED and key == KEY:
            return (record_store.Unknown("unavailable", error=OSError("read fault"))
                    if observation == "unknown" else record_store.Absent())
        return read(state, key, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(queue.records, "confirmed_item", unavailable)
        assert queue.sweep_finish_tombstones(grace_s=-1) == []
    assert list(queue.records.finishing_paths()) == tombstones
    assert path.read_bytes() == original
    assert queue.sweep_finish_tombstones(grace_s=-1) == [KEY]
    assert not path.exists()
    assert queue.ledger().held() == {}
    assert pool.record_store.read_document(queue.records.item_path(pool.record_store.DONE, KEY))[
        "published_unix"] == item["published_unix"]


def test_recovery_retires_a_positively_archived_unstamped_terminal(tmp_path):
    queue, item, _ = _history(tmp_path, False)
    path = queue.records.item_path(pool.record_store.FAILED, KEY)
    legacy = {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": KEY,
              "status": "failed", "legacy": True}
    queue.records.replace_record_at(path, legacy)
    _interrupt_after(queue, item, tmp_path, "publication", False)
    assert queue.sweep_finish_tombstones(grace_s=-1) == [KEY]
    assert not path.exists()
    assert queue.ledger().held() == {}
    assert any(record.get("legacy") is True
               for _, record in queue.records.superseded_records(KEY) if record is not None)


@pytest.mark.parametrize("binding", ["superseded_by_published_unix", "superseded_by_state"])
def test_historical_retirement_rejects_an_archive_bound_to_another_conclusion(tmp_path, binding):
    queue, item, _ = _history(tmp_path, False)
    _interrupt_after(queue, item, tmp_path, "publication", False)
    terminal = pool.record_store.read_document(queue.records.item_path(pool.record_store.DONE, KEY))
    archive = next(path for path, _ in queue.records.superseded_records(KEY)
                   if str(path) == terminal["supersedes_terminal"]["superseded_path"])
    original = archive.read_bytes()
    other = pool.record_store.read_document(archive)
    other[binding] = -1 if binding == "superseded_by_published_unix" else "failed"
    queue.records.replace_record_at(archive, other)
    tombstones = list(queue.records.finishing_paths())
    prior = queue.records.item_path(pool.record_store.FAILED, KEY)
    assert queue.sweep_finish_tombstones(grace_s=-1) == []
    assert prior.exists() and list(queue.records.finishing_paths()) == tombstones
    archive.write_bytes(original)
    assert queue.sweep_finish_tombstones(grace_s=-1) == [KEY]
    assert not prior.exists()
