"""Actual queue protocol writers and supported-generation consumers."""
import pytest

from record_compat_contract import RecordContract, generations, writer_sources, pytest_generate_tests


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "queue"


@pytest.mark.parametrize("case", ["q-item/plain", "q-attempt/executed", "q-outcome/executed"])
def test_submission_phase_fields_are_additive_for_retained_readers(case, generations, tmp_path):
    import json
    from record_compat import accepted, invoke
    from record_compat_contract import ROOT, GOLDENS, decoded
    original = decoded(GOLDENS[case])
    record = json.loads(original)
    record["phases"] = {"cas_stage": 1.25, "queue_publication": 2.0,
                        "transition_lock_wait": 15.5}
    current = json.dumps(record).encode()
    for generation, old, manifest in generations:
        # Old readers accept the additive diagnostic. New readers continue to
        # accept the original record with no diagnostic field.
        accepted(invoke(old, case, tmp_path / generation / "old", raw=current, manifest=manifest))
        accepted(invoke(ROOT, case, tmp_path / generation / "new", raw=original))


def test_timed_supersession_preserves_both_reader_directions(generations, tmp_path):
    import json
    from prismabuild import pool
    from record_compat import accepted, invoke
    from record_compat_contract import ROOT, GOLDENS, decoded, has_historical_participant
    case = "publication-ending/initial"
    old_raw = decoded(GOLDENS[case])
    reference = json.loads(old_raw)
    queue = pool.PoolQueue(tmp_path / "timed-writer")
    original = {"action_key": reference["action_key"],
                "published_unix": reference["published_unix"],
                "phases": {"queue_publication": 2.0, "transition_lock_wait": 15.5}}
    ending = queue._persist_publication_supersession(original, successor=reference["successor"])
    current = ending.path.read_bytes()
    assert set(json.loads(current)) == set(reference)
    assert current == old_raw
    supported = 0
    for generation, old, manifest in generations:
        if not has_historical_participant(case, generation, old, manifest):
            continue
        supported += 1
        accepted(invoke(old, case, tmp_path / generation / "old", raw=current, manifest=manifest))
        accepted(invoke(ROOT, case, tmp_path / generation / "new", raw=old_raw))
    assert supported
