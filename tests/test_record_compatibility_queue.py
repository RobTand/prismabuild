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
