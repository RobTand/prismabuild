"""Actual controller floor records and supported-generation consumers."""
import json

import pytest

from record_compat_contract import HISTORY, ROOT, RecordContract, decoded, generations, writer_sources, pytest_generate_tests
from record_compat import accepted, invoke, write


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "controller"
    family_group = "floor"


def test_floor_sample_cannot_reuse_another_coverage_capture(tmp_path):
    case = "floor-sample/idle"
    raw = write(ROOT, case, tmp_path / "writer")
    observed = invoke(ROOT, case, tmp_path / "reader", raw=raw)
    assert accepted(observed)["allowed"] is True
    body = json.loads(raw)
    body["coverage_sha256"] = "0" * 64
    rejected = invoke(ROOT, case, tmp_path / "foreign-coverage", raw=json.dumps(body).encode())
    assert rejected["returncode"] == 1 and not rejected.get("attestation_error"), rejected
    assert rejected["catalogued_entry_verified"] and rejected["consumer_boundary"], rejected
    assert "sample coverage changed" in rejected["error"]["message"], rejected


@pytest.mark.parametrize("case,reason", [("floor-binding/registered", "mapping_unknown"),
                                       ("floor-sample/idle", "population_unknown")])
def test_legacy_floor_records_hold_without_current_authority(case, reason, generations, tmp_path):
    for generation, _, _ in generations:
        raw = decoded(HISTORY[generation, case])
        observed = invoke(ROOT, case, tmp_path / generation, raw=raw)
        result = accepted(observed)
        assert observed["catalogued_entry_verified"] and observed["consumer_boundary"], observed
        assert result["allowed"] is False and result["reason"] == reason, result
