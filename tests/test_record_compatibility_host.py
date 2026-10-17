"""Actual host protocol writers and supported-generation consumers."""
import pytest

from record_compat_contract import RecordContract, generations, writer_sources, pytest_generate_tests


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "host"

    def test_each_distinct_wire_shape_reaches_its_real_field_boundaries(self, kind, variant, generations, tmp_path):
        self.verify_variant_boundaries(kind, variant, generations, tmp_path)
