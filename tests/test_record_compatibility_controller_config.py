"""Actual controller config records and supported-generation consumers."""
import pytest

from record_compat_contract import RecordContract, generations, writer_sources, pytest_generate_tests


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "controller"
    family_group = "config"
