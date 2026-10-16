"""Actual queue protocol writers and supported-generation consumers."""
from record_compat_contract import RecordContract, generations, writer_sources, pytest_generate_tests


class TestRecords(RecordContract):
    domain = "queue"
