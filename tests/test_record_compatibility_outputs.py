"""Actual outputs protocol writers and supported-generation consumers."""
from record_compat_contract import RecordContract, generations, writer_sources, pytest_generate_tests
from dataclasses import replace


class TestRecords(RecordContract):
    domain = "outputs"

    def test_each_distinct_wire_shape_reaches_its_real_field_boundaries(self, kind, variant, generations, tmp_path):
        selected = replace(kind, variants=(variant,),
                           field_policy=dict(kind.variant_field_policies).get(variant, kind.field_policy),
                           negative_field=dict(kind.variant_negative_fields).get(variant, kind.negative_field))
        self.test_unknown_fields_follow_real_consumer_policy(selected, generations, tmp_path / "unknown")
        self.test_invalid_record_reaches_both_real_consumer_boundaries(selected, generations, tmp_path / "foreign")
