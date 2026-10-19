"""A missing capacity observation cannot act as a known zero budget."""
import pytest

from prismabuild import residency_plan
from test_prelaunch_window_1594 import _stream_plan


def test_a_window_decision_requires_a_captured_free_budget():
    window = residency_plan.ConsumerWindow(
        _stream_plan(), None, residency_plan.LegObservation())
    with pytest.raises(TypeError, match="free_gib"):
        _ = window.decision
