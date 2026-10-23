"""Causal check of the older positive history fixture's 10 ms window."""
import time

import pytest

from prismabuild import core as pb
import test_gpu_review_r3 as prior
from test_gpu_review_r3 import generic_gpu  # noqa: F401
from test_resource_broker import authority, monitored  # noqa: F401


@pytest.mark.parametrize('window', [0.01, 10.0])
def test_positive_history_with_delay_over_ten_ms(generic_gpu, monitored, monkeypatch, window):
    a = monitored[0]
    handle = a.handle
    def delayed(uid, pid, request):
        if request['op'] == 'gpu_observation':
            time.sleep(0.03)  # Deliberately cross only the synthetic 10 ms deadline.
        return handle(uid, pid, request)
    monkeypatch.setattr(a, 'handle', delayed)
    monkeypatch.setattr(pb, 'GPU_OBSERVATION_TIMEOUT_S', window)
    if window == 0.01:
        with pytest.raises((AssertionError, KeyError)) as error:
            prior.test_q3_core_and_pool_land_in_the_same_outcome(
                generic_gpu, monitored, monkeypatch, 'all_success_zero')
        assert (error.value.args == ('gpu_record',)
                or 'Core judged unobserved' in str(error.value))
    else:
        prior.test_q3_core_and_pool_land_in_the_same_outcome(
            generic_gpu, monitored, monkeypatch, 'all_success_zero')
