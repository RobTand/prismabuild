"""A sampled claim pass is not proof that the holder-read path ran."""
from copy import deepcopy
import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def diagnostic():
    path = Path(__file__).resolve().parents[1] / "tools/maintenance/diag940_profile_coverage.py"
    spec = importlib.util.spec_from_file_location("diag940_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GENERATION = "/published/runtime-generations/immutable-generation"
POOL = GENERATION + "/src/prismabuild/pool.py"


def profile():
    return {
        "shared": {"frames": [
            {"name": "_claim_pass", "file": POOL},
            {"name": "holder_bound", "file": POOL},
            {"name": "_declared_run_bound", "file": POOL},
            {"name": "holder_bound", "file": "/unrelated/pool.py"},
        ]},
        "profiles": [
            {"name": 'Process 123 Thread 123 "MainThread"', "type": "sampled",
             "unit": "seconds", "samples": [[0], [0, 1, 2], [0, 1, 2]],
             "weights": [0.02, 0.02, 0.02]},
            {"name": 'Process 456 Thread 456 "MainThread"', "type": "sampled",
             "unit": "seconds", "samples": [[0, 1, 2]], "weights": [0.02]},
        ],
    }


def test_counts_only_the_requested_worker(diagnostic):
    result = diagnostic.summarize_profile(profile(), pid=123, generation_root=GENERATION)
    assert result["sample_count"] == 3
    assert result["functions"]["holder_bound"] == {"samples": 2, "inclusive_seconds": 0.04}
    assert result["holder_read_path_sampled"] is True
    assert result["performance_delta"] is None


def test_claim_pass_alone_does_not_establish_holder_read_cost(diagnostic):
    data = profile()
    data["profiles"][0]["samples"] = [[0], [0], [0]]
    result = diagnostic.summarize_profile(data, pid=123, generation_root=GENERATION)
    assert result["functions"]["_claim_pass"]["samples"] == 3
    assert result["holder_read_path_sampled"] is False
    assert result["absence_proves_zero_cost"] is False


def test_same_named_function_in_another_tree_does_not_count(diagnostic):
    data = profile()
    data["profiles"][0]["samples"] = [[0, 3], [0, 3], [0, 3]]
    result = diagnostic.summarize_profile(data, pid=123, generation_root=GENERATION)
    assert result["functions"]["holder_bound"]["samples"] == 0


@pytest.mark.parametrize("invalid", ["missing_worker", "bad_index", "bad_weight", "wrong_unit"])
def test_malformed_or_uncovered_profiles_refuse_by_name(diagnostic, invalid):
    data = deepcopy(profile())
    pid = 123
    if invalid == "missing_worker":
        pid = 999
    elif invalid == "bad_index":
        data["profiles"][0]["samples"][0] = [-1]
    elif invalid == "bad_weight":
        data["profiles"][0]["weights"][0] = float("nan")
    else:
        data["profiles"][0]["unit"] = "bytes"
    with pytest.raises(ValueError, match="profile"):
        diagnostic.summarize_profile(data, pid=pid, generation_root=GENERATION)
