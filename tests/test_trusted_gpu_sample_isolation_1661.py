"""Implicit capacity observations never select the live GPU broker path."""
from prismabuild import adaptive_gpu, box_capacity


def test_trusted_gpu_sample_default_uses_the_private_test_path(tmp_path, monkeypatch):
    selected = []

    def absent_sample(path):
        selected.append(path)
        raise OSError("private sample is absent")

    monkeypatch.setattr(adaptive_gpu, "Path", absent_sample)
    assert box_capacity.trusted_gpu_sample() is None
    expected = tmp_path / "live-guard" / "gpu-capacity.json"
    assert selected == [expected]
    assert adaptive_gpu.SAMPLE_PATH == expected
