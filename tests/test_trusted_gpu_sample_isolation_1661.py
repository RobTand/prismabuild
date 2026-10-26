"""Implicit trusted reads follow the configured private sample path."""
from prismabuild import adaptive_gpu


def test_trusted_sample_resolves_its_current_path(tmp_path, monkeypatch):
    private = tmp_path / 'gpu-capacity.json'
    selected = []

    def absent(path):
        selected.append(path)
        raise OSError('sample absent')

    monkeypatch.setattr(adaptive_gpu, 'SAMPLE_PATH', private)
    monkeypatch.setattr(adaptive_gpu, 'Path', absent)
    assert adaptive_gpu.trusted_sample() == {}
    assert selected == [private]
