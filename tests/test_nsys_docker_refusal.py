"""Native Nsys cannot instrument a workload started by the Docker daemon."""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb
from test_docker_shim_global_options import _shim


@pytest.mark.parametrize("mode", ["nsys", "nsys:30"])
@pytest.mark.parametrize("argv", [
    ["run", "--rm", "gpu-image", "python3", "cuda.py"],
    ["--context=default", "run", "gpu-image"],
    ["-cdefault", "container", "run", "gpu-image"],
    ["create", "gpu-image"],
    ["--debug", "container", "create", "gpu-image"],
    ["exec", "owned-container", "python3", "cuda.py"],
    ["--context", "default", "container", "exec", "owned-container", "true"],
    ["start", "old-container"],
    ["--debug", "container", "start", "old-container"],
    ["compose", "-f", "compose.yml", "up", "-d"],
])
def test_nsys_refuses_before_contacting_the_daemon(tmp_path, mode, argv):
    backend = pb.NsysProfileBackend().bind("30") if ":" in mode else pb.NsysProfileBackend()
    session = pb._ProfileSession(mode=mode, backend=backend, directory=tmp_path / "profile")
    # Use the environment the real profile launch propagates through shells
    # and Python launchers; inspecting only the action's outer argv misses them.
    environment = session.environment({"TMPDIR": str(tmp_path)})
    result, marked, forwarded = _shim(tmp_path, argv, docker_env=environment)
    assert result.returncode == 125, result.stderr
    assert "--profile nsys" in result.stderr
    assert "Docker daemon" in result.stderr
    assert "native" in result.stderr
    assert forwarded is None
    assert not marked
    assert not (tmp_path / "inspected.json").exists()


def test_nsys_does_not_take_over_a_sealed_profile_guard(tmp_path):
    session = pb._ProfileSession(
        mode="nsys", backend=pb.NsysProfileBackend(), directory=tmp_path / "profile"
    )
    with pytest.raises(pb.LocalActionError, match="sealed environment already sets"):
        session.environment({"TMPDIR": str(tmp_path), "PRISMABUILD_PROFILE_NSYS": "0"})


def test_nsys_still_allows_docker_metadata_reads(tmp_path):
    session = pb._ProfileSession(
        mode="nsys", backend=pb.NsysProfileBackend(), directory=tmp_path / "profile"
    )
    result, marked, forwarded = _shim(
        tmp_path, ["image", "inspect", "gpu-image"],
        docker_env=session.environment({"TMPDIR": str(tmp_path)}),
    )
    assert result.returncode == 0, result.stderr
    assert forwarded == ["image", "inspect", "gpu-image"]
    assert not marked
