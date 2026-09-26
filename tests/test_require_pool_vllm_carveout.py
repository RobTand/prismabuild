"""The GPU-container hook exempts vLLM and sees inside script files (#588).

The standing ruling exempts everything vLLM from admission -- serves,
censuses, routing runs, benchmarks against a live endpoint, its GPU
containers -- but ``require_pool.py`` refused any ``docker --gpus``
invocation, so exempt work routed around the hook through a script file the
lexical scan cannot see inside.  A hook that teaches the one way past it is
advisory for exactly the work it most wants to see.

It is pure CPU -- no GPU, no containers, only command text and small local
scripts -- so it runs as an ordinary CPU test shard and needs no GPU demand.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from test_require_pool import _armed, _verdict  # noqa: E402

CUDA_LINE = "/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python train.py"


def _sightings(tmp_path: Path) -> list[dict]:
    path = tmp_path / "require_pool_sightings.log"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()
            if line.strip()]


# -- the vLLM carve-out -------------------------------------------------------


@pytest.mark.parametrize("command", [
    "docker run --gpus all -p 8000:8000 vllm/vllm-openai:latest --model foo",
    "docker run --gpus all --ipc=host vllm/vllm-openai:v0.9 vllm serve foo",
    "podman run --gpus all img vllm serve --model foo --port 8000",
])
def test_a_vllm_container_passes(tmp_path, command) -> None:
    assert _verdict(_armed(tmp_path, None), command) == 0


@pytest.mark.parametrize("command", [
    "docker run --gpus all image train.py",
    "docker run --gpus all vllm/vllm-openai /opt/nccl-tests/build/all_reduce_perf",
    "docker run --gpus all vllm-image nccl-tests/all_reduce_perf -b 1G -e 8G",
    "docker run --gpus all vllm/vllm-openai pytest /tests",
])
def test_outside_the_carve_out_is_still_refused(tmp_path, command) -> None:
    """No vLLM mention, a bare collective borrowing its image, or a test
    runner inside the container: none of these is a serve, and the NCCL edge
    is decided explicitly rather than by whoever holds the keyboard."""

    assert _verdict(_armed(tmp_path, None), command) == 2


def test_the_carve_out_is_container_only(tmp_path) -> None:
    """The tessera#550 pytest refusal was correct and stays refused."""

    assert _verdict(_armed(tmp_path, None), "python3 -m pytest tests/") == 2


# -- #1183: the image reference is not the program ----------------------------


#: The GLM-5.3 serving target's repository name records the NCCL 2.30.7 swap;
#: the name is not a collective, and searching the whole segment for the
#: collective pattern refused the serve (issue #1183).
NCCL_IMAGE = ("localhost/prismaquant/spark-vllm-nccl230"
              "@sha256:a5424378a7bd6e2a6c1a4e37a2b7b0f1"
              "3b1c0f0a4d5e6f708192a3b4c5d6e7f8")


@pytest.mark.parametrize("command", [
    f"docker run --gpus all {NCCL_IMAGE} vllm serve --model GLM-5.3",
    f"docker run --gpus all {NCCL_IMAGE}",
    f"docker run --gpus all {NCCL_IMAGE} --model GLM-5.3",
    "docker run --gpus all registry/prismaquant-spark-nccl230 vllm serve foo",
])
def test_an_image_name_that_says_nccl_is_not_a_collective(
    tmp_path, command,
) -> None:
    """The name of the image is not the program it runs."""

    assert _verdict(_armed(tmp_path, None), command) == 0


@pytest.mark.parametrize("command", [
    # Option arguments before the image are the container's settings, not
    # the work, even when their names mention NCCL.
    "docker run --gpus all --ipc=host -p 8000:8000 --name glm-nccl "
    "--env NCCL_DEBUG=INFO --shm-size 16g vllm/vllm-openai:v0.9 "
    "vllm serve --model foo",
    # The explicit vLLM entry command under an image named for its NCCL.
    "docker run --gpus all registry/prismaquant-spark-nccl230 vllm serve foo",
    # Both entrypoint spellings, whose value is the program.
    "docker run --gpus all --entrypoint vllm vllm/vllm-openai serve foo",
    "docker run --gpus all --entrypoint=/usr/bin/vllm vllm/vllm-openai "
    "serve --model foo",
])
def test_option_and_entrypoint_shapes_keep_the_exemption(
    tmp_path, command,
) -> None:
    assert _verdict(_armed(tmp_path, None), command) == 0


@pytest.mark.parametrize("command", [
    # The carve-out's negative half: the collective is in the program, so
    # the image's own name changes nothing.
    f"docker run --gpus all {NCCL_IMAGE} "
    "/opt/nccl-tests/build/all_reduce_perf",
    "docker run --gpus all vllm/vllm-openai bandwidthTest --device=0",
    "docker run --gpus all vllm/vllm-openai p2pBandwidthLatencyTest",
    # The entrypoint's value is a program, and a collective there is bare.
    "docker run --gpus all --entrypoint "
    "/opt/nccl-tests/build/all_reduce_perf vllm/vllm-openai",
    # A shell nested in the container still runs the collective.
    "docker run --gpus all vllm/vllm-openai bash -c "
    "'/opt/nccl-tests/build/all_reduce_perf -b 1G'",
])
def test_a_collective_in_a_vllm_image_is_still_refused(
    tmp_path, command,
) -> None:
    assert _verdict(_armed(tmp_path, None), command) == 2


def test_quoting_does_not_widen_the_carve_out(tmp_path) -> None:
    """A quoted collective is still the work; only prose ABOUT the rule is
    exempt, and a container that names no vLLM is still refused."""

    module = _armed(tmp_path, None)
    assert _verdict(
        module, "docker run --gpus all image bash -c 'echo nccl'") == 2
    assert _verdict(module, "docker run --gpus all image train.py") == 2


# -- the script-file blind spot -----------------------------------------------


def test_gpu_work_inside_a_script_file_is_refused(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ab.sh").write_text(
        "#!/bin/bash\n" + CUDA_LINE + "\n", encoding="utf-8")

    module = _armed(tmp_path, None)
    assert _verdict(module, "bash ab.sh") == 2


def test_a_gpu_container_inside_a_script_file_is_refused(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ab.sh").write_text(
        "#!/bin/bash\ndocker run --gpus all image train.py\n",
        encoding="utf-8")

    module = _armed(tmp_path, None)
    assert _verdict(module, "bash ab.sh") == 2


def test_a_clean_script_passes_and_is_recorded(
    tmp_path, monkeypatch,
) -> None:
    """Allowed, but no longer silent: the sighting names the script."""

    monkeypatch.chdir(tmp_path)
    (tmp_path / "ok.sh").write_text("#!/bin/bash\necho hi\n", encoding="utf-8")

    assert _verdict(_armed(tmp_path, None), "bash ok.sh") == 0
    sightings = _sightings(tmp_path)
    assert len(sightings) == 1
    assert sightings[0]["script"].endswith("ok.sh")


def test_an_unreadable_script_passes_and_is_recorded(
    tmp_path, monkeypatch,
) -> None:
    """What the hook cannot read it cannot judge; the route-around is then
    visible rather than silent."""

    monkeypatch.chdir(tmp_path)
    assert _verdict(_armed(tmp_path, None), "bash nosuch.sh") == 0
    sightings = _sightings(tmp_path)
    assert len(sightings) == 1
    assert sightings[0]["script"].endswith("nosuch.sh")
    assert sightings[0]["reason"] == "unreadable"


def test_only_one_level_is_read(tmp_path, monkeypatch) -> None:
    """A script that only calls another script is allowed and logged; the
    inner file's text is not the outer command's.  The log is what makes
    deeper nesting visible instead of silent."""

    monkeypatch.chdir(tmp_path)
    (tmp_path / "inner.sh").write_text(
        "#!/bin/bash\n" + CUDA_LINE + "\n", encoding="utf-8")
    (tmp_path / "outer.sh").write_text(
        "#!/bin/bash\nbash inner.sh\n", encoding="utf-8")

    assert _verdict(_armed(tmp_path, None), "bash outer.sh") == 0
    sightings = _sightings(tmp_path)
    assert len(sightings) == 1
    assert sightings[0]["script"].endswith("outer.sh")
