"""Leg 2 -- GPU container canary action (RobTand/prismabuild#688).

Runs the source-owned payload inside a digest-pinned production image.
GPU mode executes a trivial CUDA kernel (device tensor op plus
``torch.cuda.device_count() >= 1``), and stamps the artifact with the
torch/CUDA identity. Exercises container launch, image identity, and CUDA
visibility. CPU preflight mode runs the same payload on a CPU tensor,
requires zero visible CUDA devices and writes completed checks as JSON.
Maps to the stage-A missing links #796 (bare payload with no
container identity) and #798 (no GPU demand):

* #796: the image ref is digest-pinned (``name@sha256:<hex>``) and echoed
  on the artifact's first line; ``docker run`` through PB's shim fails a
  leg whose image is missing instead of running bare. The run clears the
  entrypoint (``--entrypoint ""``) exactly as the production container
  wrapper does (``tools/tessera_campaign_container.py``): the campaign
  image family's own entrypoint performs vLLM platform inference and
  refuses to pass a command through when it stands.
* #798: the action declares ``gpu: 1`` demand AND the payload asserts
  ``device_count >= 1`` AND runs a real device-tensor op inside the
  container, so a box with no visible CUDA fails the leg loudly.

The image is operator configuration, not a default: ``build()`` reads
``PBCANARY_GPU_IMAGE`` (or the driver's ``--gpu-image``, which sets it for
the build) and raises :class:`LegBuildRefused` when it is absent or not
digest-pinned. A floating tag can never become a canary expectation.

The image must provide ``python3`` with a CUDA-enabled ``torch``. The
artifact's deterministic prefix (image line + payload digest) is fixed at
build time; the torch/CUDA stamp lines are verified by shape
(``TORCH <ver> CUDA <ver> DEVICES <n>=1``, ``KERNEL 1240``) rather than by
exact bytes, so the stamp stays evidence without becoming brittle.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

if __package__:
    from . import LegBuildRefused
    from prismabuild import digest_primitives as digests
else:
    import runpy
    from types import SimpleNamespace
    source_path = Path(__file__).resolve()
    if source_path.parent == Path("/pb-canary"):
        digest_path = source_path.with_name("digest_primitives.py")
    else:
        source_root = source_path.parents[3] if source_path.parents[1].name == "fleet" else source_path.parents[2]
        digest_path = source_root / "src/prismabuild/digest_primitives.py"
    digests = SimpleNamespace(**runpy.run_path(str(digest_path)))

    class LegBuildRefused(ValueError):
        """A standalone canary invocation cannot be built."""

NAME = "leg-2"

#: Environment variable carrying the digest-pinned campaign image ref.
IMAGE_ENV = "PBCANARY_GPU_IMAGE"

#: ``name@sha256:<64 hex>`` -- a tag without a digest never verifies.
IMAGE_PATTERN = re.compile(r"^.+@sha256:[0-9a-f]{64}$")

PAYLOAD = b"pbcanary-leg2:v1"

PAYLOAD_SHA256 = digests.raw_sha256(PAYLOAD)

#: sum(i*i for i in range(16)) == 1240; the inner kernel asserts real
#: device execution, not just a visible driver.
KERNEL_VALUE = sum(i * i for i in range(16))

assert KERNEL_VALUE == 1240

DEMAND = {"cpu": 1, "gpu": 1, "mem_gb": 8}

WAIT_S = 600

_TORCH_LINE = re.compile(r"^TORCH \S+ CUDA \S+ DEVICES (\d+)$", re.MULTILINE)


CHECKS_SCHEMA = "fleet.d38.leg2_checks.v1"
CHECK_NAMES = (
    "imports", "arguments", "payload_digest", "image_digest", "shape",
    "kernel_value", "input_reads", "device_visibility",
)
ENTRY_SCRIPT = "tools/fleet/pbcanary_legs/leg2.py"
CHECKS_PATH = ".d38/leg2-checks.json"
MAX_INPUT_BYTES = 64 * 1024
_INPUT_ID = re.compile(r"[A-Za-z0-9_.-]+")
_SHA256 = re.compile(r"[0-9a-f]{64}")


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the source-owned leg-2 payload")
    parser.add_argument("--image", required=True)
    parser.add_argument("--device", choices=("cuda", "cpu"), required=True)
    parser.add_argument("--checks-path", required=True)
    parser.add_argument("--input", action="append", default=[])
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--payload-sha256", help=argparse.SUPPRESS)
    parser.add_argument("--source-sha256", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    image_ref(env={IMAGE_ENV: args.image})
    inputs = []
    identities = set()
    for item in args.input:
        try:
            identity, rest = item.split("=", 1)
            path, digest = rest.rsplit("=", 1)
        except ValueError:
            raise ValueError("--input must be ID=PATH=SHA256") from None
        if (not _INPUT_ID.fullmatch(identity) or identity in identities
                or not path or "," in path or not _SHA256.fullmatch(digest)):
            raise ValueError("--input has an invalid or duplicate identity, path or digest")
        identities.add(identity)
        inputs.append((identity, path, digest))
    args.inputs = inputs
    if not args.inside:
        checks = Path(args.checks_path)
        if checks.is_absolute() or ".." in checks.parts or not checks.name:
            raise ValueError("--checks-path must be a relative output file below the checkout")
        root = Path.cwd().resolve()
        if not checks.resolve().is_relative_to(root) or "," in str(checks.resolve().parent):
            raise ValueError("--checks-path leaves the checkout or cannot be mounted")
        if args.payload_sha256 is not None or args.source_sha256 is not None:
            raise ValueError("payload and source digests are internal container arguments")
    return args


def _read_inputs(inputs: list[tuple[str, str, str]]) -> list[str]:
    read = []
    for identity, filename, digest in inputs:
        with Path(filename).open("rb") as handle:
            raw = handle.read(MAX_INPUT_BYTES + 1)
        if len(raw) > MAX_INPUT_BYTES:
            raise ValueError(f"input {identity} exceeds the bounded canary read")
        if digests.raw_sha256(raw) != digest:
            raise ValueError(f"input {identity} digest mismatch")
        read.append(identity)
    return read


def _run_inside(args: argparse.Namespace) -> int:
    import torch

    source_digest = digests.raw_sha256(Path(__file__).read_bytes())
    if source_digest != args.source_sha256:
        raise ValueError("payload source digest mismatch")
    payload_digest = digests.raw_sha256(PAYLOAD)
    if payload_digest != args.payload_sha256:
        raise ValueError("payload digest mismatch")
    if os.environ.get("PBCANARY_VERIFIED_IMAGE") != args.image:
        raise ValueError("image digest was not verified by the container launcher")
    reads = _read_inputs(args.inputs)
    devices = torch.cuda.device_count()
    if args.device == "cpu" and devices != 0:
        raise ValueError("CPU preflight sees a CUDA device")
    if args.device == "cuda" and devices < 1:
        raise ValueError("NO_CUDA_DEVICE")
    x = torch.arange(16, device=args.device)
    if tuple(x.shape) != (16,):
        raise ValueError("kernel input shape mismatch")
    squared = x * x
    if tuple(squared.shape) != (16,):
        raise ValueError("kernel output shape mismatch")
    scalar = squared.sum()
    if tuple(getattr(scalar, "shape", ())) != ():
        raise ValueError("kernel reduction shape mismatch")
    value = int(scalar)
    if value != KERNEL_VALUE:
        raise ValueError("kernel value mismatch")
    result = {
        "schema": CHECKS_SCHEMA,
        "checks": dict.fromkeys(CHECK_NAMES, "passed"),
        "inputs": reads,
        "image": args.image,
        "payload_sha256": payload_digest,
        "source_sha256": source_digest,
        "device": args.device,
        "shape": [16],
        "result_shape": [],
        "kernel_value": value,
    }
    output = Path(args.checks_path)
    temporary = output.with_name(output.name + ".tmp")
    temporary.write_text(digests.sorted_json(result) + "\n")
    temporary.replace(output)
    print(f"IMAGE {args.image}")
    print(f"PAYLOAD {payload_digest}")
    print(f"TORCH {torch.__version__} CUDA {torch.version.cuda} DEVICES {devices}")
    print(f"KERNEL {value}")
    return 0


def container_argv(args: argparse.Namespace, source_digest: str) -> list[str]:
    source = Path(__file__).resolve()
    output = Path(args.checks_path).resolve()
    command = [*_docker_command(), "run", "--rm", "--workdir", "/pb-canary"]
    if args.device == "cuda":
        command.extend(["--gpus", "all"])
    else:
        command.extend(["--env", "CUDA_VISIBLE_DEVICES=", "--env", "NVIDIA_VISIBLE_DEVICES=none"])
    command.extend(["--entrypoint", "", "--env", f"PBCANARY_VERIFIED_IMAGE={args.image}"])
    for variable in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        command.extend(["--env", variable + "=1"])
    command.extend(["--mount", f"type=bind,src={source},dst=/pb-canary/leg2.py,readonly",
                    "--mount", f"type=bind,src={output.parent},dst=/pb-checks",
                    "--mount", f"type=bind,src={Path(digests.__file__).resolve()},dst=/pb-canary/digest_primitives.py,readonly"])
    inside_inputs = []
    for index, (identity, filename, digest) in enumerate(args.inputs):
        path = Path(filename).resolve(strict=True)
        if not path.is_file() or "," in str(path):
            raise ValueError(f"input {identity} is not a mountable regular file")
        destination = f"/pb-inputs/{index}"
        command.extend(["--mount", f"type=bind,src={path},dst={destination},readonly"])
        inside_inputs.extend(["--input", f"{identity}={destination}={digest}"])
    command.extend([args.image, "python3", "-I", "/pb-canary/leg2.py", "--inside",
                    "--image", args.image, "--device", args.device,
                    "--checks-path", f"/pb-checks/{output.name}",
                    "--payload-sha256", PAYLOAD_SHA256,
                    "--source-sha256", source_digest, *inside_inputs])
    return command


def _docker_command() -> list[str]:
    executable = shutil.which("docker")
    if executable is None:
        raise ValueError("the PrismaBuild Docker shim is unavailable")
    return ["/usr/bin/python3", "-I", executable]


def _run_container(args: argparse.Namespace) -> int:
    inspected = subprocess.run(
        [*_docker_command(), "image", "inspect", "--format", "{{json .RepoDigests}}", args.image],
        capture_output=True, text=True, check=True, timeout=30)
    images = json.loads(inspected.stdout)
    if not isinstance(images, list) or args.image not in images:
        raise ValueError("Docker image inspection did not verify the pinned image digest")
    source_digest = digests.raw_sha256(Path(__file__).read_bytes())
    output = Path(args.checks_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    command = container_argv(args, source_digest)
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        output.unlink(missing_ok=True)
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    args = None
    try:
        args = parse_arguments(argv)
        Path(args.checks_path).unlink(missing_ok=True)
        return _run_inside(args) if args.inside else _run_container(args)
    except (ValueError, OSError, ImportError, RuntimeError, subprocess.SubprocessError) as exc:
        if args is not None:
            Path(args.checks_path).unlink(missing_ok=True)
        print(f"leg-2 checks failed: {exc}", file=sys.stderr)
        return 1


def image_ref(*, env: dict | None = None) -> str:
    """Return the pinned image ref, or raise :class:`LegBuildRefused`.

    ``env`` overrides ``os.environ`` for tests; ``None`` reads the process
    environment.
    """
    source = os.environ if env is None else env
    ref = (source.get(IMAGE_ENV) or "").strip()
    if not ref:
        raise LegBuildRefused(
            f"{NAME} build refused: {IMAGE_ENV} is unset: did not test"
        )
    if not IMAGE_PATTERN.match(ref):
        raise LegBuildRefused(
            f"{NAME} build refused: {IMAGE_ENV}={ref!r} is not "
            "digest-pinned (want name@sha256:<64 hex>): did not test"
        )
    return ref


def build() -> dict:
    """Seal the leg-2 action spec for the ``pbrun`` submission path."""
    ref = image_ref()
    return {
        "name": NAME,
        "argv": ["/usr/bin/python3", "-I", ENTRY_SCRIPT, "--image", ref, "--device", "cuda",
                 "--checks-path", CHECKS_PATH],
        "demand": dict(DEMAND),
        "wait_s": WAIT_S,
        "container_image": ref,
        "expected": {
            "leg": NAME,
            "image": ref,
            "artifact_prefix": f"IMAGE {ref}\nPAYLOAD {PAYLOAD_SHA256}\n",
            "kernel_line": f"KERNEL {KERNEL_VALUE}",
        },
    }


def verify(receipt: object, expected: object) -> tuple[bool, str]:
    """Check image identity, CUDA visibility, kernel output, CAS binding."""
    if not isinstance(expected, dict) or expected.get("leg") != NAME:
        return False, f"{NAME} artifact-digest failed: expected block names no leg-2 action"
    want_prefix = expected.get("artifact_prefix")
    want_kernel = expected.get("kernel_line", f"KERNEL {KERNEL_VALUE}")
    if not isinstance(want_prefix, str) or not isinstance(want_kernel, str):
        return False, f"{NAME} artifact-digest failed: expected artifact text absent"
    if not isinstance(receipt, dict):
        return False, f"{NAME} receipt-verified failed: receipt is not a mapping"
    artifact = receipt.get("artifact")
    if not isinstance(artifact, str):
        return False, f"{NAME} artifact-digest failed: artifact text absent"
    if not artifact.startswith(want_prefix):
        return False, (
            f"{NAME} container-identity failed: artifact lacks the pinned "
            f"image+payload prefix (#796)"
        )
    match = _TORCH_LINE.search(artifact)
    if match is None:
        return False, (
            f"{NAME} torch-identity failed: no TORCH <ver> CUDA <ver> "
            "DEVICES <n> stamp line"
        )
    if int(match.group(1)) < 1:
        return False, (
            f"{NAME} cuda-visibility failed: torch reports zero CUDA "
            "devices (#798)"
        )
    if want_kernel not in artifact.splitlines():
        return False, (
            f"{NAME} cuda-kernel failed: device tensor op output "
            f"{want_kernel!r} absent (#798)"
        )
    inner = receipt.get("receipt")
    result = inner.get("result") if isinstance(inner, dict) else None
    if not isinstance(result, dict):
        return False, f"{NAME} receipt-verified failed: receipt result absent"
    if result.get("sha256") != digests.raw_sha256(artifact.encode()):
        return False, (
            f"{NAME} receipt-verified failed: CAS result sha256 "
            "covers different bytes than the artifact"
        )
    if result.get("bytes") != len(artifact.encode()):
        return False, (
            f"{NAME} receipt-verified failed: CAS result byte count "
            "differs from the artifact"
        )
    return True, (
        f"{NAME} verified: image {str(expected.get('image'))[:24]}..., "
        f"devices {match.group(1)}, kernel {KERNEL_VALUE}, CAS-bound"
    )


if __name__ == "__main__":
    raise SystemExit(main())
