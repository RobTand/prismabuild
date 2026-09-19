"""Leg 2 -- GPU container canary action (RobTand/prismabuild#688).

Runs INSIDE a production-qualified image pinned by digest (the campaign
image), executes a trivial CUDA kernel (device tensor op plus
``torch.cuda.device_count() >= 1``), and stamps the artifact with the
torch/CUDA identity. Exercises container launch, image identity, and CUDA
visibility. Maps to the stage-A missing links #796 (bare payload with no
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

import hashlib
import re
import shlex

from . import LegBuildRefused

NAME = "leg-2"

#: Environment variable carrying the digest-pinned campaign image ref.
IMAGE_ENV = "PBCANARY_GPU_IMAGE"

#: ``name@sha256:<64 hex>`` -- a tag without a digest never verifies.
IMAGE_PATTERN = re.compile(r"^.+@sha256:[0-9a-f]{64}$")

PAYLOAD = b"pbcanary-leg2:v1"

PAYLOAD_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()

#: sum(i*i for i in range(16)) == 1240; the inner kernel asserts real
#: device execution, not just a visible driver.
KERNEL_VALUE = sum(i * i for i in range(16))

assert KERNEL_VALUE == 1240

DEMAND = {"cpu": 1, "gpu": 1, "mem_gb": 8}

WAIT_S = 600

_TORCH_LINE = re.compile(r"^TORCH \S+ CUDA \S+ DEVICES (\d+)$", re.MULTILINE)


def _inner_script() -> str:
    """Python run inside the container. Double quotes only: the driver side
    single-quotes this whole program for ``python3 -c``."""
    return (
        "import hashlib, sys\n"
        "try:\n"
        "    import torch\n"
        "except ImportError:\n"
        '    print("TORCH_IMPORT_FAILED")\n'
        "    sys.exit(10)\n"
        "n = torch.cuda.device_count()\n"
        f'print("PAYLOAD {PAYLOAD_SHA256}")\n'
        'print("TORCH " + torch.__version__ + " CUDA " + str(torch.version.cuda) + " DEVICES " + str(n))\n'
        "if n < 1:\n"
        '    print("NO_CUDA_DEVICE")\n'
        "    sys.exit(11)\n"
        'x = torch.arange(16, device="cuda")\n'
        "y = int((x * x).sum())\n"
        'print("KERNEL " + str(y))\n'
    )


def image_ref(*, env: dict | None = None) -> str:
    """Return the pinned image ref, or raise :class:`LegBuildRefused`.

    ``env`` overrides ``os.environ`` for tests; ``None`` reads the process
    environment.
    """
    import os

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
    inner = _inner_script()
    script = (
        f"echo {shlex.quote('IMAGE ' + ref)}; "
        f"docker run --rm --gpus all --entrypoint \"\" {ref} "
        f"python3 -c {shlex.quote(inner)}"
    )
    return {
        "name": NAME,
        "argv": ["bash", "-c", script],
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
    if result.get("sha256") != hashlib.sha256(artifact.encode()).hexdigest():
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
