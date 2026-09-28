"""A GPU box's worker interpreter is PrismaBuild's own, not a client's (#1269).

``worker_loop.py --python`` names the interpreter that launches the pool
worker for every action a box claims (``supervise.py`` passes the box's
declared args through; ``PoolQueue.serve_once(python=...)`` and
``execute`` build the worker's ``argv`` from it).  gx10-6b77 declared the
PrismaQuant CUDA venv there -- the client environment PB's GPU worker
started from -- while sparky's ``--gpu`` box declared no ``--python`` at
all and inherited the flag's system default, so one role had three
conventions across the fleet.

These tests pin the configuration contract the issue's acceptance states:
every ``--gpu`` box names its worker interpreter, and that interpreter is
never one of the configured ``gpu_interpreters`` -- the list of
interpreters *client GPU work* runs, which ``require_pool.py`` refuses a
command outside PrismaBuild for.  The two lists must stay disjoint: a
loop's ``--python`` that appears in ``gpu_interpreters`` is a client
environment inherited by the worker, whatever the path is called.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import require_pool as hook  # noqa: E402


def _boxes() -> dict[str, dict]:
    document = json.loads((ROOT / "tools" / "fleet" / "fleet_boxes.json").read_text())
    boxes = document["boxes"]
    assert isinstance(boxes, dict)
    return boxes


def _gpu_boxes(boxes: dict[str, dict]) -> dict[str, list[str]]:
    """Every box that offers the pool a GPU, with its declared args."""

    out: dict[str, list[str]] = {}
    for name, shape in boxes.items():
        if not isinstance(shape, dict):
            continue
        args = shape.get("args") or []
        if isinstance(args, list) and "--gpu" in args:
            out[name] = [str(arg) for arg in args]
    return out


def _declared_python(args: list[str]) -> str | None:
    """The ``--python`` a box's args name, or ``None`` when they name none."""

    for index, arg in enumerate(args):
        if arg == "--python" and index + 1 < len(args):
            return args[index + 1]
        if arg.startswith("--python="):
            return arg.split("=", 1)[1]
    return None


def test_every_gpu_box_names_its_worker_interpreter() -> None:
    gpu_boxes = _gpu_boxes(_boxes())

    assert gpu_boxes, "no --gpu box is declared; the contract cannot be vacuous"
    missing = sorted(name for name, args in gpu_boxes.items()
                     if _declared_python(args) is None)
    assert not missing, (
        f"--gpu boxes {missing} declare no --python; the worker interpreter "
        f"would be the flag's system default rather than a stated, "
        f"PrismaBuild-owned choice (#1269)")


def test_a_worker_interpreter_is_never_a_client_gpu_interpreter() -> None:
    document = json.loads((ROOT / "tools" / "fleet" / "fleet_boxes.json").read_text())
    clients = {hook.interpreter_signature(str(path))
               for path in document[hook.INTERPRETERS_FIELD]}
    assert clients, "gpu_interpreters is empty; the disjointness cannot be pinned"

    inherited = {}
    for name, args in _gpu_boxes(document["boxes"]).items():
        python = _declared_python(args)
        if python is not None and hook.interpreter_signature(python) in clients:
            inherited[name] = python
    assert not inherited, (
        f"--gpu boxes {inherited} launch the pool worker from a configured "
        f"client GPU interpreter; the worker's interpreter is PrismaBuild's "
        f"own and the client list is for client work (#1269)")
