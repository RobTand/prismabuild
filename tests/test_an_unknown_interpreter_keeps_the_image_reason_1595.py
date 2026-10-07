"""An unknown interpreter must not hide the reason a submission cannot be placed (#1595).

The PACT pilot failed before publication on 2026-10-07.  Both worker offers had
the GB10 tags and the image capability, and both had empty interpreter answers.
No offer reported the declared image.  ``pbrun`` said that no worker could run
the action.  It did not name the image.

The client removes an unknown interpreter from its probe, because the first
submission of any path is unknown by construction.  The refusal branches then
asked their counterfactual questions of the original intent, which still
carried the interpreter.  Every counterfactual failed on the interpreter, so
the image, the capability and the capacity were never named as the blocker.
The refusal itself was always safe.  These tests pin that the reason stays
visible.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

from test_pbrun_detach import _checkout, _queue, _run_pbrun  # noqa: E402

CAPABILITY = pb.CONTAINER_IMAGE_TAG
PYTHON = "/home/rob/venvs/pq-pb461728e4-tessera-43da1c39/bin/python"
REF_A = "ghcr.io/example/stage-a@sha256:" + "a" * 64
REF_B = "ghcr.io/example/stage-b@sha256:" + "b" * 64


def _offer(queue, host, *, capability=True, images=(), capacity=None):
    """One live offer in the PACT shape: the interpreter tag, no path answers."""

    tags = [host, "gb10", pb.INTERPRETER_TAG]
    if capability:
        tags.append(CAPABILITY)
    queue.announce(
        host=host, tags=tags, has_gpu=False,
        capacity=capacity or {"cpu": 4, "mem_gb": 16},
        observed_images=list(images), interpreters=None)


def _refusal(tmp_path, monkeypatch, *options) -> str:
    work = _checkout(tmp_path)
    with pytest.raises(SystemExit) as exc:
        _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--tag", "gb10",
                   *options, command=(PYTHON, "-c", "print(1)"))
    return str(exc.value)


def test_a_missing_image_is_named_beside_an_unknown_interpreter(
        tmp_path, monkeypatch, capsys):
    queue = _queue(tmp_path)
    _offer(queue, "sparky", images=[REF_B])
    _offer(queue, "sparklina", images=[REF_B])

    message = _refusal(tmp_path, monkeypatch, "--container-image", REF_A)

    assert "no recorded worker can run this action" in message
    assert f"container images: {REF_A}" in message
    assert "no recorded eligible worker reports it" in message
    assert "Load or pull the image" in message
    # The interpreter is unknown, not absent: the client says so and does not
    # call it the blocker.
    assert "every recorded eligible worker names it absent" not in message
    assert "no worker has answered for the interpreter" in capsys.readouterr().err
    assert queue.ready_items() == []


def test_a_missing_capability_is_named_beside_an_unknown_interpreter(
        tmp_path, monkeypatch):
    queue = _queue(tmp_path)
    _offer(queue, "sparky", capability=False)
    _offer(queue, "sparklina", capability=False)

    message = _refusal(tmp_path, monkeypatch, "--container-image", REF_A)

    assert f"container images: {REF_A}" in message
    assert f"no eligible worker offers {CAPABILITY}" in message
    assert "every recorded eligible worker names it absent" not in message
    assert queue.ready_items() == []


def test_a_capacity_shortfall_is_named_beside_an_unknown_interpreter(
        tmp_path, monkeypatch):
    queue = _queue(tmp_path)
    _offer(queue, "sparky", images=[REF_A], capacity={"cpu": 4, "mem_gb": 16,
                                                      "spool_gb": 241})
    _offer(queue, "sparklina", images=[REF_A], capacity={"cpu": 4, "mem_gb": 16,
                                                         "spool_gb": 241})

    message = _refusal(
        tmp_path, monkeypatch, "--container-image", REF_A, "--env",
        f"PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES={250 * 2**30}")

    assert "spool_gb 250 > sparky 241" in message
    assert "no recorded eligible worker reports" not in message
    assert queue.ready_items() == []


def test_a_placeable_image_with_an_unknown_interpreter_still_publishes(
        tmp_path, monkeypatch, capsys):
    queue = _queue(tmp_path)
    _offer(queue, "sparky", images=[REF_A])
    _offer(queue, "sparklina", images=[REF_A])
    work = _checkout(tmp_path)

    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--tag", "gb10",
                      "--container-image", REF_A,
                      command=(PYTHON, "-c", "print(1)")) == 0

    assert "no worker has answered for the interpreter" in capsys.readouterr().err
    assert len(queue.ready_items()) == 1


def test_a_unanimous_absent_interpreter_is_still_named(tmp_path, monkeypatch):
    """The separate interpreter verdict stays: absent is final and is named."""

    queue = _queue(tmp_path)
    for host in ("sparky", "sparklina"):
        _offer(queue, host, images=[REF_A])
        record_path = queue.root / "workers" / f"{host}.json"
        import json
        record = json.loads(record_path.read_text())
        record["interpreters_absent"] = [PYTHON]
        record_path.write_text(json.dumps(record))

    message = _refusal(tmp_path, monkeypatch, "--container-image", REF_A)

    assert "every recorded eligible worker names it absent" in message
    assert PYTHON in message
