"""Saying "no GPU" must be possible, and must be enforced rather than believed.

The hook's proxy for GPU work is the interpreter path, and that is the
deliberate choice: asking "does this command line touch CUDA" of arbitrary
text is exactly the guess the hook refuses to make.  The cost showed up on
2026-09-03, when two agents in one day were refused for ``kl_tool.py --help``
and ``kl_tool.py compare --help`` -- argparse text that starts no work -- and
the available workaround was to run the same thing under a different
interpreter, which is the routing-around the hook exists to prevent.

So there is now a way to say it: a segment that empties
``CUDA_VISIBLE_DEVICES`` for its own child.  The saying is enforced by the
kernel, not taken on trust -- with no device visible the child cannot do GPU
work, so this cannot become an escape route; work smuggled through it would
simply fail.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import require_pool


CUDA_PYTHON = "/home/rob/dq-runs/venvs/prismaquant-cu130/" + "bin/python"


@pytest.mark.parametrize("prefix", ["CUDA_VISIBLE_DEVICES=",
                                    'CUDA_VISIBLE_DEVICES=""',
                                    "CUDA_VISIBLE_DEVICES=''"])
def test_an_empty_device_list_is_not_gpu_work(prefix):
    assert require_pool.contends(f"{prefix} {CUDA_PYTHON} kl.py --help") is False


def test_a_real_device_list_is_still_refused():
    """The escape hatch is emptiness, not the variable's presence."""
    assert require_pool.contends(
        f"CUDA_VISIBLE_DEVICES=0 {CUDA_PYTHON} kl.py dump --model x") is True


def test_the_bare_interpreter_is_still_refused():
    assert require_pool.contends(f"{CUDA_PYTHON} kl.py dump --model x") is True


def test_the_exemption_does_not_cross_a_separator():
    """A neighbour's exemption never vouches for the next segment.

    ``cmd1 && cmd2`` is two commands, and the one that empties the device list
    speaks only for itself -- the same rule the heredoc fix had to restore
    after a rejoin glued two segments into one.
    """
    assert require_pool.contends(
        f"CUDA_VISIBLE_DEVICES= {CUDA_PYTHON} kl.py --help"
        f" && {CUDA_PYTHON} kl.py dump --model x") is True
