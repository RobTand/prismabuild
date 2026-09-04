"""An NFS "would block" is not a contract violation.

``O_NONBLOCK`` is on every regular-file open in ``core`` so that a FIFO or a
device cannot block the process before ``fstat`` gets to refuse it.  POSIX
gives the flag no meaning for regular files, but NFSv4 does: the client can
answer ``EAGAIN`` while a delegation is recalled.  The whole fleet's checkouts
sit on one NFS export and every worker opens the same closure stamp, so the
open fails for a reason that has nothing to do with the file -- five actions in
the live queue died that way, each burning an attempt.

Two properties, and the second is the one that keeps the flag meaningful: a
transient "would block" is retried, and a path that will *always* block is
still refused rather than opened blocking.
"""
from __future__ import annotations

import errno
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core


def test_a_transient_would_block_is_retried(tmp_path, monkeypatch):
    target = tmp_path / "closure.json"
    target.write_text('{"ok": true}')
    real, calls = os.open, []

    def flaky(path, flags, *args, **kwargs):
        calls.append(path)
        if len(calls) <= 2:
            raise OSError(errno.EAGAIN, "Resource temporarily unavailable",
                          str(path))
        return real(path, flags, *args, **kwargs)

    monkeypatch.setattr(core.os, "open", flaky)
    monkeypatch.setattr(core.time, "sleep", lambda _s: None)
    assert core._read_regular_file(target, where="closure") == b'{"ok": true}'
    assert len(calls) == 3


def test_a_persistent_would_block_is_still_refused(tmp_path, monkeypatch):
    """The flag is protection against a blocking open, and stays protection."""
    target = tmp_path / "fifo-shaped"
    target.write_text("x")

    def always(path, flags, *args, **kwargs):
        raise OSError(errno.EAGAIN, "Resource temporarily unavailable",
                      str(path))

    monkeypatch.setattr(core.os, "open", always)
    monkeypatch.setattr(core.time, "sleep", lambda _s: None)
    with pytest.raises(core.ActionContractError) as caught:
        core._read_regular_file(target, where="closure")
    assert "closure" in str(caught.value)


def test_every_other_errno_is_unchanged(tmp_path, monkeypatch):
    """Only EAGAIN is transient; a missing file must not be retried into one."""
    target = tmp_path / "gone.json"
    calls = []

    def missing(path, flags, *args, **kwargs):
        calls.append(path)
        raise FileNotFoundError(errno.ENOENT, "No such file", str(path))

    monkeypatch.setattr(core.os, "open", missing)
    monkeypatch.setattr(core.time, "sleep", lambda _s: None)
    with pytest.raises(core.ActionContractError):
        core._read_regular_file(target, where="closure")
    assert len(calls) == 1


def test_a_real_fifo_is_refused_rather_than_blocking(tmp_path):
    """End to end, with no monkeypatching: the flag's actual job.

    A FIFO with no writer would block an open that lacked ``O_NONBLOCK``, and
    the retry must not have turned that refusal into a hang.
    """
    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)
    with pytest.raises(core.ActionContractError) as caught:
        core._read_regular_file(fifo, where="closure")
    assert "closure" in str(caught.value)
