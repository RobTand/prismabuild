"""CPU descriptor-lifetime witnesses for the experimental checkout copy.

Injected ioctl outcomes exercise descriptor ownership, not filesystem reflink
support, a production reader lease, a real Git pack, or performance.
"""
from __future__ import annotations

import errno
import importlib.util
import os
from pathlib import Path

import pytest


@pytest.fixture
def harness():
    source = (Path(__file__).resolve().parents[1] / "tools" / "maintenance"
              / "diag_811_e1_checkout_cache.py")
    spec = importlib.util.spec_from_file_location("diag811_copy_test", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_closed(descriptors):
    for descriptor in descriptors:
        with pytest.raises(OSError) as failure:
            os.fstat(descriptor)
        assert failure.value.errno == errno.EBADF


@pytest.mark.parametrize("mutation", ["replace", "unlink"])
def test_copy_fallback_retains_the_opened_object(harness, tmp_path, monkeypatch,
                                                mutation):
    source, destination = tmp_path / "object.pack", tmp_path / "private.pack"
    expected = b"old-pack"
    source.write_bytes(expected)
    descriptors = []

    def unsupported_reflink(destination_fd, operation, source_fd):
        assert operation == harness.FICLONE
        assert os.pread(source_fd, len(expected), 0) == expected
        descriptors.extend((destination_fd, source_fd))
        if mutation == "replace":
            source.replace(tmp_path / "retired.pack")
            source.write_bytes(b"new-pack")  # Same length, different object.
        else:
            source.unlink()
        raise OSError(errno.EOPNOTSUPP, "injected unsupported reflink")

    monkeypatch.setattr(harness.fcntl, "ioctl", unsupported_reflink)
    assert harness.copy_or_reflink(source, destination) == "copy"
    assert destination.read_bytes() == expected, "fallback reopened a different object"
    assert_closed(descriptors)


@pytest.mark.parametrize("method", ["copy", "reflink"])
def test_copy_method_controls_close_their_descriptors(harness, tmp_path,
                                                     monkeypatch, method):
    source, destination = tmp_path / "object.idx", tmp_path / "private.idx"
    expected = b"index-bytes"
    source.write_bytes(expected)
    descriptors = []

    def selected_reflink(destination_fd, operation, source_fd):
        assert operation == harness.FICLONE
        descriptors.extend((destination_fd, source_fd))
        if method == "copy":
            raise OSError(errno.EOPNOTSUPP, "injected unsupported reflink")
        assert os.write(destination_fd, os.pread(source_fd, len(expected), 0)) == len(expected)
        return 0

    monkeypatch.setattr(harness.fcntl, "ioctl", selected_reflink)
    assert harness.copy_or_reflink(source, destination) == method
    assert destination.read_bytes() == expected
    assert_closed(descriptors)
