"""Optional PB pytest witness of each admitted Python, including its real bytes."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import sys
import tempfile

import pytest
from prismabuild import digest_primitives, immutable_files


@pytest.hookimpl(specname='pytest_addoption')
def pytest_c13_identity_option(parser):
    parser.addoption('--c13-runtime-evidence', required=True)


@pytest.hookimpl(specname='pytest_sessionstart')
def pytest_c13_admitted_identity(session):
    executable = Path(sys.executable).resolve()
    raw = executable.read_bytes()
    digest = digest_primitives.raw_sha256(raw)
    directory = Path(session.config.getoption('--c13-runtime-evidence'))
    directory.mkdir(parents=True, exist_ok=True)
    captured = directory / digest
    immutable_files.publish_exact(captured, raw, where='C13 admitted interpreter',
                                  conflict_error=RuntimeError)
    print('c13-admitted-runtime: ' + json.dumps(dict(
        version=sys.version, version_info=list(sys.version_info[:3]),
        executable=sys.executable, resolved=str(executable), bytes=len(raw),
        sha256=digest, captured=str(captured), host=socket.gethostname(),
        cpus=sorted(os.sched_getaffinity(0)), tmpdir=tempfile.gettempdir())), flush=True)
