"""Queue clients have a stable default without disabling pressure diagnostics."""
import os
import subprocess
import sys

import pytest

from prismabuild import filesystem_capacity as capacity
from queue_space_support import QUEUE_SPACE_BOOTSTRAP


@pytest.mark.parametrize("state", ["warning", "critical", "inodes"])
def test_queue_diagnostics_ignore_the_hosts_capacity(tmp_path, state):
    program = (
        f"from queue_space_host import install_host_space; install_host_space({state!r})\n"
        + QUEUE_SPACE_BOOTSTRAP
        + "from prismabuild import filesystem_capacity as c\n"
        + f"result = c.queue_space_diagnostic({str(tmp_path)!r})\n"
        + "assert result['state'] == 'ok', result\n"
        + "assert result['available_bytes'] == 1099511627776, result\n"
        + "assert result['inode_pressure'] is False, result\n"
    )
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    done = subprocess.run([sys.executable, "-c", program], env=environment,
                          capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, done.stderr


def test_an_explicit_os_sample_still_controls_the_queue(tmp_path, monkeypatch):
    sample = os.statvfs_result((4096, 4096, (100 << 30) // 4096,
                               (100 << 30) // 4096, (3 << 30) // 4096,
                               1000, 900, 900, 0, 255))
    monkeypatch.setattr(os, "statvfs", lambda path: sample)
    result = capacity.queue_space_diagnostic(tmp_path)
    assert result["state"] == "critical"
    assert result["available_bytes"] == 3 << 30
