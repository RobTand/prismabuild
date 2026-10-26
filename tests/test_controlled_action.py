import subprocess
import sys

from prismabuild import pool

from controlled_action import controlled_actions


def test_unrelated_subprocess_with_env_none_inherits_environment(monkeypatch):
    monkeypatch.setenv("PB_TEST_INHERITED_ENV", "inherited")

    with controlled_actions(monkeypatch, {}):
        with pool.subprocess.Popen(
            [sys.executable, "-c",
             "import os; print(os.environ['PB_TEST_INHERITED_ENV'])"],
            env=None, stdout=subprocess.PIPE, text=True,
        ) as process:
            stdout, _ = process.communicate(timeout=60)

    assert process.returncode == 0
    assert stdout == "inherited\n"
