"""The real container-stop qualifier refuses to claim a proof it did not run (#1599)."""
import json
import os
from pathlib import Path
import subprocess
import sys

QUALIFIER = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "qualify_docker_stop.py"


def _run(env_update):
    env = {k: v for k, v in os.environ.items()
           if k not in ("PRISMABUILD_CONTAINER_OWNER", "PRISMABUILD_CONTAINER_MARKER")}
    env.update(env_update)
    return subprocess.run([sys.executable, str(QUALIFIER)], env=env,
                          capture_output=True, text=True, timeout=60)


def test_outside_an_admitted_action_it_did_not_test():
    done = _run({})
    assert done.returncode == 2
    report = json.loads(done.stdout)
    assert report["verdict"] == "did_not_test"
    assert "admitted" in report["reason"]


def test_a_missing_image_is_did_not_test_and_never_proved(tmp_path):
    fake = tmp_path / "bin"
    fake.mkdir()
    done = _run({"PRISMABUILD_CONTAINER_OWNER": "1" * 64,
                 "PRISMABUILD_CONTAINER_MARKER": str(tmp_path / "owner.used"),
                 "PBDOCKER_STOP_IMAGE": "pbdocker-stop-no-such-image:never"})
    assert done.returncode == 2, done.stdout
    assert json.loads(done.stdout)["verdict"] == "did_not_test"
