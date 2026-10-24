"""Runtime client mediation must not depend on optional ownership metadata.

These are cooperative configured-client/native process tests, not raw API or
hostile-code confinement tests. Fake Docker establishes control flow only.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import core as pb
from prismabuild._container_exec import launch_argv
from test_container_absolute_client_execution_acceptance import (
    ALIASES, _action, _client, _successful_action,
)
from test_core import _isolated_synthetic_launch_context  # noqa: F401


@pytest.mark.parametrize("missing", ["both", "owner", "marker"])
@pytest.mark.parametrize("mode", ["direct", "native-child"])
@pytest.mark.parametrize("alias", ALIASES, ids=lambda alias: "-".join(alias))
def test_class_gate_precedes_optional_ownership_metadata(
        tmp_path, capfd, missing, mode, alias):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    client, calls, environment = _client(tmp_path)
    marker = Path(environment["PRISMABUILD_CONTAINER_MARKER"])
    for name, condition in (("OWNER", missing in {"both", "owner"}),
                            ("MARKER", missing in {"both", "marker"})):
        if condition:
            environment.pop("PRISMABUILD_CONTAINER_" + name)
    if missing == "both":
        inventory = _action(checkout, client, environment, mode=mode,
                            args=["image", "ls", "--quiet"])
        _successful_action(tmp_path, checkout, inventory, b"inventory")
    capfd.readouterr()
    action = _action(checkout, client, environment, mode=mode,
                     args=[*alias, "undeclared:test"])
    original = copy.deepcopy(action)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    with pytest.raises(pb.LocalActionError) as failure:
        pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout)
    assert "class_context_missing" in capfd.readouterr().err + str(failure.value)
    assert not calls.exists()
    assert not marker.exists()
    assert not (checkout / "result.bin").exists()
    assert cas.lookup(action) is None
    assert action == original


@pytest.mark.parametrize("spawn", ["fork", "exec"])
@pytest.mark.parametrize("alias", ["run", "create", "exec"])
def test_inventory_client_helpers_cannot_inherit_a_forwarding_exemption(
        tmp_path, spawn, alias):
    calls = tmp_path / "work-reached-client"
    request = tmp_path / "request.json"
    helper = tmp_path / "helper.py"
    helper.write_text(
        "import json, pathlib, subprocess, sys\n"
        "raise SystemExit(subprocess.call(json.loads(pathlib.Path(sys.argv[1]).read_text())))\n"
    )
    client = tmp_path / "docker"
    request.write_text(json.dumps([str(client), alias, "test"]))
    client.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, subprocess, sys\n"
        "args = sys.argv[1:]\n"
        "if args[:2] == ['context', 'inspect']:\n"
        "    print(json.dumps('unix:///var/run/docker.sock')); raise SystemExit(0)\n"
        "if args == ['version']:\n"
        f"    command = {[sys.executable, str(helper), str(request)]!r}\n"
        + ("    os.execv(command[0], command)\n" if spawn == "exec" else
           "    raise SystemExit(subprocess.call(command))\n")
        + f"pathlib.Path({str(calls)!r}).write_text('work started')\n"
    )
    client.chmod(0o755)
    environment = dict(os.environ, PRISMABUILD_DOCKER_REAL=str(client),
                       PRISMABUILD_DOCKER_TESTING="1")
    for name in ("PRISMABUILD_CONTAINER_OWNER", "PRISMABUILD_CONTAINER_MARKER",
                 pb._CONTAINER_CLASS_LAUNCH_ENV):
        environment.pop(name, None)
    result = subprocess.run(launch_argv([str(client), "version"], environment),
                            env=environment, capture_output=True, text=True,
                            timeout=60, check=False)
    assert result.returncode == 125, result.stderr
    assert "class_context_missing" in result.stderr
    assert not calls.exists()


def test_nonleader_thread_exec_preserves_native_exit_status(tmp_path):
    # Force mediation even on hosts without Docker. The payload never uses it.
    client = tmp_path / "docker"
    client.write_text("unused configured client")
    environment = dict(os.environ, PRISMABUILD_DOCKER_REAL=str(client))
    code = (
        "import os, sys, threading\n"
        "threading.Thread(target=lambda: os.execv(sys.executable, "
        "[sys.executable, '-c', 'print(\"native exec completed\")'])).start()\n"
        "threading.Event().wait(60)\n"
        "raise SystemExit('exec did not replace the process')\n"
    )
    result = subprocess.run(launch_argv([sys.executable, "-c", code], environment),
                            env=environment, capture_output=True, text=True,
                            timeout=90, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "native exec completed"


def test_profiler_is_untraced_but_its_actual_payload_is_mediated(tmp_path, monkeypatch):
    profiler_trace = tmp_path / "profiler-tracer"
    sampler = tmp_path / "sampler"
    sampler.write_text(
        f"#!{sys.executable}\n"
        "import json, pathlib, subprocess, sys\n"
        "if '--version' in sys.argv:\n"
        "    print('test sampler'); raise SystemExit(0)\n"
        "args = sys.argv[1:]\n"
        "assert '--nonblocking' in args\n"
        f"pathlib.Path({str(profiler_trace)!r}).write_text(\n"
        "    pathlib.Path('/proc/self/status').read_text().split('TracerPid:')[1].split()[0])\n"
        "result = subprocess.run(args[args.index('--') + 1:], check=False)\n"
        # This fixture profile only allows Core to finish normal ingestion. The
        # test's expectations are independently observed process relationships,
        # not these fabricated sample values (real py-spy has its own regression).
        "pathlib.Path(args[args.index('--output') + 1]).write_text(json.dumps({\n"
        "    '$schema': 'https://www.speedscope.app/file-format-schema.json',\n"
        "    'shared': {'frames': []}, 'profiles': []}))\n"
        "raise SystemExit(result.returncode)\n"
    )
    sampler.chmod(0o755)
    monkeypatch.setattr(pb.PySpyProfileBackend, "locate", lambda self: str(sampler))
    client = tmp_path / "docker"
    client.write_text("unused configured client")
    action = _action(
        tmp_path, Path(sys.executable), {"PRISMABUILD_DOCKER_REAL": str(client)},
        mode="direct", args=["-c", "import pathlib; "
                              "status = pathlib.Path('/proc/self/status').read_text(); "
                              "pathlib.Path('result.bin').write_text("
                              "status.split('TracerPid:')[1].split()[0])"])
    action.pop("action_key")
    action["params"]["profile"] = "sample"
    action = pb.seal_action(action)
    original = copy.deepcopy(action)
    pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=tmp_path)
    assert profiler_trace.read_text() == "0"
    assert int((tmp_path / "result.bin").read_text()) > 0
    assert action == original
