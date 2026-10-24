"""Acceptance: supported absolute Docker-client paths need runtime mediation.

Exercise the configured test client directly and through an ordinary native
Python launcher. No arbitrary renamed/copied binaries, raw daemon protocols,
mount manipulation, or general hostile-code sandbox guarantee is asserted.
The native child's argv comes from a declared input, so inspecting only the
parent command string cannot establish that no container work will execute.

These local actions deliberately have no worker-owned class context. Inventory
and native work remain usable, but run/create (including container aliases)
must not reach the client. The pool launch matrix separately supplies a real
ClassImagePolicy and changes its observations between launches.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

import pytest

from prismabuild import core as pb
from prismabuild.container_images import LOCAL_ENDPOINT
from test_core import (
    _body,
    _isolated_synthetic_launch_context,  # noqa: F401 -- reused autouse fixture
)


ALIASES = [("run",), ("create",), ("container", "run"), ("container", "create")]


def _client(tmp_path):
    """A supported, explicitly configured client; never discover host Docker."""
    calls = tmp_path / "daemon-work.jsonl"
    client = tmp_path / "docker"
    client.write_text(
        f"#!{sys.executable}\n"
        "import json, pathlib, sys\n"
        "args = sys.argv[1:]\n"
        "while args and args[0] in ('--host', '--context'):\n"
        "    args = args[2:]\n"
        "if args[:2] == ['context', 'inspect']:\n"
        # _local_daemon requests {{json .Endpoints.docker.Host}}, not the full
        # inspect object and not an unquoted endpoint string.
        f"    print(json.dumps({LOCAL_ENDPOINT!r})); raise SystemExit(0)\n"
        "if args[:1] == ['container']:\n"
        "    args = args[1:]\n"
        "if args[:2] == ['image', 'ls']:\n"
        "    pathlib.Path('result.bin').write_bytes(b'inventory')\n"
        "    raise SystemExit(0)\n"
        "if args[:1] not in (['run'], ['create']):\n"
        "    raise SystemExit(2)\n"
        f"with pathlib.Path({str(calls)!r}).open('a') as stream:\n"
        "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if '--cidfile' in args:\n"
        "    pathlib.Path(args[args.index('--cidfile') + 1]).write_text('e' * 64 + '\\n')\n"
        "pathlib.Path('result.bin').write_bytes(b'unmediated container launch')\n"
        "print('e' * 64)\n"
    )
    client.chmod(0o755)
    cgroup = tmp_path / "cgroup"
    cgroup.write_text("0::/\n")
    environment = {
        # Existing shim switches, not proposed launch-context configuration.
        "PRISMABUILD_DOCKER_TESTING": "1",
        "PRISMABUILD_DOCKER_REAL": str(client),
        "PRISMABUILD_CGROUP_FILE": str(cgroup),
        "PRISMABUILD_CONTAINER_OWNER": "a" * 64,
        "PRISMABUILD_CONTAINER_MARKER": str(tmp_path / "container.marker"),
    }
    return client, calls, environment


def _action(checkout, client, environment, *, mode, args):
    closure_paths = ["task_code.py"]
    if mode == "direct":
        argv = [str(client), *args]
    else:
        # An ordinary native launcher with data-dependent child argv, not an
        # attempt to hide/rename the supported client or escape a sandbox.
        request = checkout / "request.json"
        request.write_text(json.dumps([str(client), *args]))
        helper = checkout / "launcher.py"
        helper.write_text(
            "import json, pathlib, subprocess, sys\n"
            "raise SystemExit(subprocess.call(json.loads(pathlib.Path(sys.argv[1]).read_text())))\n"
        )
        closure_paths.extend([request.name, helper.name])
        argv = [sys.executable, helper.name, request.name]
    body = _body(checkout, argv=argv)
    body["environment"]["variables"] = dict(environment)
    body["code_closure"] = pb.build_code_closure(checkout, closure_paths)
    return pb.seal_action(body)


def _successful_action(tmp_path, checkout, action, expected):
    before = copy.deepcopy(action)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    result = pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout)
    assert action == before, "runtime mediation must not rewrite the sealed action"
    assert cas.lookup(action) == result["receipt"]
    assert "local_result_claim_sha256" in result
    assert (checkout / "result.bin").read_bytes() == expected
    (checkout / "result.bin").unlink()


@pytest.mark.parametrize("mode", ["direct", "native-child"])
@pytest.mark.parametrize("alias", ALIASES, ids=lambda alias: "-".join(alias))
def test_absolute_client_work_requires_worker_class_context(tmp_path, capfd, mode, alias):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    client, calls, environment = _client(tmp_path)
    # Positive controls belong to each refusal case: class enforcement must
    # preserve ordinary native work and inventory through the same client path.
    native_body = _body(checkout)
    native_body["environment"]["variables"] = dict(environment)
    _successful_action(tmp_path, checkout, pb.seal_action(native_body), b"result")
    inventory = _action(
        checkout, client, environment, mode=mode, args=["image", "ls", "--quiet"])
    _successful_action(tmp_path, checkout, inventory, b"inventory")
    assert not calls.exists(), "inventory must not create containers"
    assert not Path(environment["PRISMABUILD_CONTAINER_MARKER"]).exists()
    capfd.readouterr()  # Keep the refusal diagnostic independent of controls.

    action = _action(
        checkout, client, environment, mode=mode, args=[*alias, "undeclared:test"])
    before = copy.deepcopy(action)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    failure = None
    try:
        pb.run_local_action(action, cas_root=cas.root, checkout_root=checkout)
    except pb.LocalActionError as error:
        failure = error
    captured = capfd.readouterr()

    assert not calls.exists(), (
        "configured absolute Docker work reached the client without a worker-owned "
        "class context; a PATH shim alone cannot enforce the class launch gate"
    )
    assert failure is not None, "unsupported container work must not publish a sealed result"
    # A missing output, executable preflight failure, invalid fake endpoint, or
    # other unrelated LocalActionError is not evidence of class enforcement.
    diagnostic = (captured.err + "\n" + str(failure)).lower()
    assert "class" in diagnostic and any(
        word in diagnostic for word in ("context", "missing", "unavailable", "unknown")
    ), diagnostic
    assert action == before, "refusal must preserve the original sealed claims"
    assert cas.lookup(action) is None
    assert not (checkout / "result.bin").exists()
    assert not Path(environment["PRISMABUILD_CONTAINER_MARKER"]).exists(), (
        "missing context must refuse before an ownership transaction is opened"
    )
