"""Raw v1 bytes and both client/service versions against #1894's Git source."""
import http.client
import importlib
import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from prismabuild import queue_bell, queue_notifier

BASE_BELL_REVISION = "8b2d47ff824e89aa60f83126d397bbd4207fe3d2"
V1_EPOCH = "a" * 32
V1_KEY = "b" * 64


@pytest.fixture(scope="module")
def base_bell_modules(tmp_path_factory):
    parent = tmp_path_factory.mktemp("v1-base")
    package = parent / "bell_base_1894"
    package.mkdir()
    (package / "__init__.py").write_text("")
    checkout = Path(__file__).resolve().parents[1]
    for name in ("queue_bell", "queue_bell_events", "queue_bell_http", "queue_notifier"):
        source = subprocess.check_output(["git", "-C", str(checkout), "show",
            f"{BASE_BELL_REVISION}:src/prismabuild/{name}.py"])
        (package / f"{name}.py").write_bytes(source)
    sys.path.insert(0, str(parent))
    try:
        yield SimpleNamespace(client=importlib.import_module("bell_base_1894.queue_bell"),
                              server=importlib.import_module("bell_base_1894.queue_notifier"))
    finally:
        sys.path.remove(str(parent))
        for name in tuple(sys.modules):
            if name == "bell_base_1894" or name.startswith("bell_base_1894."):
                sys.modules.pop(name)


def _v1_exchange(client_module, server_module, case, root):
    for directory in queue_bell.STATE_DIRS:
        (root / directory).mkdir(parents=True)
    service = server_module.BellService(root, [("127.0.0.1", 0)])
    # Freeze only the nondeterministic epoch, before either service starts.
    # All response serialization and event projection come from the real code.
    service.ring.epoch = V1_EPOCH
    with service:
        port = service.servers[0].server_address[1]
        assert port != queue_bell.DEFAULT_PORT
        client = client_module.Client(root, host="127.0.0.1", port=port)
        cursor = client_module.Cursor(V1_EPOCH, 0)
        path = "/v1/now"
        if case in ("events", "empty", "reset"):
            if case == "events":
                service.ring.append("done", V1_KEY)
            elif case == "empty":
                service.ring.append("ready", V1_KEY)
            else:
                service.ring.reset()
            path = f"/v1/wait?since={cursor}&dirs=done&keys={V1_KEY}&timeout=0.01"
        elif case == "bad-query":
            path = "/v1/wait?since=invalid&timeout=0"
        elif case == "unknown-endpoint":
            path = "/v1/missing"
        elif case == "wrong-queue":
            client.queue_root = root / "other"

        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        try:
            connection.request("GET", path,
                headers={"X-PrismaBuild-Queue": str(client.queue_root.absolute())})
            response = connection.getresponse()
            status, raw = response.status, response.read()
        finally:
            connection.close()
        if case in ("bad-query", "unknown-endpoint", "wrong-queue"):
            assert status == {"bad-query": 400, "unknown-endpoint": 404, "wrong-queue": 503}[case]
            with pytest.raises(client_module.Unavailable, match=str(status)):
                client._request(path, deadline=time.monotonic() + 1)
        elif case == "now":
            actual = client.now()
            assert (actual.epoch, actual.seq) == (V1_EPOCH, 0)
        else:
            assert client.wait(cursor, ("done",), (V1_KEY,), 0.05) == json.loads(raw)
        return status, raw


@pytest.mark.parametrize("client_version", ["base", "current"])
@pytest.mark.parametrize("server_version", ["base", "current"])
@pytest.mark.parametrize("case", ["now", "events", "empty", "reset", "bad-query", "wrong-queue", "unknown-endpoint"])
def test_raw_v1_body_and_mixed_versions_match_base(
        tmp_path, base_bell_modules, client_version, server_version, case):
    baseline = _v1_exchange(base_bell_modules.client, base_bell_modules.server,
                            case, tmp_path / "baseline")
    actual = _v1_exchange(base_bell_modules.client if client_version == "base" else queue_bell,
                         base_bell_modules.server if server_version == "base" else queue_notifier,
                         case, tmp_path / "actual")
    assert actual == baseline
