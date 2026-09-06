"""Dashboard deployment preserves unrelated dashboards and concurrent edits."""
import copy
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("deploy_dashboard", ROOT / "fleet/observability/deploy_dashboard.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Client:
    url = "http://grafana.invalid"

    def __init__(self, existing=None, healthy=True):
        self.existing = existing
        self.healthy = healthy
        self.writes = []

    def request(self, path, body=None, missing_ok=False):
        if body is not None:
            self.writes.append((path, copy.deepcopy(body)))
            if path == "/api/dashboards/db":
                self.existing = {"dashboard": body["dashboard"], "meta": {"folderUid": body["folderUid"]}}
                return {"status": "success", "url": "/d/prismabuild-fleet/fleet", "version": 8}
            return {"uid": body["uid"]}
        if path.endswith("/health"):
            return {"status": "OK" if self.healthy else "ERROR"}
        if path.startswith("/api/datasources/"):
            return {"type": "prometheus", "name": "Existing metrics"}
        if path.startswith("/api/dashboards/"):
            return copy.deepcopy(self.existing)
        if path.startswith("/api/folders/"):
            return None
        raise AssertionError(path)


@pytest.fixture
def dashboard():
    return {"uid": "prismabuild-fleet", "title": "PB", "tags": ["prismabuild"], "version": 1,
            "panels": [{"id": 1}], "templating": {"list": [{"current": {}}]}}


def test_create_uses_selected_datasource_and_own_folder(tmp_path, dashboard):
    client = Client()
    result = MODULE.deploy(client, dashboard, "existing-prometheus", tmp_path)
    assert result["url"] == "http://grafana.invalid/d/prismabuild-fleet/fleet"
    assert client.writes[0] == ("/api/folders", {"uid": "prismabuild", "title": "PrismaBuild"})
    body = client.writes[1][1]
    assert body["dashboard"]["templating"]["list"][0]["current"]["value"] == "existing-prometheus"
    assert body["folderUid"] == "prismabuild"
    assert body["overwrite"] is False


def test_existing_dashboard_is_backed_up_and_version_checked(tmp_path, dashboard):
    previous = copy.deepcopy(dashboard)
    previous.update(id=42, version=7, title="Previous PB")
    existing = {"dashboard": previous, "meta": {"folderUid": "operators"}}
    client = Client(existing)
    MODULE.deploy(client, dashboard, "prom", tmp_path)
    assert len(client.writes) == 1
    body = client.writes[0][1]
    assert body["folderUid"] == "operators"
    assert body["dashboard"]["id"] == 42 and body["dashboard"]["version"] == 7
    assert body["overwrite"] is False
    import json
    assert json.loads((tmp_path / "prismabuild-fleet-v7.json").read_text()) == existing


def test_uid_collision_does_not_modify_an_unrelated_dashboard(tmp_path, dashboard):
    previous = copy.deepcopy(dashboard)
    previous["tags"] = ["network"]
    client = Client({"dashboard": previous, "meta": {}})
    with pytest.raises(ValueError, match="unrelated"):
        MODULE.deploy(client, dashboard, "prom", tmp_path)
    assert client.writes == []


def test_unhealthy_datasource_does_not_publish_empty_dashboard(tmp_path, dashboard):
    client = Client(healthy=False)
    with pytest.raises(ValueError, match="health"):
        MODULE.deploy(client, dashboard, "prom", tmp_path)
    assert client.writes == []


def test_version_conflict_is_not_retried_with_overwrite(tmp_path, dashboard):
    client = Client()
    original = client.request

    def conflict(path, body=None, missing_ok=False):
        if path == "/api/dashboards/db":
            client.writes.append((path, body))
            raise RuntimeError("Grafana /api/dashboards/db: HTTP 412")
        return original(path, body, missing_ok)

    client.request = conflict
    with pytest.raises(RuntimeError, match="412"):
        MODULE.deploy(client, dashboard, "prom", tmp_path)
    writes = [body for path, body in client.writes if path == "/api/dashboards/db"]
    assert len(writes) == 1 and writes[0]["overwrite"] is False
