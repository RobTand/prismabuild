"""Isolated stopped fleet and historical-root coverage for custody tests."""
import socket
import time
from pathlib import Path

import pytest

from prismabuild import fleet_roster, legacy_custody_host
from prismabuild.legacy_custody import LegacyCustodyMigration


@pytest.fixture(autouse=True)
def stopped_legacy_hosts(tmp_path, monkeypatch):
    host = socket.gethostname()
    gate = {"draining": True, "owner": "private-test-operator", "changed_unix": 1.}
    real_stopped = legacy_custody_host.legacy_stopped_host
    def observe_stopped():
        return {"host": host, "observed_unix": time.time(), "gate": gate,
                "broker_draining": True, "active_scopes": 0, "supervisor": "inactive",
                "writer_processes": 0, "cron_fenced": True}
    monkeypatch.setattr(legacy_custody_host, "legacy_stopped_host", observe_stopped)
    monkeypatch.setattr(fleet_roster.Roster, "at", classmethod(lambda cls, *a, **k:
        fleet_roster.Roster.capture(b'{"boxes":{"' + host.encode() + b'":{}}}')))
    canonical_box = fleet_roster.Roster.canonical_box
    monkeypatch.setattr(fleet_roster.Roster, "canonical_box", lambda self, name:
        canonical_box(self, host if name in {"storage", "test"} else name))
    original = LegacyCustodyMigration._legacy_scan_evidence
    def fixture_coverage(self, kind, name, holder, key, owner):
        if self.roots is None:
            root = self.queue.root.parent / "legacy-materialized"
            root.mkdir(exist_ok=True)
            self.roots = {"runtime": "e9e9d907721282f517e4e8feca40eda1aec79e5d",
                "hosts": {host: {"coverage_evidence": "isolated fixture's complete e9 roots and empty occupancy",
                "all_historical_roots": True, "roots": [
                    {"path": str(root), "kind": "checkout"},
                    {"path": str(root / "cleanup-failures"), "kind": "cleanup-failures"},
                    {"path": str(root.parent / "legacy-quarantine"), "kind": "quarantine"}],
                "occupancy_roots": {}}}}
        covered = self.roots["hosts"][host]
        covered.setdefault("occupancy_roots", {}).setdefault(kind + ":" + name + ":" + holder,
            [str(self.queue.root.parent / "empty-legacy-occupancy" / kind / name / holder)])
        for spec in covered["roots"]:
            Path(spec["path"]).mkdir(parents=True, exist_ok=True)
        for path in covered["occupancy_roots"][kind + ":" + name + ":" + holder]:
            Path(path).mkdir(parents=True, exist_ok=True)
        return original(self, kind, name, holder, key, owner)
    monkeypatch.setattr(LegacyCustodyMigration, "_legacy_scan_evidence", fixture_coverage)
    return {"real_stopped_host": real_stopped, "fixture_coverage": fixture_coverage,
            "real_scan_evidence": original, "gate": gate}
