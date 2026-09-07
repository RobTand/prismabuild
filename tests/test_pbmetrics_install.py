"""The exporter's installer must stay coupled to what the fleet publishes.

`pbmetrics` was written, documented, and never started: nothing ran it and
nothing scraped it, so a well-built exporter answered none of the questions it
was built for. The installer closes that, and these tests guard the two joints
where it could quietly come apart again -- the file it runs must be a file the
fleet actually publishes, and the script must refuse rather than half-install.

Nothing here installs anything, starts a service, or touches Netdata.
"""

from __future__ import annotations

from pathlib import Path
import os
import re
import subprocess
import sys

import pytest
import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
INSTALLER = REPOSITORY / "tools" / "fleet" / "install_pbmetrics.sh"
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))


def test_the_installer_runs_a_file_the_fleet_publishes() -> None:
    """A unit pointing into the published runtime needs that file published.

    Workers execute the published generation, not a checkout. If `pbmetrics.py`
    ever left `publish_runtime.py`'s list, the unit would keep pointing at a
    path that no longer appears in new generations and would fail only after a
    publish -- the moment nobody is looking at the exporter.
    """

    import publish_runtime

    assert "pbmetrics.py" in publish_runtime.FLEET_SCRIPTS
    assert "/tools/fleet/pbmetrics.py" in INSTALLER.read_text()
    # And the installer itself is published, because the boxes that most need
    # it -- dl380g10 and sparklina -- have no checkout to run it from.
    assert "install_pbmetrics.sh" in publish_runtime.FLEET_SCRIPTS


def test_the_installer_refuses_rather_than_half_installs() -> None:
    """Every precondition exits non-zero before anything is written."""

    text = INSTALLER.read_text()

    assert "set -euo pipefail" in text
    # The root check has to come before any write, or a non-root run leaves a
    # partial installation behind while reporting a permission error.
    root_check = text.index("must run as root")
    for write in ("systemctl", "/etc/systemd/system", "/etc/netdata"):
        assert root_check < text.index(write), write
    # The exporter is proved against the real queue before a unit exists to
    # restart-loop on it.
    assert text.index("--once") < text.index("systemctl enable")


def test_the_installer_binds_loopback_and_writes_nothing() -> None:
    """The exporter reads the queue; a unit that could write it is a hazard.

    Binding beyond loopback would also publish one box's view of the whole
    fleet's queue, which is a deliberate choice rather than a default: every
    box can run its own.
    """

    text = INSTALLER.read_text()

    assert "--listen 127.0.0.1" in text
    assert "ProtectSystem=strict" in text
    assert "ProtectHome=read-only" in text
    assert "NoNewPrivileges=yes" in text


def test_the_scrape_does_not_outpace_the_exporter_cache() -> None:
    """A scrape faster than the cache buys repetition, not resolution."""

    import pbmetrics

    interval = re.search(r'"update_every":\s*(\d+)', INSTALLER.read_text())
    assert interval is not None
    assert float(interval.group(1)) >= pbmetrics.DEFAULT_CACHE_SECONDS


def test_the_script_parses() -> None:
    assert subprocess.run(["bash", "-n", str(INSTALLER)]).returncode == 0


@pytest.fixture
def install_fixture(tmp_path):
    """Run the real installer with only its absolute /etc paths redirected.

    Every privileged command is a stub; the exporter and queue are temporary.
    There is no live service or collector access in this harness.
    """
    etc = tmp_path / "etc"
    (etc / "systemd/system").mkdir(parents=True)
    (etc / "netdata/go.d").mkdir(parents=True)
    runtime = tmp_path / "runtime"
    (runtime / "tools/fleet").mkdir(parents=True)
    (runtime / "tools/fleet/pbmetrics.py").write_text("raise SystemExit(0)\n")
    queue = tmp_path / "queue"
    queue.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    commands = {
        "id": '#!/bin/sh\necho 0\n',
        "sudo": '#!/bin/sh\nshift 2\nexec "$@"\n',
        "systemctl": '#!/bin/sh\necho "$*" >> "$CALLS"\n'
        'if [ "$*" = "restart netdata" ]; then exit "${RESTART_RC:-0}"; fi\n'
        'if [ "$*" = "is-active netdata" ]; then exit "${ACTIVE_RC:-0}"; fi\n',
    }
    for name, content in commands.items():
        path = bin_dir / name
        path.write_text(content)
        path.chmod(0o755)
    script = tmp_path / "install.sh"
    script.write_text(INSTALLER.read_text().replace("/etc/", str(etc) + "/"))
    calls = tmp_path / "calls"
    env = {
        **os.environ, "PATH": str(bin_dir) + ":" + os.environ["PATH"],
        "PBMETRICS_RUNTIME": str(runtime), "PBMETRICS_QUEUE": str(queue),
        "PBMETRICS_PYTHON": sys.executable, "CALLS": str(calls),
    }
    config = etc / "netdata/go.d/prometheus.conf"

    def run(**overrides):
        return subprocess.run(["bash", str(script)], env={**env, **overrides},
                              text=True, capture_output=True)

    return config, calls, run


def test_install_preserves_jobs_and_is_idempotent(install_fixture):
    config, calls, run = install_fixture
    unrelated = {"name": "unrelated", "url": "http://127.0.0.1:9090/metrics"}
    config.write_text(yaml.safe_dump({"jobs": [unrelated]}))
    result = run()
    assert result.returncode == 0, result.stderr
    assert config.read_text().count("jobs:") == 1
    first = config.read_bytes()
    loaded = yaml.safe_load(first)
    assert loaded["jobs"][0] == unrelated
    assert [job["name"] for job in loaded["jobs"]] == ["unrelated", "prismabuild"]
    assert loaded["jobs"][1]["update_every"] == 10
    result = run()
    assert result.returncode == 0, result.stderr
    assert config.read_bytes() == first
    backups = list(config.parent.glob("prometheus.conf.pb-before.*"))
    assert len(backups) == 1
    assert yaml.safe_load(backups[0].read_text()) == {"jobs": [unrelated]}
    assert "is-active netdata" in calls.read_text()


@pytest.mark.parametrize("original", [None, "", "# collector settings\n", "update_every: 20\n", "jobs: []\n"])
def test_install_creates_one_job(install_fixture, original):
    config, _, run = install_fixture
    if original is not None:
        config.write_text(original)
    result = run(PBMETRICS_PORT="9470")
    assert result.returncode == 0, result.stderr
    loaded = yaml.safe_load(config.read_text())
    assert loaded["jobs"] == [{"name": "prismabuild", "url": "http://127.0.0.1:9470/metrics", "update_every": 10}]
    if original == "update_every: 20\n":
        assert loaded["update_every"] == 20


@pytest.mark.parametrize("original", [
    "jobs: [\n", "[]\n", "null\n", "jobs: null\n", "jobs: {}\n", "jobs: [1]\n",
    "jobs: []\njobs: []\n", "jobs: [{name: other, name: duplicate}]\n",
    "jobs: [{name: prismabuild}, {name: prismabuild}]\n",
    "defaults: &defaults {name: other}\njobs: [*defaults]\n",
    "jobs: [{<<: {name: other}}]\n", "jobs: !unknown []\n",
    "jobs: []\n---\njobs: []\n",
    "jobs: [{name: on}]\n", "jobs: [{name: other, value: 1:20}]\n",
    "jobs: [{name: other, value: 2026-09-06}]\n",
])
def test_invalid_or_ambiguous_config_refused_before_changes(install_fixture, original):
    config, calls, run = install_fixture
    config.write_text(original)
    result = run()
    assert result.returncode != 0
    assert "invalid Netdata configuration" in result.stderr
    assert config.read_text() == original
    assert not calls.exists()
    assert not list(config.parent.glob("prometheus.conf.pb-*"))
    assert not (config.parents[2] / "systemd/system/prismabuild-metrics.service").exists()


def test_existing_quoted_job_and_comment_stay_unchanged(install_fixture):
    config, _, run = install_fixture
    original = '# keep this comment\njobs:\n  - name: "prismabuild"\n    url: http://127.0.0.1:9469/metrics\n'
    config.write_text(original)
    config.chmod(0o640)
    result = run()
    assert result.returncode == 0, result.stderr
    assert config.read_text() == original
    assert config.stat().st_mode & 0o777 == 0o640


def test_name_in_comment_does_not_hide_missing_job(install_fixture):
    config, _, run = install_fixture
    config.write_text('# name: prismabuild\njobs: [{name: other}]\n')
    result = run()
    assert result.returncode == 0, result.stderr
    assert [job["name"] for job in yaml.safe_load(config.read_text())["jobs"]] == ["other", "prismabuild"]


@pytest.mark.parametrize("failure", [{"RESTART_RC": "1"}, {"ACTIVE_RC": "3"}])
def test_netdata_failure_reported(install_fixture, failure):
    config, _, run = install_fixture
    original = "jobs: [{name: unrelated}]\n"
    config.write_text(original)
    result = run(**failure)
    assert result.returncode != 0
    assert "Netdata failed after installation" in result.stderr
    backups = list(config.parent.glob("prometheus.conf.pb-before.*"))
    assert len(backups) == 1
    assert backups[0].read_text() == original


def test_missing_yaml_dependency_refused(install_fixture, tmp_path):
    config, calls, run = install_fixture
    config.write_text("jobs: []\n")
    (tmp_path / "yaml.py").write_text('raise ImportError("fixture: missing yaml")\n')
    result = run(PYTHONPATH=str(tmp_path))
    assert result.returncode != 0
    assert "needs PyYAML" in result.stderr
    assert config.read_text() == "jobs: []\n"
    assert not calls.exists()


def test_changed_file_preserves_mode(install_fixture):
    config, _, run = install_fixture
    config.write_text("jobs: []\n")
    config.chmod(0o640)
    result = run()
    assert result.returncode == 0, result.stderr
    assert config.stat().st_mode & 0o777 == 0o640


def test_symlink_config_refused(install_fixture, tmp_path):
    config, calls, run = install_fixture
    target = tmp_path / "original.yml"
    target.write_text("jobs: []\n")
    config.symlink_to(target)
    result = run()
    assert result.returncode != 0
    assert "symlink" in result.stderr
    assert target.read_text() == "jobs: []\n"
    assert config.is_symlink()
    assert not calls.exists()
