"""Run the installers against redirected fixture paths, never host units."""
import configparser
import json
import os
import re
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def installer(tmp_path):
    staged = tmp_path / "staged"
    staged.mkdir()
    bindir = tmp_path / "bin"
    bindir.mkdir()
    sandbox = tmp_path / "filesystem"
    for prefix in ("etc", "opt", "var/lib", "home/rob"):
        (sandbox / prefix).mkdir(parents=True, exist_ok=True)
    (sandbox / "etc/systemd/system").mkdir(parents=True)
    systemctl_log = tmp_path / "systemctl.jsonl"
    programs = {
        "id": "print('0')",
        "stat": "import sys; print('0' if '%u' in sys.argv else '755')",
        "chown": "pass",
        "systemctl": """import json, os, sys
args = sys.argv[1:]
with open(os.environ['PB_TEST_SYSTEMCTL_LOG'], 'a') as handle:
    handle.write(json.dumps(args) + '\\n')
unit = next((arg for arg in args if arg.endswith('.service')), '')
state = os.environ.get('PB_TEST_STATE', 'inactive') if unit == os.environ.get('PB_TEST_UNIT') else 'inactive'
if 'is-active' in args:
    sys.exit(0 if state == 'active' else 3)
if 'show' in args:
    print('inactive' if state == 'query-error' else '' if state == 'empty' else state)
    sys.exit(1 if state == 'query-error' else 0)
""",
        "install": "import os,sys\na=[]; i=1\nwhile i<len(sys.argv):\n if sys.argv[i] in ('-o','-g'): i+=2\n else: a.append(sys.argv[i]); i+=1\nos.execv('/usr/bin/install',['install',*a])",
    }
    for name, program in programs.items():
        path = bindir / name
        path.write_text('#!' + sys.executable + '\n' + program + '\n')
        path.chmod(0o755)
    for leaf in ("digest_primitives", "generation_receipt", "upgrade_client", "runtime_publication"):
        (staged / (leaf + ".py")).write_text("# fixture staging input\n")

    def run(name, *args):
        source = (ROOT / "tools/fleet" / name).read_text()
        source = re.sub(r"/var/lib|/home/rob|/etc|/opt",
                        lambda match: str(sandbox / match.group().lstrip('/')), source)
        script = staged / name
        script.write_text(source)
        return subprocess.run(["bash", str(script), *args], capture_output=True, text=True,
                              env={**os.environ, "PATH": str(bindir) + ':' + os.environ['PATH'],
                                   "PB_TEST_SYSTEMCTL_LOG": str(systemctl_log)}, timeout=30)

    return sandbox, run, systemctl_log


@pytest.mark.parametrize("cap_first", [True, False])
def test_cap_install_covers_ancillary_units(installer, cap_first):
    sandbox, run, _ = installer
    installers = [
        ("install_client_upgrader.sh", []),
        ("install_movement_publisher.sh", ["--approval-key", "0" * 64]),
        ("install_supervisor_unit.sh", ["--system", "--slice", "prismabuild.slice", "--cpus", "4-7,12-15", "--memory-gb", "32"]),
    ]
    if cap_first:
        installers = installers[-1:] + installers[:-1]
    for name, args in installers:
        result = run(name, *args)
        assert result.returncode == 0, (name, result.stdout, result.stderr)
    units = sandbox / "etc/systemd/system"
    effective = {}
    for unit in ("prismabuild-supervisor.service", "prismabuild-resource-broker.service",
                 "prismabuild-client-upgrade.service", "prismabuild-movement-publish.service"):
        parser = configparser.ConfigParser(interpolation=None, strict=False)
        members = [units / unit, *sorted((units / (unit + ".d")).glob("*.conf"))]
        parser.read(members)
        effective[unit] = parser.get("Service", "Slice", fallback="system.slice")
        assert parser.get("Service", "AllowedCPUs") == "4-7,12-15"
        if unit in ("prismabuild-client-upgrade.service", "prismabuild-movement-publish.service"):
            expected = "128M" if "client-upgrade" in unit else "256M"
            assert parser.get("Service", "MemoryMax") == expected
            assert parser.get("Service", "TasksMax") == "16"
    print("EFFECTIVE INSTALLER SLICES", effective)
    assert set(effective.values()) == {"prismabuild.slice"}, effective


@pytest.mark.parametrize("service", ["supervisor", "resource-broker", "client-upgrade", "movement-publish"])
@pytest.mark.parametrize("state", ["active", "deactivating", "activating", "failed", "empty", "query-error"])
def test_cap_install_refuses_without_stopped_evidence(installer, monkeypatch, service, state):
    sandbox, run, systemctl_log = installer
    unit = f"prismabuild-{service}.service"
    monkeypatch.setenv("PB_TEST_UNIT", unit)
    monkeypatch.setenv("PB_TEST_STATE", state)
    units = sandbox / "etc/systemd/system"
    for relative in ("prismabuild.slice", "prismabuild-supervisor.service",
                     "prismabuild-resource-broker.service.d/10-host-cap.conf",
                     "prismabuild-client-upgrade.service.d/10-host-cap.conf",
                     "prismabuild-movement-publish.service.d/10-host-cap.conf"):
        path = units / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("existing containment\n")
    marker = sandbox / "etc/prismabuild/host-cap.conf"
    marker.parent.mkdir(parents=True)
    marker.write_text("existing marker\n")
    before = {path.relative_to(sandbox): path.read_bytes() for path in sandbox.rglob('*') if path.is_file()}

    result = run("install_supervisor_unit.sh", "--system", "--slice", "prismabuild.slice",
                 "--cpus", "4-7,12-15", "--memory-gb", "32")

    assert result.returncode != 0, (state, unit, result.stdout, result.stderr)
    assert unit in result.stderr
    after = {path.relative_to(sandbox): path.read_bytes() for path in sandbox.rglob('*') if path.is_file()}
    assert after == before
    calls = [json.loads(line) for line in systemctl_log.read_text().splitlines()]
    assert all(call[0] in ("is-active", "show") for call in calls), calls


def test_cap_install_accepts_explicitly_inactive_services(installer):
    sandbox, run, systemctl_log = installer
    result = run("install_supervisor_unit.sh", "--system", "--slice", "prismabuild.slice",
                 "--cpus", "4-7,12-15", "--memory-gb", "32")
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "MemoryMax=32G" in (sandbox / "etc/systemd/system/prismabuild.slice").read_text()
    assert "Slice=prismabuild.slice" in (sandbox / "etc/prismabuild/host-cap.conf").read_text()
    calls = [json.loads(line) for line in systemctl_log.read_text().splitlines()]
    assert ["enable", "--now", "prismabuild.slice", "prismabuild-resource-broker.service",
            "prismabuild-supervisor.service"] in calls
