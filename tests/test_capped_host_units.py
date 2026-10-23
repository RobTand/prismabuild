"""Run the installers against redirected fixture paths, never host units."""
import configparser
import os
import re
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("cap_first", [True, False])
@pytest.mark.parametrize("service", ["prismabuild-client-upgrade.service", "prismabuild-movement-publish.service"])
def test_cap_install_covers_ancillary_units(tmp_path, service, cap_first):
    staged = tmp_path / "staged"
    staged.mkdir()
    bindir = tmp_path / "bin"
    bindir.mkdir()
    sandbox = tmp_path / "filesystem"
    for prefix in ("etc", "opt", "var/lib", "home/rob"):
        (sandbox / prefix).mkdir(parents=True, exist_ok=True)
    (sandbox / "etc/systemd/system").mkdir(parents=True)
    programs = {
        "id": "print('0')",
        "stat": "import sys; print('0' if '%u' in sys.argv else '755')",
        "chown": "pass",
        "systemctl": "import sys; sys.exit(3 if 'is-active' in sys.argv else 0)",
        "install": "import os,sys\na=[]; i=1\nwhile i<len(sys.argv):\n if sys.argv[i] in ('-o','-g'): i+=2\n else: a.append(sys.argv[i]); i+=1\nos.execv('/usr/bin/install',['install',*a])",
    }
    for name, program in programs.items():
        path = bindir / name
        path.write_text('#!' + sys.executable + '\n' + program + '\n')
        path.chmod(0o755)
    for leaf in ("digest_primitives", "generation_receipt", "upgrade_client", "runtime_publication"):
        (staged / (leaf + ".py")).write_text("# fixture staging input\n")
    installers = [
        ("install_client_upgrader.sh", []),
        ("install_movement_publisher.sh", ["--approval-key", "0" * 64]),
        ("install_supervisor_unit.sh", ["--system", "--slice", "prismabuild.slice", "--cpus", "4-7,12-15", "--memory-gb", "32"]),
    ]
    if cap_first:
        installers = installers[-1:] + installers[:-1]
    for name, args in installers:
        source = (ROOT / "tools/fleet" / name).read_text()
        source = re.sub(r"/var/lib|/home/rob|/etc|/opt",
                        lambda match: str(sandbox / match.group().lstrip('/')), source)
        script = staged / name
        script.write_text(source)
        result = subprocess.run(["bash", str(script), *args], capture_output=True, text=True,
                                env={**os.environ, "PATH": str(bindir) + ':' + os.environ['PATH']}, timeout=30)
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
