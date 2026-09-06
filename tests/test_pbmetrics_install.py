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
import re
import subprocess
import sys

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

    interval = re.search(r"update_every:\s*(\d+)", INSTALLER.read_text())
    assert interval is not None
    assert float(interval.group(1)) >= pbmetrics.DEFAULT_CACHE_SECONDS


def test_the_script_parses() -> None:
    assert subprocess.run(["bash", "-n", str(INSTALLER)]).returncode == 0
