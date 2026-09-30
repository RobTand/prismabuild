"""An OS library update must not stop the broker under a running action.

needrestart selects every running service whose process still maps a library
that an upgrade replaced and restarts the selection.  On 2026-09-30 an
unattended upgrade replaced libssl3t64 on dl380g10, needrestart queued
``prismabuild-resource-broker.service`` for restart, and the stop closed every
connected launcher's execution channel without its result: action
193fea552ee6bf93c271a279c41581071f459ece08bca1164b49f06042612bc0 was recorded
failed with return code 125 ("resource broker closed without an execution
result") while its payload ran to completion (RobTand/prismabuild#1378).

The broker installer provisions a needrestart ``conf.d`` fragment that
excludes exactly this unit, and this test evaluates that fragment the way
needrestart does: the config files are Perl, they are loaded with ``do``, and
the decision for a unit is the value of the first regex that matches in the
lexically sorted ``override_rc`` keys, with 0 meaning skip
(/usr/sbin/needrestart:1135-1162).  A Python re-implementation of the load,
the match or the ordering would not be evidence about the configuration that
actually protects the service.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FLEET = ROOT / "tools" / "fleet"
INSTALLER = FLEET / "install_resource_broker.sh"
FRAGMENT = FLEET / "50-prismabuild-resource-broker.conf"
BROKER = "prismabuild-resource-broker.service"
NEAR_NAMES = (
    "prismabuild-resource-broker-helper.service",
    "prismabuild-supervisor.service",
    "prismabuild-worker.service",
)
CONTROL = "ssh.service"
INSTALLED_CONFIG = Path("/etc/needrestart/needrestart.conf")

#: needrestart's own load-and-decide sequence, reduced to its exact rule: a
#: conf file is Perl evaluated with do, the override keys are stringified
#: compiled regexes sorted lexically, and the first match wins.
_HARNESS = r'''
my %nrconf = (blacklist_rc => [], override_rc => {}, defno => 0);
my ($main, $fragment, @units) = @ARGV;
for my $file ($main, $fragment) {
    -r $file or die "unreadable needrestart config: $file\n";
    eval do { local(@ARGV, $/) = $file; <> };
    die "Error parsing $file: $@" if $@;
}
my @keys = sort keys %{$nrconf{override_rc}};
for my $rc (@units) {
    my $restart = !$nrconf{defno};
    for my $re (@keys) {
        next unless $rc =~ /$re/;
        $restart = $nrconf{override_rc}->{$re};
        last;
    }
    print "$rc=$restart\n";
}
'''

#: The shape of the installed main configuration a fragment must merge into:
#: entries it replaces rather than extends are entries the merge lost.
FIXTURE = """\
$nrconf{defno} = 0;
$nrconf{override_rc} = {
    qr(^dbus) => 0,
    qr(^docker) => 0,
    qr(^apt-daily) => 0,
    qr(^systemd-logind) => 0,
};
"""


def _decisions(main: Path, fragment: Path, units) -> dict[str, int]:
    if shutil.which("perl") is None:
        pytest.skip("needrestart configuration is Perl; perl is not installed here")
    result = subprocess.run(
        ["perl", "-e", _HARNESS, str(main), str(fragment), *units],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    decisions: dict[str, int] = {}
    for line in result.stdout.splitlines():
        unit, _, value = line.rpartition("=")
        decisions[unit] = int(value)
    return decisions


def _fixture(tmp_path: Path) -> Path:
    path = tmp_path / "needrestart-main.conf"
    path.write_text(FIXTURE, encoding="utf-8")
    return path


def test_the_fragment_the_installer_provisions_exists() -> None:
    assert FRAGMENT.is_file(), (
        "no needrestart exclusion fragment for the broker: an OS library "
        "update can still stop the broker under a running action (#1378)")


def test_without_the_fragment_needrestart_would_restart_the_broker(tmp_path) -> None:
    """The incident's selection, reproduced with needrestart's own rule."""

    decisions = _decisions(_fixture(tmp_path), _fixture(tmp_path),
                           (BROKER,) + NEAR_NAMES)
    assert decisions[BROKER] == 1
    assert all(decisions[name] == 1 for name in NEAR_NAMES)


def test_the_fragment_excludes_only_the_broker_unit(tmp_path) -> None:
    decisions = _decisions(_fixture(tmp_path), FRAGMENT,
                           (BROKER,) + NEAR_NAMES + (CONTROL, "dbus.service",
                                                     "docker.service"))
    assert decisions[BROKER] == 0
    assert all(decisions[name] == 1 for name in NEAR_NAMES)
    assert decisions[CONTROL] == 1
    # The fragment merges into the main configuration instead of replacing
    # the override table: a wholesale assignment would lose these entries.
    assert decisions["dbus.service"] == 0
    assert decisions["docker.service"] == 0


def test_the_fragment_composes_with_the_installed_needrestart_config(tmp_path) -> None:
    if not INSTALLED_CONFIG.is_file():
        pytest.skip("no installed needrestart main configuration on this box")
    decisions = _decisions(INSTALLED_CONFIG, FRAGMENT, (BROKER,))
    assert decisions[BROKER] == 0


def test_the_installer_provisions_the_fragment_without_restarting() -> None:
    text = INSTALLER.read_text(encoding="utf-8")
    assert FRAGMENT.name in text
    assert "/etc/needrestart/conf.d" in text
    assert "systemctl restart" not in text
    subprocess.run(["bash", "-n", str(INSTALLER)], check=True)


def test_the_fragment_travels_with_the_published_installer() -> None:
    import publish_runtime

    assert FRAGMENT.name in publish_runtime.FLEET_DATA
