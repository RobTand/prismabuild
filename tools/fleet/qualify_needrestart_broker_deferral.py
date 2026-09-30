#!/usr/bin/env python3
"""Read-only host-local check: does the installed needrestart config defer the broker?

Loads the installed main configuration alone (``/etc/needrestart/needrestart.conf``
by default), exactly as needrestart loads it: the file is Perl, evaluated with
``do``, and its own conf.d loader then evaluates the installed fragments.  The
command never composes the candidate repository fragment with the installed
configuration, because that composition passes even when deployment is absent --
which is precisely the state this check exists to detect.  It runs no process
scan and restarts nothing.

Reports the configuration path, sha256 and mode, plus needrestart's decision for
the broker's exact unit and controls.  Exits 0 when the broker is deferred
(restart suppressed), 1 when it is not, and 2 when the configuration or perl is
unavailable.  Run it on the host through the published pbrun entrypoint after
the installer provisions the configuration.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import stat
import subprocess
import sys

#: The published generation containing this tool supplies the digest owner,
#: in each of the publisher's layouts (tools/, tools/fleet/) and from a
#: checkout (#1386).  ``runtime_paths`` ships beside the tool in all three.
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import core  # noqa: E402

#: A new record type uses the independent ``prismabuild.*`` namespace (#1250).
#: This report type is absent from the observed current runtime generation,
#: and the inspected history found no deployed reader, so there is no known
#: deployed producer/reader contract to preserve (#1384).
SCHEMA = "prismabuild.needrestart_broker_deferral.v1"
DEFAULT_CONFIG = Path("/etc/needrestart/needrestart.conf")
BROKER = "prismabuild-resource-broker.service"
CONTROLS = (
    "prismabuild-resource-broker-helper.service",
    "prismabuild-supervisor.service",
    "ssh.service",
)

#: needrestart's exact matching rule (/usr/sbin/needrestart:1135-1162): the
#: override keys are stringified compiled regexes sorted lexically, the first
#: match decides, and 0 defers the service.  ``blacklist_rc`` and the other
#: configuration keys are loaded exactly as the installed files define them.
_PERL = r'''
my %nrconf = (blacklist_rc => [], override_rc => {}, defno => 0);
my $file = $ARGV[0];
-r $file or die "unreadable needrestart configuration: $file\n";
eval do { local(@ARGV, $/) = $file; <> };
die "Error parsing $file: $@" if $@;
my @keys = sort keys %{$nrconf{override_rc}};
for my $rc (@ARGV[1 .. $#ARGV]) {
    my $restart = !$nrconf{defno};
    my $match = "";
    for my $re (@keys) {
        next unless $rc =~ /$re/;
        $restart = $nrconf{override_rc}->{$re};
        $match = "$re";
        last;
    }
    print "$rc\t$restart\t$match\n";
}
'''


def _fail(message: str) -> int:
    """The process-boundary refusal: prints and returns status 2.

    Distinct from ``core._fail`` and ``dagster._fail``, which raise their
    own contract errors inside library validation; the collision is a
    registered exact-name exception (same_name_distinct, #1386).
    """

    print(f"qualify_needrestart_broker_deferral: {message}", file=sys.stderr)
    return 2


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Report whether the installed needrestart configuration "
                    "defers prismabuild-resource-broker.service; never adds a "
                    "candidate fragment and restarts nothing.",
    )
    parser.add_argument(
        "--config", type=Path, default=DEFAULT_CONFIG,
        help=f"installed needrestart main configuration (default: {DEFAULT_CONFIG}); "
             "its own conf.d loader evaluates the installed fragments")
    parser.add_argument("--json", action="store_true",
                        help="emit the decision report as one JSON object")
    args = parser.parse_args(argv)

    if shutil.which("perl") is None:
        return _fail("needrestart configuration is Perl; perl is not installed")
    if not args.config.is_file():
        return _fail(f"needrestart main configuration not found: {args.config}")
    try:
        raw = args.config.read_bytes()
        mode = stat.S_IMODE(args.config.stat().st_mode)
    except OSError as exc:
        return _fail(f"cannot read {args.config}: {exc}")

    result = subprocess.run(
        ["perl", "-e", _PERL, str(args.config), BROKER, *CONTROLS],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        return _fail(result.stderr.strip() or "configuration evaluation failed")
    decisions: dict[str, dict] = {}
    for line in result.stdout.splitlines():
        unit, restart, match = line.split("\t")
        decisions[unit] = {"deferred": restart == "0",
                           "matched_override": match or None}
    broker = {"unit": BROKER, **decisions[BROKER]}
    controls = {unit: {"unit": unit, **decisions[unit]} for unit in CONTROLS}
    report = {
        "schema": SCHEMA,
        "config": str(args.config),
        "config_sha256": core.raw_sha256(raw),
        "config_mode": f"{mode:04o}",
        "broker": broker,
        "controls": controls,
        "deferred": broker["deferred"],
    }
    if args.json:
        print(core._sorted_lf_bytes(report).decode("utf-8"), end="")
    else:
        print(f"config {report['config']} sha256={report['config_sha256']} "
              f"mode={report['config_mode']}")
        for record in (broker, *controls.values()):
            decision = "defer" if record["deferred"] else "restart"
            matched = (f" matched={record['matched_override']}"
                       if record["matched_override"] else "")
            print(f"  {record['unit']}: {decision}{matched}")
        print("verdict: broker deferred" if broker["deferred"]
              else "verdict: broker NOT deferred")
    return 0 if broker["deferred"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
