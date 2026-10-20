#!/usr/bin/env python3
"""Prove which client updater this host has installed, without root (#1850).

A rolling activation between two generations that carry the receipt reader
needs current proof of the updater every enrolled participant has installed:
the root timer on each host reads the next generation's receipt with that
updater, so an updater that cannot is a host the activation would strand.
The installed updater and its status are root-owned and world-readable on
every enrolled host, so an ordinary worker can read them.  Run this tool as
one ordinary PrismaBuild action on each enrolled host, from the checkout that
will activate:

    pbrun.py --cwd CHECKOUT --tag HOST --priority 5 -- \\
        python3 tools/fleet/updater_proof.py --nonce "$(date +%s%N)"

The nonce makes every run a new action, so a proof is never a cache hit.
Then pass each action key to the activation, once per host:

    publish_runtime.py --activate-generation NAME --rollout rolling \\
        --rollout-reason TEXT --installed-updater-proof KEY ...

The tool prints one JSON proof line.  It reads the enrollment file the root
timer reads, hashes the installed ``upgrade_client.py`` and copies the fields
of the timer's last ``status.json`` that the publisher judges.  It writes
nothing and needs only the standard library.  :func:`judge` is the
publisher's side of the same contract; ``publish_runtime.py`` reads the proof
from the action's verified CAS result and names the host it ran on.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import stat
import sys
import time

SCHEMA = "prismabuild.installed_updater_proof.v1"
ENROLLMENT = Path("/etc/prismabuild/client-upgrade.json")
MAX_READ = 4 * 1024 * 1024
#: The longest a proof may wait for its activation.
PROOF_MAX_AGE_S = 900.0
#: How far a proof may lie in the publisher's future (clock skew).
PROOF_MAX_SKEW_S = 60.0
#: The timer runs every 60 s with up to 10 s of jitter; a status older than
#: this means the timer is not running.
STATUS_MAX_AGE_S = 300.0
#: The installed updater executes as root, so only root may own its bytes.
TRUSTED_UID = 0


class ProofRefused(ValueError):
    pass


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read(path: Path) -> tuple[bytes, os.stat_result]:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ProofRefused(f"{path} is not a regular file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(MAX_READ + 1)
    finally:
        os.close(fd)
    if len(raw) > MAX_READ:
        raise ProofRefused(f"{path} exceeds {MAX_READ} bytes")
    return raw, info


def tool_sha256() -> str:
    return _sha256(Path(__file__).read_bytes())


def observe(*, nonce: str, enrollment: Path = ENROLLMENT) -> dict[str, object]:
    """The proof this host gives, read as whoever runs it."""

    config = json.loads(_read(enrollment)[0])
    updater = Path(config["install_dir"]) / "upgrade_client.py"
    raw, info = _read(updater)
    status = json.loads(_read(Path(config["state_dir"]) / "status.json")[0])
    installed = status.get("installed")
    return {
        "schema": SCHEMA, "host": socket.gethostname(), "nonce": nonce,
        "observed_unix": time.time(), "tool_sha256": tool_sha256(),
        "updater": {"path": str(updater), "sha256": _sha256(raw),
                    "uid": info.st_uid, "mode": stat.S_IMODE(info.st_mode)},
        "status": {"state": status.get("state"), "host": status.get("host"),
                   "checked_unix": status.get("checked_unix"),
                   "installed_sha256": (installed.get("upgrade_client.py")
                                        if isinstance(installed, dict) else None)},
    }


def _number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def judge(proof: object, *, host: str, now: float, tool: str) -> str:
    """The installed updater digest ``proof`` proves for ``host``, or refuse.

    ``host`` is the queue's word for where the action ran, never the proof's.
    ``tool`` is the publisher's own digest of this file: a proof from another
    version of the tool is not a proof of this contract.
    """

    def refuse(reason: str) -> ProofRefused:
        return ProofRefused(f"installed updater proof for {host} {reason}")

    if not isinstance(proof, dict) or proof.get("schema") != SCHEMA:
        raise refuse("is not a " + SCHEMA + " record")
    if proof.get("host") != host:
        raise refuse(f"names host {proof.get('host')!r}")
    if proof.get("tool_sha256") != tool:
        raise refuse("was produced by another version of updater_proof.py")
    observed = proof.get("observed_unix")
    if not _number(observed) or not -PROOF_MAX_SKEW_S <= now - observed <= PROOF_MAX_AGE_S:
        raise refuse(f"is not current: observed {observed!r}, now {now:.0f}, "
                     f"limit {PROOF_MAX_AGE_S:.0f} s")
    updater = proof.get("updater")
    if not isinstance(updater, dict) or not isinstance(updater.get("sha256"), str):
        raise refuse("carries no installed updater digest")
    if (type(updater.get("uid")) is not int or updater["uid"] != TRUSTED_UID
            or type(updater.get("mode")) is not int or updater["mode"] & 0o022):
        raise refuse(f"shows unsafe custody of {updater.get('path')}: "
                     f"uid {updater.get('uid')!r}, mode {updater.get('mode')!r}")
    status = proof.get("status")
    if not isinstance(status, dict) or status.get("state") != "current":
        raise refuse(f"shows updater state {(status or {}).get('state')!r}, not 'current'")
    checked = status.get("checked_unix")
    if (status.get("host") != host or not _number(checked)
            or not 0 <= observed - checked <= STATUS_MAX_AGE_S):
        raise refuse(f"shows no timer tick on {host} in the {STATUS_MAX_AGE_S:.0f} s "
                     "before it was observed")
    if status.get("installed_sha256") != updater["sha256"]:
        raise refuse("shows installed bytes that differ from the updater's last report")
    return updater["sha256"]


def last_proof(payload: bytes) -> object:
    """The last proof line in an action log, or ``None``."""

    for line in reversed(payload.decode("utf-8", "replace").splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and value.get("schema") == SCHEMA:
            return value
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--nonce", required=True,
                    help="any value unique to this run, so the action is never a cache hit")
    ap.add_argument("--enrollment", type=Path, default=ENROLLMENT, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    try:
        proof = observe(nonce=args.nonce, enrollment=args.enrollment)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"updater_proof: no proof on {socket.gethostname()}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(proof, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
