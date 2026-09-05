"""Every NodeName stanza declares the topology its box actually reports.

A socket count SLURM cannot confirm is not a harmless label.  ``task/affinity``
binds against the declared layout, and a node whose declaration and detection
disagree comes up ``DRAINED`` with ``Low socket*core*thread count`` and no
mention of either file in the message.

This file exists because the two Spark stanzas carried a layout the hardware
does not have.  Measured 2026-09-05 by installing the fleet's own 25.11.2 debs
in an ``ubuntu:24.04`` container on sparky, where hwloc reads the host's
``/sys`` and so reports the host's topology::

    $ slurmd -C
    NodeName=... CPUs=20 Boards=1 SocketsPerBoard=1 CoresPerSocket=20 \
        ThreadsPerCore=1 RealMemory=124546 Gres=gpu:unknown:1

against a checked-in stanza that read ``SocketsPerBoard=2 CoresPerSocket=10``.
The 2x10 came from ``lscpu``, which prints the GB10's two heterogeneous core
clusters as two blocks of ten; SLURM does not call those sockets.
``fleet/slurm/install.sh`` cross-checks the same four numbers on the box and
refuses the install on a mismatch, so before this fix that script refused on
both Sparks by construction.
"""
from __future__ import annotations

from pathlib import Path
import re

CONF = Path(__file__).resolve().parents[1] / "fleet" / "slurm" / "slurm.conf"

#: What each box reports, and therefore what its stanza may say.  The Sparks
#: are the measurement quoted above; dl380g10 is ``lscpu`` on that box,
#: 2 sockets x 20 cores x 2 threads.
MEASURED = {
    "sparky": {
        "CPUs": 20, "SocketsPerBoard": 1, "CoresPerSocket": 20, "ThreadsPerCore": 1,
    },
    "gx10-6b77": {
        "CPUs": 20, "SocketsPerBoard": 1, "CoresPerSocket": 20, "ThreadsPerCore": 1,
    },
    "dl380g10": {
        "CPUs": 80, "SocketsPerBoard": 2, "CoresPerSocket": 20, "ThreadsPerCore": 2,
    },
}


def node_stanzas(text: str) -> dict[str, dict[str, str]]:
    """Every ``NodeName=`` stanza, backslash continuations joined.

    The same shape ``install.sh`` parses at the shell, kept here so a stanza
    that grows a line break does not quietly stop being checked.
    """

    joined = re.sub(r"\\\n\s*", " ", text)
    stanzas: dict[str, dict[str, str]] = {}
    for line in joined.splitlines():
        line = line.strip()
        if not line.startswith("NodeName="):
            continue
        fields = dict(
            token.split("=", 1) for token in line.split() if "=" in token
        )
        stanzas[fields["NodeName"]] = fields
    return stanzas


def test_every_declared_node_matches_what_its_box_reports() -> None:
    stanzas = node_stanzas(CONF.read_text(encoding="utf-8"))
    assert set(stanzas) == set(MEASURED), stanzas.keys()
    for node, expected in MEASURED.items():
        got = {key: int(stanzas[node][key]) for key in expected}
        assert got == expected, f"{node}: declared {got}, box reports {expected}"


def test_the_declared_layout_multiplies_out_to_the_declared_cpu_count() -> None:
    """A stanza whose product is not ``CPUs`` is wrong whatever the box says."""

    for node, fields in node_stanzas(CONF.read_text(encoding="utf-8")).items():
        product = (
            int(fields["SocketsPerBoard"])
            * int(fields["CoresPerSocket"])
            * int(fields["ThreadsPerCore"])
        )
        assert product == int(fields["CPUs"]), (
            f"{node}: {product} != CPUs={fields['CPUs']}"
        )


# -- addresses ---------------------------------------------------------------
#
# Measured 2026-09-05 on all three boxes.  These are the LAN addresses; the
# 10.100.96.0/24 fabric carries only the two Sparks, and the controller is not
# on it.
ADDRESSES = {
    "sparky": "192.168.1.180",
    "gx10-6b77": "192.168.1.110",
    "dl380g10": "192.168.1.107",
}


def test_every_node_carries_its_address_because_the_names_do_not_resolve() -> None:
    """Name resolution on this fleet does not answer SLURM's question.

    Measured 2026-09-05.  On dl380g10, the controller, ``getent hosts sparky``
    and ``getent hosts gx10-6b77`` both return nothing: nsswitch is
    ``files dns mymachines``, avahi is inactive, and neither Spark is in DNS or
    in ``/etc/hosts``.  slurmctld would have had no address for either node.

    In the other direction the Sparks do resolve ``dl380g10``, to two
    addresses, wrong one first: ``getent ahostsv4 dl380g10`` answers
    192.168.1.165 (mDNS, stale, silent to ping) before 192.168.1.107.

    So the addresses live in the file.  Ports were measured open the same day
    -- 6817 and 6818 answer "connection refused" rather than timing out, in
    both directions -- so nothing else was in the way.
    """

    text = CONF.read_text(encoding="utf-8")
    for node, address in ADDRESSES.items():
        assert node_stanzas(text)[node].get("NodeAddr") == address, node
    assert "SlurmctldHost=dl380g10(192.168.1.107)" in text


def test_the_install_script_refuses_an_address_the_box_does_not_hold() -> None:
    """The addresses are DHCP leases, so they can move.

    That has to be a refusal rather than a node which never registers, and the
    box running the install is the only one that can answer the question about
    itself.
    """

    script = (
        Path(__file__).resolve().parents[1] / "fleet" / "slurm" / "install.sh"
    ).read_text(encoding="utf-8")
    assert 'declared_address="$(stanza_field "$declared" NodeAddr)"' in script
    assert "ip -4 -o addr show scope global" in script
    assert "declares no NodeAddr" in script
