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
#: are the measurement quoted above.  dl380g10 was read the same way on
#: 2026-09-05, 25.11.2 ``slurmd -C`` in an ``ubuntu:26.04`` container on that
#: box, and it confirmed rather than changed what ``lscpu`` had said:
#: ``CPUs=80 SocketsPerBoard=2 CoresPerSocket=20 ThreadsPerCore=2``.
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

    In the other direction the Sparks do resolve ``dl380g10``, wrong answers
    first: ``getent hosts`` gives ``::`` and ``getent ahostsv4`` gives
    192.168.1.165, a host silent to ping, before the live 192.168.1.107.  Both
    come from the router at 192.168.1.1, which holds two A records for
    ``dl380g10.lan``; it is a stale DHCP record there, not avahi.

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


# -- the default a hand-run job is charged -----------------------------------


def _global_setting(text: str, name: str) -> str | None:
    """A top-level ``Name=value``, ignoring comments and node stanzas."""

    for line in re.sub(r"\\\n\s*", " ", text).splitlines():
        line = line.strip()
        if line.startswith("#") or line.startswith(("NodeName=", "PartitionName=")):
            continue
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].split()[0]
    return None


def test_a_job_that_names_no_memory_is_charged_a_default_not_the_node() -> None:
    """``CR_Core_Memory`` with no default charges the node's whole
    ``RealMemory``, so one ``sbatch`` run by hand without ``--mem`` holds a box
    against every other job.  The lane always sends ``--mem``; an operator at a
    shell does not, and that is the case nothing else covers.
    """

    text = CONF.read_text(encoding="utf-8")
    assert "CR_Core_Memory" in text
    assert _global_setting(text, "DefMemPerCPU") is not None, (
        "no DefMemPerCPU: a raw sbatch takes the node's whole RealMemory"
    )
    # One or the other, never both: SLURM rejects a configuration that sets
    # DefMemPerNode alongside DefMemPerCPU.
    assert _global_setting(text, "DefMemPerNode") is None


def test_the_memory_default_is_one_every_node_can_honour_at_full_occupancy() -> None:
    """The default is applied before a node is chosen.

    A per-core default above a node's own RealMemory/CPUs ratio makes a
    whole-node job on that node ask for more memory than the node offers, and
    SLURM leaves it pending rather than running it somewhere smaller.  So the
    fleet-wide value is the minimum of the three ratios: 61440/80 = 768 on
    dl380g10, against 3686 and 4096 on the Sparks.
    """

    text = CONF.read_text(encoding="utf-8")
    setting = _global_setting(text, "DefMemPerCPU")
    assert setting is not None, "no DefMemPerCPU to check"
    default = int(setting)
    ratios = {
        node: int(fields["RealMemory"]) // int(fields["CPUs"])
        for node, fields in node_stanzas(text).items()
    }
    assert default == min(ratios.values()), ratios
    for node, fields in node_stanzas(text).items():
        charged = default * int(fields["CPUs"])
        assert charged <= int(fields["RealMemory"]), (
            f"{node}: a whole-node job defaults to {charged} MiB of "
            f"{fields['RealMemory']} MiB"
        )
