"""A box announces the client addresses its NFS reads arrive from (#580).

The storage host's ``/proc/fs/nfsd/export_stats`` counts served bytes per
*client address*; the queue names the box that claimed an action by
*hostname*; and nothing else on the fleet joins the two -- dl380g10 resolves
neither Spark by name, and the fabric address a Spark mounts from
(``10.100.98.1``) is not the LAN address a resolver would return anyway.  So
the box says which addresses are its, on the offer it already refreshes, read
from the kernel's own address table.  Absent when it could not be read, never
empty: a storage role that finds no addresses protects every client as it did
before the field existed.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import box_capacity  # noqa: E402
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402

#: ``ip -4 -o addr show scope global`` on sparky, 2026-09-17: the LAN address
#: and the three fabric links, one of which is the NFS mount's source.
IP_ADDR = (
    "2: enP7s7    inet 192.168.1.180/24 brd 192.168.1.255 scope global dynamic noprefixroute enP7s7\\       valid_lft 56012sec preferred_lft 56012sec\n"
    "3: enp1s0f0np0    inet 10.100.96.1/24 brd 10.100.96.255 scope global noprefixroute enp1s0f0np0\\       valid_lft forever preferred_lft forever\n"
    "4: enp1s0f1np1    inet 10.100.98.1/24 brd 10.100.98.255 scope global noprefixroute enp1s0f1np1\\       valid_lft forever preferred_lft forever\n"
    "5: enP2p1s0f0np0    inet 10.100.97.1/24 brd 10.100.97.255 scope global noprefixroute enP2p1s0f0np0\\       valid_lft forever preferred_lft forever\n"
    "4: enp1s0f1np1    inet 10.100.98.1/24 brd 10.100.98.255 scope global secondary enp1s0f1np1\\       valid_lft forever preferred_lft forever\n"
    "garbage line without an address\n"
)


def test_the_addresses_are_read_from_the_kernels_own_table() -> None:
    seen: list[list[str]] = []

    def runner(argv: list[str]) -> str:
        seen.append(argv)
        return IP_ADDR

    assert box_capacity.ipv4_addresses(runner) == [
        "10.100.96.1", "10.100.97.1", "10.100.98.1", "192.168.1.180"]
    assert seen == [["ip", "-4", "-o", "addr", "show", "scope", "global"]]


def test_a_reading_that_cannot_be_taken_is_none_not_empty() -> None:
    """Absent means "not measured"; an empty list would mean "reads from nowhere"."""

    def broken(argv: list[str]) -> str:
        raise OSError("no ip")

    assert box_capacity.ipv4_addresses(broken) is None
    assert box_capacity.ipv4_addresses(lambda argv: "") == []


def test_the_offer_carries_the_addresses_only_when_they_were_read(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    queue.announce(host="sparky", tags=["gb10"], has_gpu=True,
                   addresses=["10.100.98.1", "192.168.1.180", "10.100.98.1"])
    record = json.loads((queue.root / pool.WORKERS / "sparky.json").read_text())
    assert record["addresses"] == ["10.100.98.1", "192.168.1.180"]

    queue.announce(host="older", tags=["gb10"], has_gpu=True)
    record = json.loads((queue.root / pool.WORKERS / "older.json").read_text())
    assert "addresses" not in record


def test_the_storage_role_follows_the_claim_to_the_offer_to_the_addresses(
        tmp_path: Path) -> None:
    """Every link is a record the fleet already writes, and a missing link says which."""

    fleet = Fleet(tmp_path)
    fleet.offer("sparky", addresses=["10.100.98.1", "192.168.1.180"],
                announced_unix=1000.0)
    fleet.offer("older", addresses=None)

    served = prewarm_loop.served_addresses(fleet.queue, "sparky")
    assert served["served_host"] == "sparky"
    assert served["served_addresses"] == ("10.100.98.1", "192.168.1.180")
    assert served["served_reason"] == "attributed"
    assert served["offer_age_s"] > 0.0

    older = prewarm_loop.served_addresses(fleet.queue, "older")
    assert older["served_host"] == "older"
    assert older["served_addresses"] == ()
    assert older["served_reason"] == "offer for older announces no addresses"
    assert isinstance(older["offer_age_s"], float)
    assert prewarm_loop.served_addresses(fleet.queue, "nobody")["served_reason"] == (
        "no offer for nobody")
    assert prewarm_loop.served_addresses(fleet.queue, "")["served_reason"] == (
        "claim names no host")
