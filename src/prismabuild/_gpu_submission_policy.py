"""Early placement-only GPU policy, independent of runtime device accounting."""
import argparse
from collections.abc import Mapping, Sequence
from pathlib import Path

from . import contracts, fleet_roster


def _eligible_gpu_classes(roster: fleet_roster.Roster, tags: Sequence[str]) -> set[str]:
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False, exit_on_error=False)
    parser.add_argument("--class", dest="klass", default="gb10")
    parser.add_argument("--tag", action="append", default=[])
    parser.add_argument("--gpu", action="store_true")
    parser.add_argument("--gpu-slots", type=int, default=0)
    code_tags = {contract.tag for contract in contracts.CONTRACTS}
    required = {tag for tag in tags if tag not in code_tags
                and not tag.startswith("runtime-generation:")}
    classes = set()
    for key, names, entry in roster.declarations():
        box = fleet_roster.Box(key, entry, roster)
        if fleet_roster.is_absent(key, entry) or box.loops == 0:
            continue
        try:
            worker, _ = parser.parse_known_args(box.loop_args)
        except argparse.ArgumentError as exc:
            raise ValueError(f"invalid GPU placement for roster box {key!r}: {exc}") from exc
        if (worker.gpu or worker.gpu_slots > 0) and required <= {
                *names, worker.klass, *worker.tag}:
            classes.add(worker.klass)
    return classes


def _require_gb10_memory_subset(
    *, demand: Mapping[str, object], gpu_memory_gb: object,
    tags: Sequence[str] = (), host_class: str | None = None,
    gpu: bool = False, exclusive: bool = False,
) -> None:
    """Compare explicit GB10 caps with pbrun's effective host-memory demand.

    Numeric strings are accepted by campaign manifests. Evaluate normalized
    numbers without changing their source mapping or the sealed common spec.
    Unresolved placement is deliberately not inferred from a hostname or device.
    Discrete VRAM is independent of the host reservation.
    """
    if gpu_memory_gb is None or not ("gb10" in tags or host_class == "gb10"):
        return
    normalized_demand = {name.strip(): value for name, value in demand.items()}
    mem_gb = int(normalized_demand.get("mem_gb", 16 if gpu else 4))
    if exclusive:
        mem_gb = max(mem_gb, 16)
    cap = float(gpu_memory_gb)
    if cap > mem_gb:
        raise ValueError(
            f"GB10 gpu_memory_gb={cap:g} exceeds mem_gb={mem_gb}: "
            "the GPU memory cap must be a subset of mem_gb "
            "(gpu_memory_gb <= mem_gb), not an additional reservation"
        )
