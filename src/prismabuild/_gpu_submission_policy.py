"""Early placement-only GPU policy, independent of runtime device accounting."""
from collections.abc import Mapping, Sequence


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
