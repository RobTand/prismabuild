"""The physical host DRAM bound shared by observation and admission."""
from __future__ import annotations

import math

from .gpu_sample_facts import nonnegative_float

# Keep the existing operating margin when bounding driver-held unified DRAM.
MEMORY_MARGIN_GB = 8


def physical_memory_budget_gib(host_total_bytes, *, margin_gb=MEMORY_MARGIN_GB) -> int | None:
    total = nonnegative_float(host_total_bytes, positive=True)
    if total is None:
        return None
    return max(0, math.floor(total / 1024**3) - margin_gb)
