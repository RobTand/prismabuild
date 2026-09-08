"""A fixed-work GPU action, for accepting and pricing #372's Tier 2 modes.

Fixed *iterations*, never fixed wall-clock: an arm that stops after N seconds
does less work when it is slower, which is the one shape that cannot measure a
profiler's overhead.  The checksum is printed so two arms can be shown to have
done the same arithmetic.

Usage: ``gpu_profile_probe.py RESULT_PATH ITERATIONS [--helper]``.  With
``--helper`` it uses ``tools/profile_torch.py``, which is what an action opting
into ``--profile torch`` does; without it, it ignores the contract, which is
what the refusal path needs.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))


def _run(iterations: int) -> tuple[float, float]:
    import torch

    device = torch.device("cuda")
    size = 2048
    left = torch.randn(size, size, device=device, dtype=torch.bfloat16)
    right = torch.randn(size, size, device=device, dtype=torch.bfloat16)
    torch.cuda.synchronize()
    started = time.perf_counter()
    product = left
    for index in range(iterations):
        product = torch.mm(left, right)
        if index % 2000 == 0:
            torch.cuda.synchronize()
    torch.cuda.synchronize()
    return time.perf_counter() - started, float(product[0, 0])


def main(argv: list[str]) -> int:
    result_path = Path(argv[1])
    iterations = int(argv[2])
    use_helper = "--helper" in argv[3:]
    if use_helper:
        from profile_torch import prismabuild_torch_profile

        with prismabuild_torch_profile():
            elapsed, checksum = _run(iterations)
    else:
        elapsed, checksum = _run(iterations)
    body = {
        "iterations": iterations,
        "wall_s": round(elapsed, 6),
        "checksum": checksum,
        "helper": use_helper,
    }
    result_path.write_text(json.dumps(body, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(body, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
