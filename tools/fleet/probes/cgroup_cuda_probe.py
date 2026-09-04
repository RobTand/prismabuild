"""Does a cgroup MemoryMax charge a CUDA allocation on GB10 unified memory?

Three arms, one cap.  Each allocates the same number of bytes in the same
steps; the only difference is which allocator takes them.  The finding is read
from inside the child -- ``memory.current`` for what the cgroup charges, and
``MemAvailable`` for what the box lost -- so a survival is distinguishable from
a cap that saw nothing, rather than being inferred from a kill that did or did
not happen.
"""
import json
import os
import sys
import time


def cgroup_dir() -> str:
    with open("/proc/self/cgroup", encoding="utf-8") as fh:
        for line in fh:
            parts = line.strip().split(":", 2)
            if len(parts) == 3 and parts[0] == "0":
                return "/sys/fs/cgroup" + parts[2]
    return ""


def read_int(path: str) -> int:
    try:
        with open(path, encoding="utf-8") as fh:
            raw = fh.read().strip()
        return -1 if raw == "max" else int(raw)
    except OSError:
        return -1


def mem_available_mb() -> int:
    with open("/proc/meminfo", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    return -1


def main() -> int:
    arm = sys.argv[1]
    target_mb = int(sys.argv[2])
    step_mb = int(sys.argv[3])
    cg = cgroup_dir()
    facts = {
        "arm": arm,
        "cgroup": cg,
        "memory_max": read_int(cg + "/memory.max"),
        "memory_swap_max": read_int(cg + "/memory.swap.max"),
        "mem_available_mb_start": mem_available_mb(),
    }
    held = []
    torch = None
    if arm != "host":
        import torch as _torch
        torch = _torch
        facts["torch"] = _torch.__version__
        facts["cuda_available"] = _torch.cuda.is_available()
        facts["memory_current_after_import"] = read_int(cg + "/memory.current")
        if arm in ("cuda", "cuda_managed"):
            _torch.zeros(1, device="cuda").cpu()   # create the context first
            facts["memory_current_after_context"] = read_int(cg + "/memory.current")
    print(json.dumps(facts), flush=True)
    steps = []
    got_mb = 0
    while got_mb < target_mb:
        n = min(step_mb, target_mb - got_mb)
        if arm == "host":
            block = bytearray(n * 1024 * 1024)
            block[::4096] = b"\x01" * len(block[::4096])   # fault every page
        elif arm == "cuda":
            block = torch.empty(n * 1024 * 1024, dtype=torch.uint8, device="cuda")
            block.fill_(1)
            torch.cuda.synchronize()
        elif arm == "pinned":
            block = torch.empty(n * 1024 * 1024, dtype=torch.uint8,
                                pin_memory=True)
            block.fill_(1)
        else:
            raise SystemExit(f"unknown arm {arm!r}")
        held.append(block)
        got_mb += n
        steps.append({
            "held_mb": got_mb,
            "memory_current": read_int(cg + "/memory.current"),
            "memory_peak": read_int(cg + "/memory.peak"),
            "mem_available_mb": mem_available_mb(),
        })
        print(json.dumps(steps[-1]), flush=True)
        time.sleep(0.05)
    print(json.dumps({
        "arm": arm, "verdict": "SURVIVED", "held_mb": got_mb,
        "memory_current": read_int(cg + "/memory.current"),
        "memory_peak": read_int(cg + "/memory.peak"),
        "mem_available_mb_end": mem_available_mb(),
        "memory_events": open(cg + "/memory.events").read().split()
        if os.path.exists(cg + "/memory.events") else [],
    }), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
