"""Foreign GPU memory must survive held add-back and offer smoothing."""
from prismabuild import box_capacity
from test_box_capacity import gpu_sample

GIB = 1 << 30
CAPACITY = {"cpu": 8, "gpu": 1, "mem_gb": 104}
NOW = 100.0


def sample(foreign_bytes):
    value = gpu_sample(foreign=[{"pid": 7, "gpu_uuid": "GPU-0", "used_bytes": foreign_bytes}])
    value.update(sampled_unix=NOW, host_total_bytes=128 * GIB,
                 host_available_bytes=120 * GIB)
    return value


def test_foreign_memory_bounds_unresident_held_addback():
    seen = box_capacity.observe(CAPACITY, {"mem_gb": 80},
                                gpu_sample=sample(32 * GIB), mem_gb=120, load1=0, now=NOW)
    assert seen.capacity["mem_gb"] <= 88


def test_fresh_foreign_growth_bounds_an_older_larger_offer():
    observer = box_capacity.CapacityObserver(samples=3)
    observer.offer(CAPACITY, {}, gpu_sample=sample(0), mem_gb=120, load1=0, now=NOW)
    offered = observer.offer(CAPACITY, {"mem_gb": 80},
                             gpu_sample=sample(32 * GIB), mem_gb=120, load1=0, now=NOW)
    assert offered["mem_gb"] <= 88


def test_unknown_foreign_residency_immediately_closes_memory_offer():
    observer = box_capacity.CapacityObserver(samples=3)
    observer.offer(CAPACITY, {}, gpu_sample=sample(0), mem_gb=120, load1=0, now=NOW)
    offered = observer.offer(CAPACITY, {"mem_gb": 80},
                             gpu_sample=sample(None), mem_gb=120, load1=0, now=NOW)
    assert offered["mem_gb"] == 0
