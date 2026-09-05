"""The two real box shapes, plus the cases that would silently return nonsense.

Written against a synthetic sysfs rather than the host, so the GB10 and Xeon
answers stay checkable from any machine -- including the one shape neither box
has (heterogeneous *and* SMT), which no amount of running it here would cover.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import cpu_topology as topo  # noqa: E402


def _sysfs(tmp_path, capacities, siblings=None):
    """Build a fake /sys/devices/system/cpu.  ``capacities`` maps cpu -> value."""
    root = tmp_path / "cpu"
    for cpu, cap in capacities.items():
        d = root / f"cpu{cpu}" / "topology"
        d.mkdir(parents=True)
        if cap is not None:
            (root / f"cpu{cpu}" / "cpu_capacity").write_text(f"{cap}\n")
        if siblings and cpu in siblings:
            (d / "thread_siblings_list").write_text(siblings[cpu] + "\n")
    return root


def test_gb10_shape_splits_five_and_five_not_a_contiguous_half(tmp_path):
    # The real numbers: X925 cores do not all report the same capacity, so an
    # equality-with-max test would return only cpu19.
    caps = {}
    for cpu in range(20):
        caps[cpu] = {0: 718, 1: 731}[cpu // 10] if cpu % 10 < 5 else (
            1024 if cpu == 19 else (1017 if cpu >= 15 else 997))
    root = _sysfs(tmp_path, caps)
    preferred, fallback = topo.classify(root)
    assert topo.as_range(preferred) == "5-9,15-19"
    assert topo.as_range(fallback) == "0-4,10-14"
    # The pin a person would reach for is half wrong, which is the whole point.
    assert set(range(10)) & set(preferred) and set(range(10)) & set(fallback)


def test_xeon_shape_prefers_physical_cores_over_their_smt_siblings(tmp_path):
    caps = {cpu: 1024 for cpu in range(80)}
    sibs = {cpu: f"{cpu % 40},{cpu % 40 + 40}" for cpu in range(80)}
    root = _sysfs(tmp_path, caps, sibs)
    preferred, fallback = topo.classify(root)
    assert topo.as_range(preferred) == "0-39"
    assert topo.as_range(fallback) == "40-79"


def test_uniform_box_with_no_smt_prefers_everything(tmp_path):
    root = _sysfs(tmp_path, {cpu: 1024 for cpu in range(4)})
    preferred, fallback = topo.classify(root)
    assert preferred == [0, 1, 2, 3] and fallback == []


def test_missing_capacity_file_is_treated_as_uniform_not_as_slow(tmp_path):
    # A kernel without cpu_capacity must not classify every core as fallback.
    root = _sysfs(tmp_path, {cpu: None for cpu in range(4)})
    assert topo.preferred_cpus(root) == [0, 1, 2, 3]


def test_heterogeneous_and_smt_together_demotes_both_axes(tmp_path):
    # Neither box is this shape; the rule still has to be right for it.
    caps = {0: 1024, 1: 1024, 2: 512, 3: 512}
    sibs = {0: "0,1", 1: "0,1", 2: "2,3", 3: "2,3"}
    root = _sysfs(tmp_path, caps, sibs)
    preferred, fallback = topo.classify(root)
    assert preferred == [0]                 # fast and leads its SMT pair
    assert fallback == [1, 2, 3]            # fast sibling first, then the slow pair
    assert fallback[0] == 1


def test_preferred_is_never_empty(tmp_path):
    # Every CPU demoted (all are SMT siblings of something) must still yield a
    # usable set rather than an empty affinity mask, which would raise on pin.
    root = _sysfs(tmp_path, {0: 1024, 1: 1024}, {0: "0,1", 1: "0,1"})
    assert topo.preferred_cpus(root) == [0]
    empty = tmp_path / "nothing"
    empty.mkdir()
    assert topo.preferred_cpus(empty) == []


def test_as_range_round_trips_the_shapes_taskset_prints():
    assert topo.as_range([5, 6, 7, 8, 9, 15, 16, 17, 18, 19]) == "5-9,15-19"
    assert topo.as_range([3]) == "3"
    assert topo.as_range([1, 3, 5]) == "1,3,5"
    assert topo.as_range([]) == ""


def test_pin_never_widens_an_outer_restriction(tmp_path, monkeypatch):
    # A container cpuset or an outer taskset is an explicit decision; this
    # module narrows within it and never escapes it.
    root = _sysfs(tmp_path, {cpu: (1024 if cpu >= 2 else 512) for cpu in range(4)})
    monkeypatch.setattr(topo.os, "sched_getaffinity", lambda _pid: {0, 1})
    applied = []
    monkeypatch.setattr(topo.os, "sched_setaffinity",
                        lambda _pid, mask: applied.append(set(mask)))
    # Preferred is {2,3}; intersected with the outer mask that is empty, so the
    # outer mask stands and nothing is set.
    assert topo.pin_to_preferred(root) == [0, 1]
    assert applied == []


def test_offline_leader_does_not_demote_its_online_sibling(tmp_path):
    root = _sysfs(tmp_path, {0: 1024, 1: 1024}, {0: '0-1', 1: '0-1'})
    (root / 'online').write_text('1\n')
    assert topo.classify(root) == ([1], [])


def test_allowed_sibling_becomes_primary_without_promoting_slow_cores(tmp_path):
    root = _sysfs(tmp_path, {0: 1024, 1: 1024, 2: 512}, {0: '0-1', 1: '0-1'})
    assert topo.classify(root, allowed={1, 2}) == ([1], [2])
    assert topo.classify(root, allowed={2}) == ([], [2])


def test_x86_hybrid_pmu_without_capacity(tmp_path):
    root = _sysfs(tmp_path, {0: None, 1: None, 2: None}, {0: '0-1', 1: '0-1'})
    pmu = tmp_path / 'devices'
    (pmu / 'cpu_atom').mkdir(parents=True)
    (pmu / 'cpu_atom' / 'cpus').write_text('2\n')
    assert topo.classify(root, pmu_root=pmu) == ([0], [1, 2])


def test_inherited_mask_does_not_readd_known_offline_cpu(tmp_path, monkeypatch):
    root = _sysfs(tmp_path, {0: 1024, 1: 1024}, {0: '0-1', 1: '0-1'})
    (root / 'online').write_text('1\n')
    monkeypatch.setattr(topo.os, 'sched_getaffinity', lambda _: {0, 1, 9})
    assert topo.inherited_tiers(root) == {'preferred': [1], 'fallback': []}


def test_capacity_fluctuations_within_classes_do_not_reorder_tokens(tmp_path):
    root = _sysfs(tmp_path, {0: 500, 1: 510, 2: 1000, 3: 1024})
    before = topo.classify(root)
    (root / 'cpu0' / 'cpu_capacity').write_text('515')
    (root / 'cpu2' / 'cpu_capacity').write_text('1024')
    assert topo.classify(root) == before == ([2, 3], [0, 1])
