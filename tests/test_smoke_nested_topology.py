"""Nested SLURM uses hardware topology, independently of OMP's nproc override."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def topology():
    spec = importlib.util.spec_from_file_location(
        "smoke_topology", ROOT / "fleet/slurm/smoke/topology.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_replaces_every_synthetic_node_and_preserves_routing(topology):
    text = """TaskPlugin=task/cgroup,task/affinity
NodeName=dl380g10 CPUs=1 SocketsPerBoard=2 CoresPerSocket=0 ThreadsPerCore=1 \\
    RealMemory=61440 Feature=x86,cpu Weight=1
NodeName=sparky CPUs=20 SocketsPerBoard=1 CoresPerSocket=20 ThreadsPerCore=1 Gres=gpu:1,shard:2
PartitionName=cpu Nodes=dl380g10
"""
    hardware = "NodeName=host CPUs=20 Boards=1 SocketsPerBoard=1 CoresPerSocket=20 ThreadsPerCore=1 RealMemory=124546\nUpTime=1"
    result = topology.configure(text, hardware)
    nodes = [line for line in result.splitlines() if line.startswith("NodeName=")]
    assert len(nodes) == 2
    for line in nodes:
        for value in ["CPUs=20", "Boards=1", "SocketsPerBoard=1", "CoresPerSocket=20", "ThreadsPerCore=1"]:
            assert value in line
        assert "CoresPerSocket=0" not in line
    assert "Feature=x86,cpu Weight=1" in result
    assert "PartitionName=cpu Nodes=dl380g10" in result
    assert "TaskPlugin=task/cgroup,task/affinity" in result
    assert "TaskPluginParam=SlurmdSpecOverride" in result


def test_preserves_existing_binding_option_and_is_idempotent(topology):
    hardware = "CPUs=80 Boards=1 SocketsPerBoard=2 CoresPerSocket=20 ThreadsPerCore=2"
    result = topology.configure("NodeName=a CPUs=1\nTaskPluginParam=Threads\n", hardware)
    assert "TaskPluginParam=Threads,SlurmdSpecOverride" in result
    assert topology.configure(result, hardware) == result


@pytest.mark.parametrize("hardware", ["CPUs=1", "CPUs=0 Boards=1 SocketsPerBoard=1 CoresPerSocket=1 ThreadsPerCore=1"])
def test_incomplete_hardware_fails_before_starting_daemons(topology, hardware):
    with pytest.raises(ValueError, match="complete positive CPU topology"):
        topology.configure("NodeName=a CPUs=1\n", hardware)
