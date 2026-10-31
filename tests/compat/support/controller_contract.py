"""Controller family ownership for file-level PB sharding."""


def controller_partition(kind):
    name = kind.name
    if name.startswith("input-d38-"):
        return "preflight"
    if name.startswith(("input-", "shape-", "gpu-capacity")):
        return "inputs"
    if name.startswith("slurm-"):
        return "slurm"
    if name == "q-outcome":
        return "outcomes"
    if name.startswith("state-reclaim-"):
        return "reclaim"
    if name.startswith("state-"):
        return "state"
    if name.startswith("floor-"):
        return "floor"
    if name.startswith("broker-"):
        return "broker"
    if name == "upgrade-rollout":
        return "rollout"
    if name == "upgrade-marker":
        return "markers"
    if name.startswith("upgrade-"):
        return "upgrade"
    if name in ("pg-ticket", "pg-turn", "pg-reserved", "pg-done"):
        return "prelaunch_turns"
    if name in ("pg-release", "pg-decline"):
        return "prelaunch_release"
    if name.startswith("pg-"):
        return "prelaunch"
    if name.startswith("edge-"):
        return "edges"
    if name.startswith("gang-"):
        return "gang"
    if name.startswith("config-"):
        return "config"
    return "runtime"
