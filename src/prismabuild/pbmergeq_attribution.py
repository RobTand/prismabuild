"""Pure failure attribution policy for the merge queue."""

from prismabuild import core


def environment_identity(producer: dict, interpreter: dict, runtime: dict,
                         config: dict, request: dict) -> str:
    """Compare observed environments without source/attempt-specific fields."""
    evidence = producer["evidence"]
    return core.canonical_sha256({
        "schema": "pbmergeq.observed_environment.v1",
        "worker": producer["worker_id"], "platform": producer["platform_key"],
        "host_class": producer["host_class"],
        "evidence": {k: evidence[k] for k in
                     ("hostname", "system", "machine", "libc", "accelerators")},
        "runtime": producer["runtime"], "executable": producer["executable"],
        "interpreter": interpreter, "toolchain": producer["toolchain"],
        "selection": {k: runtime[k] for k in ("python", "pins", "pin_sources", "pbtest")},
        "config": config,
        "environment": {key: value for key, value in request["environment"]["variables"].items()
                        if key not in {"PRISMABUILD_CONTAINER_OWNER", "PRISMABUILD_CONTAINER_MARKER"}},
        "demand": request["params"]["demand"],
    })


def unmatched_files(files, reference, observed, *, absent=()):
    return sorted(name for name in set(files) - set(absent)
                  if not reference.get(name, {}).get("identity")
                  or reference[name]["identity"] != observed.get(name, {}).get("identity"))


def failure_sets(original: set[str], baseline: set[str], rerun: set[str]):
    candidate_new = original - baseline
    return (sorted(candidate_new & rerun), sorted(original & baseline),
            sorted(candidate_new - rerun))
