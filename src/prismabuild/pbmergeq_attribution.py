"""Pure failure attribution policy for the merge queue."""

from prismabuild import core, pbtest_collection


def environment_identity(producer: dict, interpreter: dict, runtime: dict,
                         config: dict, request: dict) -> str:
    """Compare observed environments without source/attempt-specific fields."""
    evidence = producer["evidence"]
    command = request["params"]["command"]
    selection = pbtest_collection.decode_selection(command[3])
    return core.canonical_sha256({
        "schema": "pbmergeq.observed_environment.v2",
        "recorder": command[:3],
        "pytest_options": command[4:-len(selection["files"])],
        "capabilities": selection.get("capabilities"),
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


def file_environments(observations: dict, refused: set[str]) -> tuple[dict, list[str]]:
    """Retain all immutable groups only when the file has one environment."""
    environments, errors = {}, []
    for name, parts in observations.items():
        if name in refused:
            continue
        first = parts[0]
        if len(parts) > 1:
            groups = [part['selection'].get('node_groups', {}).get(name) for part in parts]
            if (any(group is None for group in groups)
                    or len({group['index'] for group in groups}) != len(parts)
                    or any(part['identity'] != first['identity'] for part in parts)
                    or any({key: value for key, value in group.items() if key != 'index'}
                           != {key: value for key, value in groups[0].items() if key != 'index'}
                           for group in groups)):
                errors.append(f'{name}: file has incompatible or duplicate environment observations')
                continue
        environments[name] = first if len(parts) == 1 else {**first, 'parts': parts}
    return environments, errors
