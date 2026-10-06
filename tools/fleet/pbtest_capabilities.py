"""Per-file capability declarations and cohorting for pbtest populations.

The requirement this serves (#1495): a mixed population -- portable files and
files whose secondary dependencies live on one hardware class -- must fan out
without pinning the portable files or stranding the fenced ones, and the
declaration must be PB's, versioned, fail-closed and per-file.

Three pieces live here and nowhere else:

* :func:`load_config` / :func:`parse_config` -- the versioned configuration
  (``prismabuild.pbtest_capabilities.v1``) that names each capability once:
  the placement tags it requires of a claiming worker and the exact
  dependency requirements (:mod:`prismabuild.dependency_digest` owns those
  rules).  A capability that constrains nothing refuses; so does a field
  nobody defined.
* :func:`classify_files` -- the static, per-file classification through the
  same seam ``fleet_data`` uses: a ``@pytest.mark.pbtest_capability("name")``
  marker, module-level or per-test.  Never an import, never a host map.  A
  marker that cannot be read statically, or a name the config does not
  define, refuses the run naming the file.
* :func:`cohorts` / :func:`shard_selection` -- the deterministic cohort rule:
  files with the same declared name-set pack together, a portable file is
  never packed into a fenced shard (a union would pin it to hosts it does
  not need), and a shard seals its cohort's resolved union -- names, tags,
  dependency requirements -- into its own selection, which is the action's
  identity.

The coordinator uses only the standard library plus the repository's own
``prismabuild`` modules; the worker-side verification is
:mod:`prismabuild.dependency_digest`, embedded into the shard program.
"""
from __future__ import annotations

import ast
from pathlib import Path
import re

from prismabuild import dependency_digest

#: The configuration schema this module parses.  Anything else refuses by
#: name, so a consumer cannot ship a file a newer reader would silently
#: misread.
SCHEMA = "prismabuild.pbtest_capabilities.v1"

#: The pytest marker a test file carries when it requires a capability.
MARKER = "pbtest_capability"

_CONFIG_USE = re.compile(rf"\bmark\.{MARKER}\b")
_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


def parse_config(data: object) -> dict:
    """The validated capability configuration.

    ``{"schema": SCHEMA, "capabilities": {name: {"tags": [...],
    "dependencies": [...]}}}``.  A capability must constrain something --
    tags, dependencies, or both; a fence that fences nothing is a typo that
    would run fenced work anywhere.  Dependency entries are validated by the
    one owner (:mod:`prismabuild.dependency_digest`), so a config the
    coordinator accepts is a config the claim gate and the shard preflight
    read the same way.
    """

    if not isinstance(data, dict):
        raise ValueError("capabilities config must be a JSON object")
    if data.get("schema") != SCHEMA:
        raise ValueError(
            f"capabilities config schema must be exactly {SCHEMA!r}, "
            f"got {data.get('schema')!r}")
    unknown = sorted(set(data) - {"schema", "capabilities"})
    if unknown:
        raise ValueError(f"capabilities config has unknown fields: {unknown}")
    capabilities = data.get("capabilities")
    if not isinstance(capabilities, dict):
        raise ValueError("capabilities config needs a 'capabilities' object")
    parsed: dict[str, dict] = {}
    for name, body in capabilities.items():
        if not isinstance(name, str) or not _NAME.match(name):
            raise ValueError(
                f"capability name {name!r} must match "
                f"{_NAME.pattern!r}")
        if not isinstance(body, dict):
            raise ValueError(f"capability {name!r} must be an object")
        unknown = sorted(set(body) - {"tags", "dependencies"})
        if unknown:
            raise ValueError(
                f"capability {name!r} has unknown fields: {unknown}")
        tags = body.get("tags")
        if tags is not None:
            if (not isinstance(tags, list) or not tags
                    or any(not isinstance(t, str) or not t or "\x00" in t
                           or t.startswith("-") for t in tags)):
                raise ValueError(
                    f"capability {name!r} tags must be a nonempty list of "
                    "explicit tag names")
        dependencies = body.get("dependencies")
        if tags is None and dependencies is None:
            raise ValueError(
                f"capability {name!r} constrains nothing: declare tags, "
                "dependencies, or both")
        files: list[dict] = []
        observations: list[dict] = []
        if dependencies is not None:
            if not isinstance(dependencies, list) or not dependencies:
                raise ValueError(
                    f"capability {name!r} dependencies must be a nonempty "
                    "list of entries")
            files = dependency_digest.validate_requirements(
                [entry for entry in dependencies
                 if isinstance(entry, dict) and "kind" not in entry])
            observations = dependency_digest.validate_observations(
                [entry for entry in dependencies
                 if isinstance(entry, dict) and "kind" in entry],
                files=files)
        parsed[name] = {"tags": sorted(set(tags or ())),
                        "files": files, "observations": observations}
    return parsed


def load_config(path: Path) -> dict:
    """Read and parse one capability configuration file."""

    import json
    return parse_config(json.loads(Path(path).read_text(encoding="utf-8")))


def _marker_names(text: str, filename: str) -> tuple[list[str] | None,
                                                     str | None]:
    """The capability names one file's markers declare, or why it cannot.

    The ``fleet_data`` scan's rules, one seam over: strings and comments are
    not declarations; a marker whose arguments are not constant names is a
    declaration this static read cannot classify, and an unclassifiable
    declaration refuses the run rather than running unfenced.
    """

    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        if _CONFIG_USE.search(text):
            return None, (
                f"{filename}: uses mark.{MARKER} but its syntax cannot be "
                "statically classified; declare constant capability names")
        return [], None
    names: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == MARKER
                and isinstance(func.value, ast.Attribute)
                and func.value.attr == "mark"):
            continue
        if node.keywords:
            return None, (
                f"{filename}: a {MARKER} marker takes keyword arguments, "
                "which cannot be classified statically")
        for arg in node.args:
            if (isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                    and arg.value):
                names.append(arg.value)
            else:
                return None, (
                    f"{filename}: a {MARKER} marker argument is not a "
                    "constant capability name, so the file cannot be "
                    "classified statically")
    return sorted(set(names)), None


def classify_files(checkout: Path, files: list[str],
                   config: dict) -> tuple[dict[str, list[str]], list[str]]:
    """Which files declare which capabilities, and every named refusal.

    A declared name the config does not define is a refusal, not an
    unfenced file: silently running an x86-only file anywhere is exactly
    the outcome this contract exists to make unreachable.
    """

    declared: dict[str, list[str]] = {}
    problems: list[str] = []
    for name in files:
        try:
            text = (checkout / name).read_text(encoding="utf-8",
                                               errors="replace")
        except OSError:
            continue
        names, problem = _marker_names(text, name)
        if problem is not None:
            problems.append(problem)
            continue
        unknown = sorted(set(names) - set(config))
        if unknown:
            problems.append(
                f"{name}: declares capability {unknown[0]!r}, which the "
                "capabilities config does not define")
            continue
        if names:
            declared[name] = names
    return declared, problems


def cohorts(declared: dict[str, list[str]],
            files: list[str]) -> list[tuple[tuple[str, ...], list[str]]]:
    """The deterministic per-file cohorts, portable cohort included.

    The cohort key is a file's exact declared name-set, so a portable file
    can never share a shard with a fenced one -- a shard requires the union
    of its files' capabilities, and unioning a portable file with a fenced
    one would pin the portable file to hosts it does not need.  Order is
    the cohort key sort, then discovery order within a cohort, so packing
    is reproducible from the same inputs.
    """

    groups: dict[tuple[str, ...], list[str]] = {}
    for name in files:
        groups.setdefault(tuple(declared.get(name, ())), []).append(name)
    return sorted(groups.items())


def shard_selection(names: tuple[str, ...], config: dict) -> dict:
    """The sealed per-shard capability object: the cohort's resolved union.

    Tags are the union of the cohort's capabilities; dependencies are the
    merged requirement entries, deduplicated across capabilities by the one
    validator -- which refuses the same path pinned to two digests, so a
    merge can never quietly pick a side.  A cohort whose capabilities fence
    by tags alone merges to an empty requirement union (#1495); only the
    standalone ``--requires-files`` must be nonempty.  The result rides in
    the shard's selection JSON, which is part of the action's identity: a
    changed declaration re-keys every shard it fences.
    """

    tags: set[str] = set()
    files: list[dict] = []
    observations: list[dict] = []
    for name in sorted(names):
        body = config[name]
        tags.update(body["tags"])
        files.extend(body["files"])
        observations.extend(body["observations"])
    merged_files = (dependency_digest.validate_requirements(files)
                    if files else [])
    merged_observations = dependency_digest.validate_observations(
        observations, files=merged_files)
    return {"names": sorted(names), "tags": sorted(tags),
            "dependencies": [*merged_files, *merged_observations]}
