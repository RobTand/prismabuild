"""Digest-pinned secondary dependencies: one owner for the rule and the probe.

A pbtest population can declare that a file requires bytes beyond the primary
interpreter -- PrismaQuant's immutable x86 projection producer is the case that
commissioned this (#1495).  The requirement is versioned configuration in the
consumer checkout; this module is the one place that says what a requirement
*is*, who may claim a row that carries one, and how the bytes are observed:

* ``DEPENDENCY_DIGEST_TAG`` rides every published requirement (the #714
  shape), so a worker loop from before this contract never offers the tag and
  never claims a row whose requirements it cannot even see.
* :func:`validate_requirements` is the one validator for exact-file entries --
  ``{"path", "sha256"}`` -- shared by ``pool.publish`` (the row projection),
  ``pbrun`` (the flag), ``pbtest`` (the config) and the shard preflight.
* :func:`digest_file` is the one byte observation, used at claim time and
  again before pytest, so the claim gate and the preflight cannot disagree.
* :func:`observe_distribution` runs a stdlib-only probe *through the pinned
  interpreter* to observe an installed distribution's imported module and
  compound payload with the consumer fixture's exact composition -- one
  sha256 over every included ``dist.files`` entry's ``name + NUL + bytes``,
  sorted by name, with the imported origin and every located file asserted
  under the interpreter's own ``sys.prefix``.

The module uses only the target interpreter's standard library: it is embedded
into pbtest's sealed shard program the way ``resource_scope`` is, so a shard's
verification never depends on what the target environment happens to have
installed.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

#: The capability a worker must offer before it may claim a row carrying
#: dependency requirements.  A loop from before this contract offers neither
#: the tag nor the claim check, so requiring the tag is what makes "claimed by
#: a worker that ignores the digest" unreachable rather than unlikely.
DEPENDENCY_DIGEST_TAG = "dependency-digest-v1"

#: The one prefix of the evidence line a verifying shard prints.
EVIDENCE_PREFIX = "pbtest dependency digest: "

_HEX64 = frozenset("0123456789abcdef")
_READ_CHUNK = 1 << 20


def _is_hex64(value: object) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and set(value) <= _HEX64)


def _is_absolute_path(value: object) -> bool:
    return (isinstance(value, str) and value.startswith("/")
            and value != "/" and "\x00" not in value)


def validate_requirements(entries: object) -> list[dict]:
    """The exact-file requirement entries, normalized and conflict-checked.

    Each entry is exactly ``{"path", "sha256"}``: an absolute worker-visible
    path and the sha256 of its bytes.  The same path twice with one digest is
    stated once; the same path with two digests is a contradiction, and a
    contradiction is refused by name rather than resolved by order.
    """

    if not isinstance(entries, list) or not entries:
        raise ValueError("requirements must be a nonempty list of entries")
    by_path: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            raise ValueError(
                f"requirement entry must be exactly {{path, sha256}}, "
                f"got {sorted(entry) if isinstance(entry, dict) else entry!r}")
        path, sha256 = entry["path"], entry["sha256"]
        if not _is_absolute_path(path):
            raise ValueError(
                f"requirement path must be absolute and nonempty, "
                f"got {path!r}")
        if not _is_hex64(sha256):
            raise ValueError(
                f"requirement sha256 for {path!r} must be 64 lowercase hex, "
                f"got {sha256!r}")
        seen = by_path.get(path)
        if seen is not None and seen != sha256:
            raise ValueError(
                f"requirement for {path!r} states two digests: {seen} "
                f"and {sha256}; a path is one set of bytes")
        by_path[path] = sha256
    return [{"path": path, "sha256": by_path[path]}
            for path in sorted(by_path)]


#: The exact key set of an installed-distribution observation.  The consumer
#: fixture's own algorithm, parameterized: nothing here names a project.
_DISTRIBUTION_KEYS = frozenset({
    "kind", "interpreter_path", "module", "module_sha256", "distribution",
    "include_prefixes", "include_suffixes", "sha256"})


def validate_observations(entries: object, *, files: list[dict]) -> list[dict]:
    """The installed-distribution entries, pinned to the file requirements.

    ``kind`` is ``"installed_distribution"`` and every other field names the
    observation: which interpreter to import through (``interpreter_path`` --
    which must also be an exact-file requirement above, so the probe only ever
    executes bytes the same requirement pins), which module to import, the
    distribution whose ``dist.files`` form the payload, the include filters,
    and the two expected digests.  A tree hash is not representable here and
    is not accepted: the payload is defined by this algorithm or not at all.
    """

    if not isinstance(entries, list):
        raise ValueError("observations must be a list when present")
    pinned = {entry["path"]: entry["sha256"] for entry in files}
    observed: list[dict] = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != _DISTRIBUTION_KEYS:
            raise ValueError(
                "installed_distribution entry must be exactly "
                f"{sorted(_DISTRIBUTION_KEYS)}, "
                f"got {sorted(entry) if isinstance(entry, dict) else entry!r}")
        if entry["kind"] != "installed_distribution":
            raise ValueError(
                f"unknown dependency kind {entry['kind']!r}; the only "
                "observation is an installed_distribution")
        module = entry["module"]
        if (not isinstance(module, str) or not module or "\x00" in module):
            raise ValueError(f"module must be a nonempty name, got {module!r}")
        distribution = entry["distribution"]
        if (not isinstance(distribution, str) or not distribution
                or "\x00" in distribution):
            raise ValueError(
                f"distribution must be a nonempty name, "
                f"got {distribution!r}")
        for field in ("include_prefixes", "include_suffixes"):
            values = entry[field]
            if (not isinstance(values, list) or not values
                    or any(not isinstance(v, str) or not v or "\x00" in v
                           for v in values)):
                raise ValueError(
                    f"{field} must be a nonempty list of nonempty strings, "
                    f"got {values!r}")
        for suffix in entry["include_suffixes"]:
            if not suffix.startswith(".") or len(suffix) < 2:
                raise ValueError(
                    f"include_suffixes entries are filename suffixes like "
                    f"'.py', got {suffix!r}")
        if not _is_hex64(entry["sha256"]):
            raise ValueError(
                f"payload sha256 for {module!r} must be 64 lowercase hex")
        if not _is_hex64(entry["module_sha256"]):
            raise ValueError(
                f"module_sha256 for {module!r} must be 64 lowercase hex; the "
                "imported module's actual bytes are compared to it")
        interpreter = entry["interpreter_path"]
        if interpreter not in pinned:
            raise ValueError(
                f"installed_distribution for {module!r} imports through "
                f"{interpreter!r}, which no exact-file requirement pins; add "
                "{{path, sha256}} for it so the probe only ever executes "
                "pinned bytes")
        observed.append(entry)
    return observed


def validate_sealed(sealed: object) -> dict:
    """The sealed per-shard capability selection a shard is asked to verify.

    ``{"names", "tags", "dependencies"}`` -- which capabilities, which
    placement tags they resolved to, and their requirements.  Anything else is
    a malformed seal and refuses the shard before pytest rather than running
    with a fence nobody can read.
    """

    if not isinstance(sealed, dict) or set(sealed) != {
            "names", "tags", "dependencies"}:
        raise ValueError(
            "sealed capabilities must be exactly "
            "{names, tags, dependencies}")
    names, tags, dependencies = (sealed["names"], sealed["tags"],
                                 sealed["dependencies"])
    if (not isinstance(names, list) or not names
            or any(not isinstance(n, str) or not n for n in names)):
        raise ValueError("sealed capability names must be a nonempty list")
    if (not isinstance(tags, list)
            or any(not isinstance(t, str) or not t or "\x00" in t
                   for t in tags)):
        raise ValueError("sealed capability tags must be a list of names")
    files = validate_requirements(dependencies)
    observations = validate_observations(
        [entry for entry in dependencies
         if isinstance(entry, dict) and "kind" in entry],
        files=files)
    return {"names": list(names), "tags": list(tags),
            "files": files, "observations": observations}


def digest_file(path: str) -> str:
    """The sha256 of one file's bytes, read in bounded chunks."""

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(_READ_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def presence(paths: list[str]) -> tuple[list[str], list[str]]:
    """Which requirement paths this box can positively stat, and which not."""

    present, absent = [], []
    for path in paths:
        (present if os.path.isfile(path) else absent).append(path)
    return present, absent


def claim_digest(path: str) -> str | None:
    """The bytes a claim gate compares, or ``None`` when unreadable."""

    try:
        return digest_file(path)
    except OSError:
        return None


#: The probe that runs *inside* the pinned interpreter, with its inherited
#: environment -- the same reading the consumer fixture probes.  Every
#: parameter comes in as JSON on argv; nothing about any project is named
#: here.  The composition is the consumer's: one sha256 over the included
#: ``dist.files`` entries in sorted order, each feeding
#: ``name.encode() + NUL + file bytes``, with the imported module's origin and
#: every located file asserted under the interpreter's own prefix.
PROBE_PROGRAM = r"""import hashlib
import importlib
import importlib.metadata as md
import json
import sys
from pathlib import Path

spec = json.loads(sys.argv[1])
module = importlib.import_module(spec["module"])
prefix = Path(sys.prefix).resolve()
origin = Path(module.__file__).resolve()
assert origin.is_relative_to(prefix), (
    "imported module origin is outside the interpreter prefix: "
    + str(origin))
dist = md.distribution(spec["distribution"])
digest = hashlib.sha256()
included = 0
for file in sorted(dist.files, key=str):
    name = str(file)
    if (name.startswith(tuple(spec["include_prefixes"]))
            and Path(name).suffix in set(spec["include_suffixes"])):
        path = Path(dist.locate_file(file)).resolve()
        assert path.is_relative_to(prefix), (
            "included file resolves outside the interpreter prefix: "
            + str(path))
        digest.update(name.encode() + b"\0" + path.read_bytes())
        included += 1
print(json.dumps({
    "executable_sha256": hashlib.sha256(
        Path(sys.executable).read_bytes()).hexdigest(),
    "module_path": str(origin),
    "module_sha256": hashlib.sha256(origin.read_bytes()).hexdigest(),
    "package_payload_sha256": digest.hexdigest(),
    "included_files": included,
}))
"""


def observe_distribution(entry: dict) -> dict:
    """Import through the pinned interpreter and read the actual bytes.

    The probe's own failures -- import error, a shadowing module outside the
    prefix, a distribution that does not exist -- arrive as a non-zero exit
    and are refused with its stderr tail, never guessed around.
    """

    spec = json.dumps({
        "module": entry["module"],
        "distribution": entry["distribution"],
        "include_prefixes": entry["include_prefixes"],
        "include_suffixes": entry["include_suffixes"],
    })
    completed = subprocess.run(
        [entry["interpreter_path"], "-c", PROBE_PROGRAM, spec],
        capture_output=True, text=True)
    if completed.returncode != 0:
        tail = (completed.stderr or completed.stdout or "").strip()
        raise ValueError(
            f"installed_distribution observation of {entry['module']!r} "
            f"through {entry['interpreter_path']!r} failed with exit "
            f"{completed.returncode}: {tail[-800:]}")
    for line in reversed((completed.stdout or "").strip().splitlines()):
        if line.strip():
            try:
                return json.loads(line)
            except ValueError:
                break
    raise ValueError(
        f"installed_distribution observation of {entry['module']!r} printed "
        f"no readable record: {(completed.stdout or '')[-400:]!r}")


def observe_sealed(spec: dict) -> dict:
    """Verify every sealed requirement; return the evidence, or raise.

    Exact files are digested and compared bytewise.  Each installed
    distribution is observed through its pinned interpreter, and the probe's
    *actual* imported-module digest is compared to the descriptor's
    ``module_sha256`` -- a correct-looking module file somewhere else on the
    import path is refused on the digest even when a prefix check would pass,
    which is the shadowed-module case the consumer fixture guards.
    """

    by_path = {entry["path"]: entry for entry in spec["files"]}
    evidence_files = []
    for entry in spec["files"]:
        path = entry["path"]
        if not os.path.isfile(path):
            raise ValueError(
                f"required dependency is missing: {path} "
                f"(capability: {', '.join(spec['names'])})")
        observed = digest_file(path)
        if observed != entry["sha256"]:
            raise ValueError(
                f"required dependency drifted: {path} expected "
                f"{entry['sha256']} observed {observed} "
                f"(capability: {', '.join(spec['names'])})")
        evidence_files.append({"path": path, "sha256": observed})
    evidence_distributions = []
    for entry in spec["observations"]:
        observed = observe_distribution(entry)
        if observed.get("executable_sha256") != by_path[
                entry["interpreter_path"]]["sha256"]:
            raise ValueError(
                f"observed interpreter bytes at "
                f"{entry['interpreter_path']} are not the pinned requirement "
                f"(capability: {', '.join(spec['names'])})")
        if observed.get("module_sha256") != entry["module_sha256"]:
            raise ValueError(
                f"imported {entry['module']} from "
                f"{observed.get('module_path')!r} has digest "
                f"{observed.get('module_sha256')}, expected "
                f"{entry['module_sha256']}; a shadowing module or drifted "
                "install refuses rather than runs "
                f"(capability: {', '.join(spec['names'])})")
        if observed.get("package_payload_sha256") != entry["sha256"]:
            raise ValueError(
                f"installed distribution {entry['distribution']!r} payload "
                f"digest {observed.get('package_payload_sha256')}, expected "
                f"{entry['sha256']} over {observed.get('included_files')} "
                f"included files (capability: {', '.join(spec['names'])})")
        evidence_distributions.append({
            "module": entry["module"],
            "distribution": entry["distribution"],
            "module_path": observed.get("module_path"),
            "included_files": observed.get("included_files"),
        })
    return {"names": spec["names"], "files": evidence_files,
            "distributions": evidence_distributions}


def verify_sealed(selection: object) -> int:
    """The shard preflight: verify a selection's capabilities, ``1`` refuses.

    ``0`` when the selection carries no capabilities -- every existing shard
    verifies nothing and prints nothing.  Evidence prints on the one line
    :data:`EVIDENCE_PREFIX` marks; a refusal names the capability, the path
    and both digests on stderr, and the shard ends before pytest, which makes
    it a failed shard and a red run rather than a skipped test.
    """

    if not isinstance(selection, dict):
        return 0
    if selection.get("capabilities") is None:
        return 0
    try:
        spec = validate_sealed(selection["capabilities"])
        evidence = observe_sealed(spec)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        sys.stderr.write(f"pbtest: capability refusal: {exc}\n")
        return 1
    print(EVIDENCE_PREFIX + json.dumps(evidence, sort_keys=True), flush=True)
    return 0
