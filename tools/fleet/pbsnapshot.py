"""Say which files in a materialized pbrun checkout PrismaBuild generated.

``pbrun`` runs an action in a fresh checkout of a snapshot commit.  That commit
is the submitter's tree plus one file PrismaBuild wrote: the closure stamp,
``.pbrun-closure.<fingerprint>.json``.  A client that hashes its own source to
name the tree a run tested must leave that file out, or two runs of the same
source on two boxes hash differently.  It must also never leave out a file
because of its name alone, because a submitter can commit a file with that
name.

This tool is the check.  Given a checkout and its HEAD commit, it verifies the
stamp against the exact sealed action that materialized it, and prints what it
verified:

    pbsnapshot.py verify <checkout> <commit>

On success it exits 0 and prints one JSON object on stdout::

    {"schema": "prismabuild.checkout_snapshot.v1",
     "snapshot": true,
     "generated": [{"path": ..., "bytes": ..., "sha256": ...,
                    "action_key": ..., "request_sha256": ...}]}

``snapshot`` is false and ``generated`` is empty when the commit is not a pbrun
snapshot: PrismaBuild generated nothing in that tree.  A commit that says it
is a snapshot but does not verify exits 1 and names the reason on stderr.  A
caller must treat exit 1 as "unknown", never as "nothing generated".

Every check reads PrismaBuild's own records: the snapshot commit subject, the
sealed action request in the CAS, its code closure, the owner variable, and the
stamp name ``pbrun`` derives from the action.  The stamp name is recomputed
with ``pbrun.result_and_stamp_names``, the function that named it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))

import pbrun  # noqa: E402  (puts the generation's src/ on sys.path)
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

SCHEMA = "prismabuild.checkout_snapshot.v1"
#: The subject ``pbrun`` gives a snapshot commit, before the version suffix.
SUBJECT_PREFIX = b"PrismaBuild pbrun checkout snapshot "
#: Snapshot versions, by the suffix ``pbrun`` writes in the commit subject.
SNAPSHOT_SCHEMAS = {
    b"v1": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
    b"v2": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2,
}
#: Where ``pbrun`` publishes sealed action requests.
DEFAULT_CAS_ROOT = pbrun.SH / "cas"
_STAMP_NAME = re.compile(
    re.escape(pb.PBRUN_STAMP_PREFIX)
    + "[0-9a-f]{%d}" % pb.PBRUN_GENERATED_FINGERPRINT_HEX_LENGTH
    + r"\.json")


class SnapshotRefused(ValueError):
    """The commit says it is a pbrun snapshot and does not verify."""


def _require(condition: object, reason: str) -> None:
    if not condition:
        raise SnapshotRefused(reason)


def _git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args],
                                   stderr=subprocess.DEVNULL, timeout=10)


def snapshot_schema(root: Path, commit: str) -> str | None:
    """The snapshot schema the commit subject names, or ``None`` if it is not one.

    An unknown version refuses rather than reading as "not a snapshot".
    """

    subject = _git(root, "log", "-1", "--format=%s", commit).strip()
    if not subject.startswith(SUBJECT_PREFIX):
        return None
    schema = SNAPSHOT_SCHEMAS.get(subject[len(SUBJECT_PREFIX):])
    _require(schema, "unsupported pbrun snapshot version")
    return schema


def verified_stamp(root: Path, commit: str, *, cas_root: Path,
                   owner: str | None, schema: str) -> dict[str, object]:
    """The closure stamp of ``commit``, verified against its sealed action.

    The checkout's directory name locates the request; only the full checks
    below verify it.
    """

    match = re.fullmatch(r"([0-9a-f]{12})\.[^/]+", root.parent.name)
    _require(root.name == "checkout" and match, "pbrun action locator is unavailable")
    prefix = match.group(1)
    candidates = list(Path(cas_root, "requests", prefix[:2]).glob(prefix + "*.json"))
    _require(len(candidates) == 1, "pbrun action lookup is missing or ambiguous")
    raw = candidates[0].read_bytes()
    action = json.loads(raw)
    _require(isinstance(action, dict), "pbrun action is not an object")
    key = action["action_key"]
    body = {name: value for name, value in action.items() if name != "action_key"}
    _require(isinstance(key, str) and re.fullmatch(r"[0-9a-f]{64}", key)
             and key == candidates[0].stem and key == pb.canonical_sha256(body),
             "pbrun action key does not verify")
    _require(action["schema"] == pb.ACTION_SCHEMA_V2
             and action["task"]["definition_id"] == "fleet/pbrun"
             and action["task"]["definition_version"] == "v1",
             "unsupported pbrun action")
    params = action["params"]
    snapshot = params["checkout_snapshot"]
    _require(snapshot["schema"] == schema and snapshot["commit"] == commit,
             "pbrun action names another snapshot")
    _require(snapshot["input"] in action["inputs"], "pbrun snapshot input is not sealed")
    _require(params["cwd"] == snapshot["subdirectory"], "pbrun logical cwd differs")
    variables = action["environment"]["variables"]
    _require(isinstance(variables, dict), "pbrun environment variables are not an object")
    _require(owner and owner == variables.get(pool.CONTAINER_OWNER_ENV),
             "pbrun action owner differs or is unavailable")
    closure = action["code_closure"]
    _require(isinstance(closure, dict), "pbrun closure is not an object")
    closure_body = {name: value for name, value in closure.items()
                    if name != "closure_sha256"}
    _require(closure["schema"] == pb.CODE_CLOSURE_SCHEMA_V1
             and closure["closure_sha256"] == pb.canonical_sha256(closure_body)
             and len(closure["files"]) == 1, "pbrun closure does not verify")
    entry = closure["files"][0]
    filename = entry["path"]
    _require(isinstance(filename, str) and _STAMP_NAME.fullmatch(filename),
             "pbrun closure is not the generated stamp")
    subdirectory = Path(snapshot["subdirectory"])
    _require(not subdirectory.is_absolute() and ".." not in subdirectory.parts,
             "pbrun snapshot subdirectory escapes the checkout")
    relative = subdirectory / filename
    stamp_path = root / relative
    _require(stat.S_ISREG(stamp_path.lstat().st_mode), "pbrun stamp is not a regular file")
    stamp_raw = stamp_path.read_bytes()
    _require(len(stamp_raw) == entry["bytes"]
             and hashlib.sha256(stamp_raw).hexdigest() == entry["sha256"],
             "pbrun stamp differs from its sealed closure")
    _require(_git(root, "show", f"{commit}:{relative.as_posix()}") == stamp_raw,
             "pbrun stamp differs from its snapshot blob")
    stamp = json.loads(stamp_raw)
    _require(isinstance(stamp, dict) and set(stamp) == {"cwd", "head", "dirty_sha256"}
             and stamp["cwd"] == params["cwd"]
             and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", stamp["head"])
             and re.fullmatch(r"[0-9a-f]{64}", stamp["dirty_sha256"]),
             "pbrun stamp payload is not generated closure metadata")
    identity = {name: stamp[name] for name in ("head", "dirty_sha256")}
    result_name, stamp_name = pbrun.result_and_stamp_names(
        params["command"], params["cwd"], params["demand"], variables,
        identity=identity, placement=params["placement"])
    _require(filename == stamp_name and action["task"]["result_path"] == result_name,
             "pbrun stamp name does not match its action fingerprint")
    return {"path": relative.as_posix(), "bytes": entry["bytes"],
            "sha256": entry["sha256"], "action_key": key,
            "request_sha256": hashlib.sha256(raw).hexdigest()}


def verify(checkout: str | os.PathLike, commit: str, *,
           cas_root: str | os.PathLike = DEFAULT_CAS_ROOT,
           owner: str | None = None) -> dict[str, object]:
    """What PrismaBuild generated in ``checkout`` at ``commit``.

    Returns the ``prismabuild.checkout_snapshot.v1`` record.  Raises
    :class:`SnapshotRefused` when the commit says it is a snapshot and does not
    verify.  ``owner`` defaults to this process's container owner variable.
    """

    root = Path(checkout)
    schema = snapshot_schema(root, commit)
    if schema is None:
        return {"schema": SCHEMA, "snapshot": False, "generated": []}
    if owner is None:
        owner = os.environ.get(pool.CONTAINER_OWNER_ENV)
    try:
        stamp = verified_stamp(root, commit, cas_root=Path(cas_root),
                               owner=owner, schema=schema)
    except SnapshotRefused:
        raise
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        raise SnapshotRefused(f"pbrun snapshot does not verify: "
                              f"{type(exc).__name__}: {exc}") from exc
    return {"schema": SCHEMA, "snapshot": True, "generated": [stamp]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("verify", help="verify a checkout's generated files")
    check.add_argument("checkout", help="Git checkout containing the commit to inspect")
    check.add_argument("commit", help="Git commit or ref whose generated files must verify")
    check.add_argument("--cas-root", default=str(DEFAULT_CAS_ROOT),
                       help="CAS root used to verify the generated-file request and content bindings")
    check.add_argument("--owner", default=None,
                       help=f"default: ${pool.CONTAINER_OWNER_ENV}")
    args = parser.parse_args(argv)
    try:
        record = verify(args.checkout, args.commit, cas_root=args.cas_root,
                        owner=args.owner)
    except (SnapshotRefused, OSError, subprocess.SubprocessError) as exc:
        print(f"pbsnapshot: refused: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(record, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
