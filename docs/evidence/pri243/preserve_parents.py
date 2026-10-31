"""Preserve and inspect the exact two-parent rollout fixture history."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
PR = "a1bac460f128cbd7872349c8a679277efd8c485c"
MAIN = "8be4d4056fbcd6285a932d3ac4bf5d8bf6015036"
DATA = "tests/compat/"
HISTORY = DATA + "history-controls/"
COLLISION = HISTORY + "superseded-upgrade-intent-f1d6ce6052c13d54/"


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def mappings():
    for parent in (PR, MAIN):
        for name in ("goldens-rollout.jsonl", "capture-rollout.json", "historical-rollout.jsonl"):
            yield parent, DATA + name, HISTORY + "parent-rollout-" + parent[:12] + "/" + name
        prefix = COLLISION if parent == MAIN else COLLISION.rstrip("/") + "-" + parent[:12] + "/"
        for source in git("ls-tree", "-r", "--name-only", parent, "--", COLLISION).decode().splitlines():
            yield parent, source, prefix + source.removeprefix(COLLISION)


def preserve():
    for parent, source, target in mappings():
        raw = git("show", parent + ":" + source)
        path = ROOT / target
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.read_bytes() == raw:
            continue
        if path.exists() and target != source:
            raise ValueError("refusing to overwrite an existing immutable archive: " + target)
        path.write_bytes(raw)
        path.chmod(0o444)
    for name in ("goldens-rollout.jsonl", "capture-rollout.json"):
        (ROOT / DATA / name).write_bytes(git("show", MAIN + ":" + DATA + name))


def inspect():
    rows = []
    for parent, source, target in mappings():
        raw = git("show", parent + ":" + source)
        if (ROOT / target).read_bytes() != raw:
            raise ValueError("parent bytes differ: " + target)
        rows.append(dict(parent=parent, source=source, preserved=target,
                         bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
    print(json.dumps(rows, sort_keys=True, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preserve", action="store_true")
    args = parser.parse_args()
    if args.preserve:
        preserve()
    inspect()
