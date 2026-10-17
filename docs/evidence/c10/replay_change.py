#!/usr/bin/env python3
"""Prove the exact binary source patch twice in an isolated Git index."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--base", default="578e54d0788c0f3b138f5dcf217e980322bb7726")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    def git(*words, env=None, stdin=None):
        return subprocess.check_output(["git", *words], cwd=args.root, env=env, input=stdin)

    base = git("rev-parse", args.base).decode().strip()
    head = git("rev-parse", args.head).decode().strip()
    expected = git("rev-parse", head + "^{tree}").decode().strip()
    patch = git("diff", "--binary", "--full-index", base, head)
    repeated = git("diff", "--binary", "--full-index", base, head)
    assert patch == repeated
    (args.output / "source.patch").write_bytes(patch)
    with tempfile.TemporaryDirectory(prefix="c10-source-replay-") as temporary:
        environment = dict(os.environ, GIT_INDEX_FILE=str(Path(temporary) / "index"))
        git("read-tree", base, env=environment)
        results = []
        for _ in range(2):
            tree = git("write-tree", env=environment).decode().strip()
            if tree == expected:
                results.append("already-applied")
                continue
            assert tree == git("rev-parse", base + "^{tree}").decode().strip()
            git("apply", "--cached", "--check", "-", env=environment, stdin=patch)
            git("apply", "--cached", "-", env=environment, stdin=patch)
            assert git("write-tree", env=environment).decode().strip() == expected
            results.append("applied")
        git("apply", "--cached", "--reverse", "--check", "-", env=environment, stdin=patch)
    assert results == ["applied", "already-applied"]
    changes = []
    for row in git("diff", "--raw", "--no-abbrev", base, head).decode().splitlines():
        metadata, path = row.split("\t")
        old_mode, new_mode, old_blob, new_blob, status = metadata.split()
        assert not status.startswith("R"), "literal paths are preserved"
        changes.append(dict(path=path, status=status, before_blob=old_blob,
                            after_blob=new_blob, before_mode=old_mode[1:], after_mode=new_mode))
    report = dict(source_only=True, base=base, head=head, tree=expected,
                  patch_sha256=hashlib.sha256(patch).hexdigest(), patch_bytes=len(patch),
                  deterministic=True, replay=results, idempotent=True, files=changes)
    (args.output / "replay.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in
                      ("base", "head", "tree", "deterministic", "replay", "idempotent")}))


if __name__ == "__main__":
    main()
