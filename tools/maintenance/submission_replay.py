"""Replay the pinned C14 patch from its exact base without creating commits."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import raw_sha256

BASE = "f0bcb21e195378e7fb6e85e1bbfde24c45672e6b"


def replay_submission_patch(repo: Path, patch: Path, *, digest: str, tree: str):
    raw = patch.read_bytes()
    if raw_sha256(raw) != digest:
        raise ValueError("migration patch does not match its pinned digest")
    with tempfile.TemporaryDirectory(prefix="c14-replay-") as temporary:
        checkout = Path(temporary) / "checkout"
        subprocess.run(["git", "clone", "--no-hardlinks", "--quiet", "--no-checkout",
                        str(repo), str(checkout)], check=True)
        def git(*arguments, input=None):
            return subprocess.check_output(["git", "-C", str(checkout), *arguments], input=input)
        git("checkout", "--quiet", "--detach", BASE)
        if git("status", "--porcelain"):
            raise ValueError("replay base is not clean")
        git("apply", "--index", "--binary", "--whitespace=nowarn", "-", input=raw)
        actual = git("write-tree").decode().strip()
        if actual != tree:
            raise ValueError(f"replayed tree {actual} differs from {tree}")
        return {"base": BASE, "tree": actual, "patch_sha256": digest, "patch_bytes": len(raw),
                "paths": git("diff", "--cached", "--name-only").decode().splitlines(),
                "untracked": git("ls-files", "--others", "--exclude-standard").decode().splitlines()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--patch", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = replay_submission_patch(args.repo, args.patch, digest=args.sha256, tree=args.tree)
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return bool(result["untracked"])


if __name__ == "__main__":
    raise SystemExit(main())
