"""Check PR #2053's raw parent preservation in an admitted PB checkout."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
PARENTS = ("c9e297a786ecab4cdea942a16d11963ed3b7e7ef",
           "a8e0f76abc6757e1193eec7cfabea06b70a89fbf")
ARCHIVE = ROOT / "tests/compat/history-controls/superseded-upgrade-intent-563da2ff3aa4ad5d"
SIDECAR = ARCHIVE / "provenance-9284c8e1585709efaf4fcce8f2813464e68bf8fd.json"


def git_bytes(parent, path):
    return subprocess.check_output(["git", "-C", str(ROOT), "show", parent + ":" + path])


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify():
    prefix = str(ARCHIVE.relative_to(ROOT)) + "/"
    require((ARCHIVE / "provenance.json").read_bytes() == git_bytes(PARENTS[1], prefix + "provenance.json"),
            "canonical main provenance changed")
    require(SIDECAR.read_bytes() == git_bytes(PARENTS[0], prefix + "provenance.json"),
            "approved adoption sidecar changed or was discarded")
    for name in ("capture-rollout.json", "goldens-rollout.jsonl"):
        require((ARCHIVE / name).read_bytes() == git_bytes(PARENTS[0], prefix + name)
                == git_bytes(PARENTS[1], prefix + name), "shared archive bytes changed")
    for parent in PARENTS:
        directory = ARCHIVE.parent / ("merge-2053-" + parent)
        manifest = json.loads((directory / "preservation-manifest.json").read_bytes())
        require(manifest["source_parent"] == parent, "parent manifest misattributes source")
        require(set(manifest["files"]) == {"capture-rollout.json", "goldens-rollout.jsonl"},
                "incomplete parent capture pair")
        for name, identity in manifest["files"].items():
            raw = (directory / name).read_bytes()
            require(raw == git_bytes(parent, identity["original_path"]), "parent bytes changed")
            require(identity["sha256"] == hashlib.sha256(raw).hexdigest()
                    and identity["bytes"] == len(raw), "parent manifest disagrees with raw bytes")
    # Exercise actual retained-source discovery. Nested preservation files must
    # not become generation descriptors, and the original descriptor stays live.
    sys.path.insert(0, str(ROOT / "tests/compat/support"))
    from record_compat import retained
    controls = ARCHIVE.parent
    index = controls / "prepared.json"
    descriptors = (json.loads(index.read_bytes()) if index.exists() else
                   [json.loads(path.read_bytes()) for path in sorted(controls.glob("*.json"))])
    with retained(controls, history_control=True) as generations:
        require([row[0] for row in generations] == [row["generation"] for row in descriptors],
                "raw preservation changed retained-generation discovery")
    print(json.dumps({"parent_pairs_verified": len(PARENTS), "provenance_events_verified": 2,
                      "retained_generations": [row["generation"] for row in descriptors],
                      "raw_sidecar_sha256": hashlib.sha256(SIDECAR.read_bytes()).hexdigest()}, sort_keys=True))


if __name__ == "__main__":
    verify()
