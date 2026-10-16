"""Generate and qualify exact production writer bytes inside an admitted action."""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild.record_kinds import KINDS, cases
from prismabuild.compatibility_window import require_reachable

HERE = Path(__file__).resolve().parent
CAPTURE = ROOT / "tests/compat/generations"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def reached(observed, reference):
    module, qualified = reference.split(":")
    if module.startswith("prismabuild."):
        paths = {"src/" + module.replace(".", "/") + ".py"}
    else:
        paths = {"tools/" + module + ".py", "tools/fleet/" + module + ".py"}
    return any(call[0] in paths and call[3] == qualified
               for call in observed.get("calls", []))


def invoke(root, case, store, *, raw=None, manifest=None, evidence=None):
    kind = next(kind for kind in KINDS if kind.name == case.split("/", 1)[0])
    request = {"case": case, "op": "write" if raw is None else "read"}
    if raw is not None:
        request["raw"] = base64.b64encode(raw).decode()
    command = [sys.executable, "-I", "-S", str(HERE / "compat_isolate.py"),
               "--root", str(root), "--adapters", str(HERE / "compat_adapters.py"),
               "--store", str(store)]
    if manifest:
        command.extend(["--manifest", str(manifest)])
    if kind.stable_paths:
        bwrap = shutil.which("bwrap")
        if not bwrap:
            raise RuntimeError("stable-path writer fixture requires qualified bubblewrap; no fallback")
        store.mkdir(parents=True, exist_ok=True)
        stable = Path("/opt/prismabuild-compat")
        namespace = [bwrap, "--unshare-pid", "--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc", "--tmpfs", "/opt",
                     "--ro-bind", str(root), str(stable / "generation"),
                     "--ro-bind", str(HERE), str(stable / "driver"),
                     "--bind", str(store), str(stable / "store")]
        command = [sys.executable, "-I", "-S", str(stable / "driver/compat_isolate.py"),
                   "--root", str(stable / "generation"), "--adapters", str(stable / "driver/compat_adapters.py"),
                   "--store", str(stable / "store")]
        if manifest:
            namespace.extend(["--ro-bind", str(manifest), str(stable / "manifest.json")])
            command.extend(["--manifest", str(stable / "manifest.json")])
        command = namespace + ["--"] + command
    process = subprocess.run(command, input=json.dumps(request), text=True,
                             capture_output=True, timeout=45)
    try:
        result = json.loads(process.stdout)
    except ValueError:
        result = {"stdout": process.stdout, "stderr": process.stderr}
    result["returncode"] = process.returncode
    if kind.stable_paths and result.get("affinity") != sorted(os.sched_getaffinity(0)):
        result["returncode"] = 2
        result["attestation_error"] = "private namespace did not preserve PB CPU affinity"
    if raw is None and not kind.fixture_source:
        writer = dict(kind.variant_writers).get(case.split("/", 1)[1])
        if writer is None:
            if len(kind.writers) != 1:
                raise ValueError("each variant of a multi-writer kind must name its actual writer")
            writer = kind.writers[0]
        references = (writer,) + kind.legacy_writers
    else:
        references = (kind.reader,) + kind.legacy_readers
    result["catalogued_operation"] = "source-reader" if raw is None and kind.fixture_source else request["op"]
    result["catalogued_entry_verified"] = any(reached(result, reference) for reference in references)
    if raw is None and result["returncode"] == 0 and kind.wire_id is not None:
        actual_wire = json.loads(base64.b64decode(result["result"]["raw"]))
        result["wire_identity_verified"] = isinstance(actual_wire, dict) and actual_wire.get("schema") == kind.wire_id
        if not result["wire_identity_verified"]:
            result["returncode"] = 3
            result["entry_error"] = "actual writer bytes disagree with the catalogued wire identity"
    if process.returncode in (0, 1) and not result["catalogued_entry_verified"]:
        result["returncode"] = 3
        result["entry_error"] = "adapter did not reach its catalogued production entry"
    if raw is None and kind.fixture_source and result["returncode"] == 0:
        captured = result["result"]
        source = Path(captured["source_file"]).relative_to(
            Path("/opt/prismabuild-compat/generation") if kind.stable_paths else root)
        digest = sha(base64.b64decode(captured["raw"]))
        if (str(source) not in kind.fixture_source or digest != captured["source_sha256"]
                or sha((root / source).read_bytes()) != digest
                or manifest and json.loads(manifest.read_bytes())["files"].get(str(source)) != digest):
            result["returncode"] = 2
            result["attestation_error"] = "configuration bytes do not match their immutable source provenance"
    if evidence:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        with evidence.open("x") as stream:
            stream.write(json.dumps(result, sort_keys=True, indent=2) + "\n")
        evidence.chmod(0o444)
    return result


def accepted(result):
    assert result["returncode"] == 0, result
    assert result.get("isolated") and result.get("no_site") and result.get("imports"), result
    return result["result"]


def write(root, case, store, **kw):
    return base64.b64decode(accepted(invoke(root, case, store, **kw))["raw"])


@contextmanager
def retained(directory=CAPTURE, *, history_control=False):
    if history_control:
        sources = [json.loads(path.read_bytes()) for path in sorted(directory.glob("*.json"))]
    else:
        window = json.loads((directory / "window.json").read_bytes())
        assert window["complete"] is True and window["rollback"]["generation"] in {
            row["generation"] for row in window["generations"]}
        prepared = directory / "prepared.json"
        sources = window["generations"] + (json.loads(prepared.read_bytes()) if prepared.exists() else [])
    with tempfile.TemporaryDirectory(prefix="compat-closure-") as tmp:
        result = []
        for generation in sources:
            name = generation["generation"]
            manifest_path = directory / (name + ".json")
            manifest = json.loads(manifest_path.read_bytes())
            archive = directory / (name + ".tar.gz")
            assert sha(archive.read_bytes()) == generation["archive_sha256"] == manifest["archive_sha256"]
            destination = Path(tmp) / name
            destination.mkdir()
            with tarfile.open(archive) as tar:
                provenance_file = generation.get("provenance_file", "RUNTIME_VERSION.json")
                expected = set(manifest["files"]) | {provenance_file}
                assert len(tar.getmembers()) == len(expected)
                assert {member.name for member in tar.getmembers()} == expected
                for member in tar.getmembers():
                    assert member.isfile() and not member.name.startswith("/") and ".." not in Path(member.name).parts
                    content = tar.extractfile(member).read()
                    path = destination / member.name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(content)
                    path.chmod(0o444)
            receipt = (destination / provenance_file).read_bytes()
            assert sha(receipt) == generation["receipt_sha256"] == manifest["receipt_sha256"]
            version = json.loads(receipt)
            assert version["generation"] == name and version["commit"] == generation["commit"] == manifest["commit"]
            for name, expected in manifest["files"].items():
                assert sha((destination / name).read_bytes()) == expected == version["files"][name]
            if not history_control:
                require_reachable(ROOT, destination)
            result.append((generation["generation"], destination, manifest_path))
        yield result


def generate(output, selected=None):
    output.mkdir(parents=True, exist_ok=False)
    historical, goldens, failures = [], [], []
    started = datetime.now(timezone.utc).isoformat()
    selected = list(cases() if selected is None else selected)
    source = subprocess.check_output(["git", "-C", str(ROOT), "show", "-s", "--format=%H%n%P%n%T"],
                                     text=True).splitlines()

    def check(result, case, generation, phase):
        if result["returncode"]:
            failures.append({"case": case, "generation": generation, "phase": phase,
                             "returncode": result["returncode"], "error": result.get("error"),
                             "attestation_error": result.get("attestation_error"),
                             "entry_error": result.get("entry_error")})
            return False
        accepted(result)
        return True

    with retained() as generations, tempfile.TemporaryDirectory(prefix="compat-generate-") as tmp:
        for case in selected:
            kind = next(kind for kind in KINDS if kind.name == case.split("/", 1)[0])
            phase = "source" if kind.fixture_source else "writer"
            label = case.replace("/", "-")
            before = len(failures)
            result = invoke(ROOT, case, Path(tmp) / (label + "-new"),
                            evidence=output / "attestations" / (label + "-new-" + phase + ".json"))
            if not check(result, case, "candidate", phase):
                continue
            raw = base64.b64decode(result["result"]["raw"])
            origin = {"origin": "immutable configuration; no runtime writer"} if kind.fixture_source else {}
            goldens.append({"case": case, "sha256": sha(raw), "raw": base64.b64encode(raw).decode(), **origin})
            for generation, old, manifest in generations:
                prefix = output / "attestations" / generation
                stores = Path(tmp) / generation
                result = invoke(old, case, stores / (label + "-old"), manifest=manifest,
                                evidence=prefix / (label + "-old-" + phase + ".json"))
                if not check(result, case, generation, phase):
                    continue
                prior = base64.b64decode(result["result"]["raw"])
                check(invoke(ROOT, case, stores / (label + "-forward"), raw=prior,
                             evidence=prefix / (label + "-forward.json")), case, generation, "forward")
                check(invoke(old, case, stores / (label + "-reverse"), raw=raw, manifest=manifest,
                             evidence=prefix / (label + "-reverse.json")), case, generation, "reverse")
                historical.append({"case": case, "generation": generation,
                                   "receipt_sha256": json.loads(manifest.read_bytes())["receipt_sha256"],
                                   "sha256": sha(prior), "raw": base64.b64encode(prior).decode(), **origin})
            print(case, "both directions accepted" if len(failures) == before else "RED; evidence retained", flush=True)
    for name, rows in (("goldens.jsonl", goldens), ("historical.jsonl", historical)):
        path = output / name
        with path.open("x") as stream:
            for row in rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
        path.chmod(0o444)
    path = output / "qualification.json"
    path.write_text(json.dumps({"current_captures": len(goldens), "historical_captures": len(historical),
                                "configuration_captures": sum("origin" in row for row in goldens),
                                "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
                                "selected_cases": selected,
                                "source_git": {"snapshot": source[0], "parents": source[1].split(), "tree": source[2]},
                                "failures": failures, "passed": not failures}, sort_keys=True, indent=2) + "\n")
    path.chmod(0o444)
    if failures:
        raise SystemExit(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", type=Path, required=True)
    parser.add_argument("--expect-machine")
    parser.add_argument("--case", action="append")
    parser.add_argument("--kind", action="append", help="capture every scenario of a named record kind")
    args = parser.parse_args()
    if args.expect_machine and platform.machine() != args.expect_machine:
        parser.error(f"expected {args.expect_machine}, observed {platform.machine()}")
    print(json.dumps({"machine": platform.machine(), "python": sys.version}), flush=True)
    if args.case and set(args.case) - set(cases()):
        parser.error("unknown catalogue case")
    if args.kind and set(args.kind) - {kind.name for kind in KINDS}:
        parser.error("unknown catalogue kind")
    selected = set(args.case or ())
    if args.kind:
        selected.update(case for case in cases() if case.split("/")[0] in args.kind)
    generate(args.generate, [case for case in cases() if case in selected] if selected else None)


if __name__ == "__main__":
    main()
