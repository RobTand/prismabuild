"""Generate and qualify exact production writer bytes inside an admitted action."""
from __future__ import annotations

import argparse
import ast
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import gzip
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
from compat_references import module_paths

HERE = Path(__file__).resolve().parent
CAPTURE = ROOT / "tests/compat/generations"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def decode_wire(kind, raw):
    if kind.encoding == "move-pricing-line":
        return json.loads(raw.split(b"\t", 2)[2])
    if kind.encoding not in ("json", "gzip-json"):
        raise ValueError(f"unqualified record encoding: {kind.encoding}")
    return json.loads(gzip.decompress(raw) if kind.encoding == "gzip-json" else raw)


def probe_wire(kind, body):
    raw = (json.dumps(body, sort_keys=True) + "\n").encode()
    if kind.encoding == "move-pricing-line":
        from prismabuild.pool import _move_pricing_line
        framing = _move_pricing_line("a" * 64 + ".json", {}).split(b"\t", 2)[:2]
        return b"\t".join([*framing, raw])
    return gzip.compress(raw, mtime=0) if kind.encoding == "gzip-json" else raw


def reached(observed, reference):
    module, qualified = reference.split(":")
    paths = module_paths(module)
    return any(call[0] in paths and call[3] == qualified
               for call in observed.get("calls", []))


def historical_absence(root, case, manifest):
    kind = next(kind for kind in KINDS if kind.name == case.split("/", 1)[0])
    if not (kind.older_input_absence or kind.older_participant_absence):
        return None
    assert kind.wire_id
    if kind.older_input_absence:
        assert kind.caller_input and not kind.writers
    paths = []
    for reference in (kind.reader,) + kind.legacy_readers:
        module = reference.split(":")[0]
        paths.extend(module_paths(module))
    if any((root / path).is_file() for path in paths):
        if kind.older_input_absence:
            return None
        for reference in (kind.reader,) + kind.writers + kind.legacy_readers + kind.legacy_writers:
            module, qualified = reference.split(":")
            candidates = module_paths(module)
            for candidate in candidates:
                if not (root / candidate).is_file():
                    continue
                nodes = ast.parse((root / candidate).read_bytes()).body
                found = True
                for component in qualified.split("."):
                    node = next((node for node in nodes if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                                 and node.name == component), None)
                    if node is None:
                        found = False
                        break
                    nodes = node.body
                if found:
                    return None
    provenance = json.loads(manifest.read_bytes())
    for name, expected in provenance["files"].items():
        raw = (root / name).read_bytes()
        assert sha(raw) == expected, "absence claim requires every immutable closure member"
        assert kind.wire_id.encode() not in raw, "missing adapter module does not prove this wire kind absent"
    return {"case": case, "generation": provenance["generation"],
            "receipt_sha256": provenance["receipt_sha256"], "archive_sha256": provenance["archive_sha256"],
            "absent_reader_modules": sorted(paths), "closure_files_checked": len(provenance["files"]),
            "reason": ("caller input has no reader or writer in this generation; no historical writer bytes exist"
                       if kind.caller_input else
                       "wire ID and all declared reader/writer entries absent from the complete immutable generation; no codec credit")}


def invoke(root, case, store, *, raw=None, manifest=None, evidence=None, adapter_path=None):
    kind = next(kind for kind in KINDS if kind.name == case.split("/", 1)[0])
    request = {"case": case, "op": "write" if raw is None else "read"}
    if raw is not None:
        request["raw"] = base64.b64encode(raw).decode()
    adapter_path = HERE / "compat_adapters.py" if adapter_path is None else adapter_path
    command = [sys.executable, "-I", "-S", str(HERE / "compat_isolate.py"),
               "--root", str(root), "--adapters", str(adapter_path),
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
                   "--root", str(stable / "generation"), "--adapters", str(
                       stable / "driver" / adapter_path.name if adapter_path.parent == HERE else adapter_path),
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
    if raw is None and not (kind.fixture_source or kind.caller_input):
        writer = dict(kind.variant_writers).get(case.split("/", 1)[1])
        if writer is None:
            if len(kind.writers) != 1:
                raise ValueError("each variant of a multi-writer kind must name its actual writer")
            writer = kind.writers[0]
        references = (writer,) + kind.legacy_writers
    else:
        references = (dict(kind.variant_readers).get(case.split("/", 1)[1], kind.reader),) + kind.legacy_readers
    if raw is None and kind.caller_input and kind.writers:
        present = []
        for reference in kind.writers:
            module = reference.split(":", 1)[0]
            candidates = module_paths(module)
            if any((root / path).is_file() for path in candidates):
                present.append(reference)
        result["caller_input_constructor_verified"] = any(reached(result, reference) for reference in present) if present else None
        if present and not result["caller_input_constructor_verified"]:
            result["returncode"] = 3
            result["entry_error"] = "available qualification-input constructor was not reached"
    result["catalogued_operation"] = ("input-reader" if raw is None and kind.caller_input else
                                     "source-reader" if raw is None and kind.fixture_source else request["op"])
    result["catalogued_entry_verified"] = any(reached(result, reference) for reference in references)
    if raw is None and result["returncode"] == 0 and kind.wire_id is not None:
        actual_wire = decode_wire(kind, base64.b64decode(result["result"]["raw"]))
        result["wire_identity_verified"] = isinstance(actual_wire, dict) and actual_wire.get("schema") == kind.wire_id
        if not result["wire_identity_verified"]:
            result["returncode"] = 3
            result["entry_error"] = "actual writer bytes disagree with the catalogued wire identity"
    if process.returncode in (0, 1) and not result["catalogued_entry_verified"]:
        result["returncode"] = 3
        result["entry_error"] = "adapter did not reach its catalogued production entry"
    if process.returncode in (0, 1) and raw is not None and not result.get("consumer_boundary"):
        result["returncode"] = 3
        result["entry_error"] = "reader did not mark the end of fixture setup"
    if process.returncode in (0, 1) and not result.get("compiled_source_code_verified"):
        result["returncode"] = 2
        result["attestation_error"] = "executed production code was not verified against immutable source"
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
    assert result.get("compiled_source_code_verified") and result.get("stdlib_imports"), result
    assert result.get("interpreter", {}).get("sha256"), result
    return result["result"]


def write(root, case, store, *, observe=None, **kw):
    result = invoke(root, case, store, **kw)
    raw = base64.b64decode(accepted(result)["raw"])
    if observe is not None:
        observe(result, raw)
    return raw


@contextmanager
def retained(directory=CAPTURE, *, history_control=False):
    if history_control:
        index = directory / "prepared.json"
        sources = (json.loads(index.read_bytes()) if index.exists() else
                   [json.loads(path.read_bytes()) for path in sorted(directory.glob("*.json"))])
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


def historical_writer(kind, reader_root, reader_manifest, sources, *, case):
    source = dict(kind.variant_writer_sources).get(case.split("/", 1)[1], kind.historical_writer_source)
    if source is None:
        return reader_root, reader_manifest, None
    name, root, manifest = next(row for row in sources if row[0] == source)
    provenance = json.loads(manifest.read_bytes())
    return root, manifest, {key: provenance[key] for key in ("generation", "commit", "tree", "receipt_sha256", "archive_sha256", "published")}


def generate(output, selected=None):
    output.mkdir(parents=True, exist_ok=False)
    historical, goldens, failures, absences = [], [], [], []
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

    with retained() as generations, retained(ROOT / "tests/compat/writer-sources", history_control=True) as writer_sources, \
            tempfile.TemporaryDirectory(prefix="compat-generate-") as tmp:
        for case in selected:
            kind = next(kind for kind in KINDS if kind.name == case.split("/", 1)[0])
            phase = "input" if kind.caller_input else "source" if kind.fixture_source else "writer"
            label = case.replace("/", "-")
            before = len(failures)
            result = invoke(ROOT, case, Path(tmp) / (label + "-new"),
                            evidence=output / "attestations" / (label + "-new-" + phase + ".json"))
            if not check(result, case, "candidate", phase):
                continue
            raw = base64.b64decode(result["result"]["raw"])
            repeat = invoke(ROOT, case, Path(tmp) / (label + "-new-repeat"),
                            evidence=output / "attestations" / (label + "-new-repeat-" + phase + ".json"))
            if not check(repeat, case, "candidate", phase + "-repeat"):
                continue
            repeated = base64.b64decode(repeat["result"]["raw"])
            if raw != repeated:
                failures.append({"case": case, "generation": "candidate", "phase": phase + "-reproducibility",
                                 "expected_sha256": sha(raw), "actual_sha256": sha(repeated)})
                continue
            origin = ({"origin": "deterministic caller input; no runtime writer"} if kind.caller_input else
                      {"origin": "immutable configuration; no runtime writer"} if kind.fixture_source else {})
            goldens.append({"case": case, "sha256": sha(raw), "raw": base64.b64encode(raw).decode(), **origin})
            for generation, old, manifest in generations:
                absence = historical_absence(old, case, manifest)
                if absence is not None:
                    absences.append(absence)
                    check(invoke(ROOT, case, Path(tmp) / (label + "-current-input-" + generation), raw=raw,
                                 evidence=output / "attestations" / generation / (label + "-current-input.json")),
                          case, generation, "current-input-reader; historical participant absent")
                    continue
                prefix = output / "attestations" / generation
                stores = Path(tmp) / generation
                writer_root, writer_manifest, writer_provenance = historical_writer(kind, old, manifest, writer_sources, case=case)
                result = invoke(writer_root, case, stores / (label + "-old"), manifest=writer_manifest,
                                evidence=prefix / (label + "-old-" + phase + ".json"))
                if not check(result, case, generation, phase):
                    continue
                prior = base64.b64decode(result["result"]["raw"])
                repeat = invoke(writer_root, case, stores / (label + "-old-repeat"), manifest=writer_manifest,
                                evidence=prefix / (label + "-old-repeat-" + phase + ".json"))
                if not check(repeat, case, generation, phase + "-repeat"):
                    continue
                repeated = base64.b64decode(repeat["result"]["raw"])
                if prior != repeated:
                    failures.append({"case": case, "generation": generation, "phase": phase + "-reproducibility",
                                     "expected_sha256": sha(prior), "actual_sha256": sha(repeated)})
                    continue
                check(invoke(ROOT, case, stores / (label + "-forward"), raw=prior,
                             evidence=prefix / (label + "-forward.json")), case, generation, "forward")
                check(invoke(old, case, stores / (label + "-reverse"), raw=raw, manifest=manifest,
                             evidence=prefix / (label + "-reverse.json")), case, generation, "reverse")
                historical.append({"case": case, "generation": generation,
                                   "receipt_sha256": json.loads(manifest.read_bytes())["receipt_sha256"],
                                   "sha256": sha(prior), "raw": base64.b64encode(prior).decode(),
                                   **({"writer_source": writer_provenance} if writer_provenance else {}), **origin})
            print(case, "both directions accepted" if len(failures) == before else "RED; evidence retained", flush=True)
    for name, rows in (("goldens.jsonl", goldens), ("historical.jsonl", historical), ("absent-participants.jsonl", absences)):
        path = output / name
        with path.open("x") as stream:
            for row in rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
        path.chmod(0o444)
    path = output / "qualification.json"
    path.write_text(json.dumps({"current_captures": len(goldens), "historical_captures": len(historical),
                                "configuration_captures": sum("origin" in row for row in goldens),
                                "historical_participant_absences": len(absences),
                                "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
                                "selected_cases": selected,
                                "independent_writer_reproduction_required": True,
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
