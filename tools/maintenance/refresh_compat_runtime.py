"""Adopt source-identity-only runtime goldens from a verified PB capture."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def differences(before, after, path=""):
    if isinstance(before, dict) and isinstance(after, dict):
        if not (before.keys() == after.keys()):
            raise ValueError(('wire member sets changed', path))
        return [row for key in before
                for row in differences(before[key], after[key], path + "/" + key)]
    return [] if before == after else [dict(path=path, before=before, after=after)]


def refresh(args):
    root = args.root.resolve()
    data = root / "tests/compat"
    capture = args.capture.resolve()
    report = json.loads((capture / "qualification.json").read_bytes())
    if not (report['passed'] and (not report['failures'])):
        raise ValueError('capture did not pass qualification')
    parents = report["source_git"]["parents"]
    if not (len(parents) == 1):
        raise ValueError('capture integrity check failed: len(parents) == 1')
    producer = parents[0]
    unchanged = ["src", "tools/prismabuild_worker.py", "tests/compat/support"]
    drift = subprocess.check_output(
        ["git", "-C", str(root), "diff", "--name-only", producer, "--", *unchanged],
        text=True).splitlines()
    if not (not drift):
        raise ValueError(('capture source differs from current production/driver', drift))
    prior = data / "goldens-receipt.jsonl"
    old_raw = prior.read_bytes()
    before = {row["case"]: row for row in rows(prior)}
    current = {row["case"]: row for row in rows(capture / "goldens.jsonl")}
    expected = {kind + "/" + variant for kind in
                ("core-receipt", "core-attestation", "core-runtime")
                for variant in ("in-process", "script")}
    if not (set(before) == set(current) == set(report['selected_cases']) == expected):
        raise ValueError("capture integrity check failed: set(before) == set(current) == set(report['selected_cases']) == expected")
    if not ((capture / 'historical.jsonl').read_bytes() == (data / 'historical-receipt.jsonl').read_bytes()):
        raise ValueError('historical bytes changed')
    changes = {}
    identities = {}
    for name in ("core", "digest_primitives"):
        raw = (root / "src/prismabuild" / (name + ".py")).read_bytes()
        identities[name] = dict(bytes=len(raw), sha256=sha(raw))
    for case in sorted(expected):
        old = base64.b64decode(before[case]["raw"], validate=True)
        new = base64.b64decode(current[case]["raw"], validate=True)
        if not (sha(old) == before[case]['sha256'] and sha(new) == current[case]['sha256']):
            raise ValueError("capture integrity check failed: sha(old) == before[case]['sha256'] and sha(new) == current[case]['sha256']")
        changes[case] = differences(json.loads(old), json.loads(new))
        prefix = ("/producer/runtime" if case.startswith("core-receipt/") else
                  "/runtime" if case.startswith("core-attestation/") else "")
        allowed = {prefix + "/runtime_sha256"}
        if case.startswith("core-receipt/"):
            allowed |= {"/producer/attestation_sha256", "/receipt_sha256"}
        elif case.startswith("core-attestation/"):
            allowed.add("/attestation_sha256")
        for name in identities:
            allowed |= {prefix + "/" + name + "/" + field for field in ("bytes", "sha256")}
        if not ({row['path'] for row in changes[case]} <= allowed):
            raise ValueError("capture integrity check failed: {row['path'] for row in changes[case]} <= allowed")
        if not (changes[case]):
            raise ValueError('capture did not change this current-source golden')
        body = json.loads(new)
        runtime = body["producer"]["runtime"] if case.startswith("core-receipt/") else body["runtime"] if case.startswith("core-attestation/") else body
        for name, identity in identities.items():
            if not (all((runtime[name][field] == value for field, value in identity.items()))):
                raise ValueError('capture integrity check failed: all((runtime[name][field] == value for field, value in identity.items()))')
    archive = data / "history-controls" / ("superseded-runtime-" + sha(old_raw)[:16])
    if not (not archive.exists()):
        raise ValueError('old current bytes already archived; inspect partial adoption')
    with tempfile.TemporaryDirectory(prefix="adopt-runtime-") as tmp:
        staged = Path(tmp)
        subprocess.run([sys.executable, str(root / "tests/compat/support/adopt_compat_capture.py"),
                        "--capture", str(capture), "--destination", str(staged),
                        "--suffix", "receipt", "--primary-audit", str(args.primary_audit)], check=True)
        if not ((staged / 'historical-receipt.jsonl').read_bytes() == (data / 'historical-receipt.jsonl').read_bytes()):
            raise ValueError("capture integrity check failed: (staged / 'historical-receipt.jsonl').read_bytes() == (data / 'historical-receipt.jsonl').read_bytes()")
        if not (not (staged / 'absent-participants-receipt.jsonl').exists()):
            raise ValueError("capture integrity check failed: not (staged / 'absent-participants-receipt.jsonl').exists()")
        archive.mkdir(parents=True)
        (archive / prior.name).write_bytes(old_raw)
        provenance = dict(reason="Only attested Core/digest source identities and dependent self-digests changed; actual writer bytes independently reproduced and unchanged retained consumers accepted both directions.",
                          sha256=sha(old_raw), records=len(before), credit="superseded current-source fixture; no supported historical-generation credit",
                          capture_source_parent=producer, differences=changes)
        old_manifest = data / "capture-receipt.json"
        if old_manifest.exists():
            shutil.copyfile(old_manifest, archive / old_manifest.name)
        (archive / "provenance.json").write_text(json.dumps(provenance, sort_keys=True, indent=2) + "\n")
        for path in archive.iterdir():
            path.chmod(0o444)
        for name in ("goldens-receipt.jsonl", "capture-receipt.json"):
            target = data / name
            if target.exists():
                target.chmod(0o644)
            shutil.copyfile(staged / name, target)
            target.chmod(0o444)
    result = dict(cases=6, historical_rows_byte_exact=12, source_identities=identities,
                  archive=str(archive.relative_to(root)), capture_source_parent=producer,
                  current_goldens_sha256=sha(prior.read_bytes()), differences=changes)
    with args.report.open("x") as stream:
        stream.write(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("cases", "historical_rows_byte_exact", "archive", "current_goldens_sha256")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--primary-audit", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    refresh(parser.parse_args())


if __name__ == "__main__":
    main()
