"""Adopt source-identity-only current goldens from a verified PB capture."""
from __future__ import annotations

import argparse
import ast
import base64
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild.digest_primitives import raw_sha256 as sha, sorted_json


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def differences(before, after, path=""):
    if isinstance(before, dict) and isinstance(after, dict):
        if not (before.keys() == after.keys()):
            raise ValueError(('wire member sets changed', path))
        return [row for key in before
                for row in differences(before[key], after[key], path + "/" + key)]
    return [] if before == after else [dict(path=path, before=before, after=after)]


def runtime_source_pin(root):
    core = ast.parse((root / 'src/prismabuild/core.py').read_bytes())
    pins = [node for node in core.body if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == '_IMMUTABLE_FILES_SOURCE'
                    for target in node.targets)]
    raw = (root / 'src/prismabuild/immutable_files.py').read_bytes()
    identity = dict(sha256=sha(raw), bytes=len(raw))
    if len(pins) != 1 or ast.literal_eval(pins[0].value) != identity:
        raise ValueError('capture source requires the exact immutable helper pin in Core')
    return identity


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
    unchanged = ["src", "tools/fleet", "tools/prismabuild_worker.py", "tests/compat/support"]
    drift = subprocess.check_output(
        ["git", "-C", str(root), "diff", "--name-only", producer, "--", *unchanged],
        text=True).splitlines()
    if not (not drift):
        raise ValueError(('capture source differs from current production/driver', drift))
    intent = args.profile == "upgrade-intent"
    helper_identity = None if intent else runtime_source_pin(root)
    suffix = "rollout" if intent else "receipt"
    expected = ({"upgrade-intent/armed"} if intent else
                {kind + "/" + variant for kind in
                 ("core-receipt", "core-attestation", "core-runtime")
                 for variant in ("in-process", "script")})
    prior = data / f"goldens-{suffix}.jsonl"
    old_raw = prior.read_bytes()
    before = {row["case"]: row for row in rows(prior) if row["case"] in expected}
    current = {row["case"]: row for row in rows(capture / "goldens.jsonl")}
    if not (set(before) == set(current) == set(report['selected_cases']) == expected):
        raise ValueError("capture integrity check failed: set(before) == set(current) == set(report['selected_cases']) == expected")
    historical_path = data / f"historical-{suffix}.jsonl"
    historical = b"".join(line for line in historical_path.read_bytes().splitlines(keepends=True)
                           if json.loads(line)["case"] in expected)
    if not ((capture / 'historical.jsonl').read_bytes() == historical):
        raise ValueError('historical bytes changed')
    changes = {}
    identities = {}
    members = ({"agent": "tools/fleet/upgrade_client.py", "coordinator": "tools/fleet/publish_runtime.py"}
               if intent else {name: "src/prismabuild/" + name + ".py"
                               for name in ("core", "digest_primitives")})
    for name, member in members.items():
        raw = (root / member).read_bytes()
        identities[name] = dict(bytes=len(raw), sha256=sha(raw))
    for case in sorted(expected):
        old = base64.b64decode(before[case]["raw"], validate=True)
        new = base64.b64decode(current[case]["raw"], validate=True)
        if not (sha(old) == before[case]['sha256'] and sha(new) == current[case]['sha256']):
            raise ValueError("capture integrity check failed: sha(old) == before[case]['sha256'] and sha(new) == current[case]['sha256']")
        changes[case] = differences(json.loads(old), json.loads(new))
        if intent:
            if not ({row['path'] for row in changes[case]} <= {"/agent_sha256", "/coordinator_sha256"}):
                raise ValueError('upgrade intent changed outside source identities')
            body = json.loads(new)
            for name, identity in identities.items():
                if body[name + "_sha256"] != identity["sha256"]:
                    raise ValueError('upgrade intent does not bind actual selected source bytes')
            if not changes[case]:
                raise ValueError('capture did not change this current-source golden')
            continue
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
    label = "upgrade-intent" if intent else "runtime"
    archive = data / "history-controls" / ("superseded-" + label + "-" + sha(old_raw)[:16])
    if not (not archive.exists()):
        raise ValueError('old current bytes already archived; inspect partial adoption')
    if args.report.exists():
        raise ValueError('adoption report already exists')
    with tempfile.TemporaryDirectory(prefix="adopt-runtime-") as tmp:
        staged = Path(tmp)
        subprocess.run([sys.executable, str(root / "tests/compat/support/adopt_compat_capture.py"),
                        "--capture", str(capture), "--destination", str(staged),
                        "--suffix", suffix, "--primary-audit", str(args.primary_audit)], check=True)
        if not ((staged / f'historical-{suffix}.jsonl').read_bytes() == historical):
            raise ValueError("capture integrity check failed: (staged / 'historical-receipt.jsonl').read_bytes() == (data / 'historical-receipt.jsonl').read_bytes()")
        if not (not (staged / f'absent-participants-{suffix}.jsonl').exists()):
            raise ValueError("capture integrity check failed: not (staged / 'absent-participants-receipt.jsonl').exists()")
        archive.mkdir(parents=True)
        (archive / prior.name).write_bytes(old_raw)
        reason = ("Only actual updater/coordinator source identities changed" if intent else
                  "Only attested Core/digest source identities and dependent self-digests changed")
        provenance = dict(reason=reason + "; actual writer bytes independently reproduced and unchanged retained consumers accepted both directions.",
                          sha256=sha(old_raw), records=len(rows(prior)), selected_cases=sorted(expected),
                          credit="superseded current-source fixture; no supported historical-generation credit",
                          previous_current_commit=subprocess.check_output(
                              ["git", "-C", str(root), "log", "-1", "--format=%H", "--", str(prior.relative_to(root))],
                              text=True).strip(),
                          historical_sha256=sha(historical_path.read_bytes()),
                          capture_source_parent=producer, differences=changes)
        old_manifest = data / f"capture-{suffix}.json"
        if old_manifest.exists():
            shutil.copyfile(old_manifest, archive / old_manifest.name)
        (archive / "provenance.json").write_text(sorted_json(provenance) + "\n")
        for path in archive.iterdir():
            path.chmod(0o444)
        adopted = {row["case"]: line for line in (staged / prior.name).read_bytes().splitlines(keepends=True)
                   for row in [json.loads(line)]}
        replacement = b"".join(adopted.get(json.loads(line)["case"], line)
                               for line in old_raw.splitlines(keepends=True))
        (staged / prior.name).chmod(0o644)
        (staged / prior.name).write_bytes(replacement)
        manifest_path = staged / f"capture-{suffix}.json"
        adoption = json.loads(manifest_path.read_bytes())
        adoption["retained_current_cases"] = [row["case"] for row in rows(prior)
                                             if row["case"] not in expected]
        adoption["superseded_current_archive"] = str(archive.relative_to(root))
        adoption["files"] = {prior.name: sha(replacement),
                             historical_path.name: sha(historical_path.read_bytes())}
        manifest_path.chmod(0o644)
        manifest_path.write_text(sorted_json(adoption) + "\n")
        for name in (prior.name, f"capture-{suffix}.json"):
            target = data / name
            if target.exists():
                target.chmod(0o644)
            shutil.copyfile(staged / name, target)
            target.chmod(0o444)
    result = dict(cases=len(expected), historical_rows_byte_exact=len(historical.splitlines()), source_identities=identities,
                  archive=str(archive.relative_to(root)), capture_source_parent=producer,
                  current_goldens_sha256=sha(prior.read_bytes()), differences=changes)
    if helper_identity is not None:
        result['immutable_helper_bound_by_core'] = helper_identity
    with args.report.open("x") as stream:
        stream.write(sorted_json(result) + "\n")
    print(json.dumps({key: result[key] for key in ("cases", "historical_rows_byte_exact", "archive", "current_goldens_sha256")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--profile", choices=("runtime", "upgrade-intent"), default="runtime")
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--primary-audit", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    refresh(parser.parse_args())


if __name__ == "__main__":
    main()
