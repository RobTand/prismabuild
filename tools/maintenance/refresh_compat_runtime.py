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


def rows(path, *, binary=False):
    raw = path.read_bytes() if binary else path.read_text()
    return [json.loads(line) for line in raw.splitlines()]


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


CONTROLLER_PROFILES = {
    "upgrade-attestation": ("rollout", frozenset({"upgrade-attestation/first", "upgrade-attestation/repeated"})),
    "qualification-receipt": ("qualification-ram-telemetry", frozenset({"runtime-receipt/qualification"})),
}
QUALIFICATION_FIXTURE = b"# private qualification wire fixture member\n"


def wire(row):
    raw = base64.b64decode(row["raw"], validate=True)
    if sha(raw) != row["sha256"]:
        raise ValueError("capture row SHA disagrees with its exact writer bytes")
    return raw


def source_members(root):
    raw = (root / "tools/fleet/upgrade_client.py").read_bytes()
    selected = {"MEMBERS", "OPTIONAL_MEMBERS"}
    declarations = {target.id: ast.literal_eval(node.value)
                    for node in ast.parse(raw).body if isinstance(node, ast.Assign)
                    for target in node.targets if isinstance(target, ast.Name) and target.id in selected}
    if declarations.keys() != selected:
        raise ValueError("updater source must declare both exact member maps")
    return declarations


def validate_delta(profile, old_row, new_row, root, declarations):
    old, new = wire(old_row), wire(new_row)
    if old == new:
        raise ValueError("capture did not change this current golden")
    before, after = json.loads(old), json.loads(new)
    if profile == "upgrade-attestation":
        updater = (root / "tools/fleet/upgrade_client.py").read_bytes()
        expected = {**before, "client_sha256": sha(updater)}
        if after != expected or before["client_sha256"] == expected["client_sha256"]:
            raise ValueError("attestation changed outside the actual updater source SHA")
        return {"field": "/client_sha256", "before": before["client_sha256"], "after": sha(updater)}
    member = declarations["OPTIONAL_MEMBERS"]["park_proof.py"]
    required = set(declarations["MEMBERS"].values()) | {
        "tools/fleet/publish_runtime.py", "tools/fleet/fleet_boxes.json"}
    if len(required) != 6 or len(before["files"]) != 6 or set(before["files"]) != required or member in required:
        raise ValueError("qualification baseline must have exactly the original six members")
    expected = {**before, "files": {**before["files"], member: sha(QUALIFICATION_FIXTURE)}}
    if after != expected or len(after["files"]) != 7 or set(after["files"]) != required | {member}:
        raise ValueError("qualification changed outside the required park fixture member")
    return {"field": "/files/" + member, "change": "added", "sha256": sha(QUALIFICATION_FIXTURE),
            "credit": "private supplied fixture bytes; no execution or installation of this member"}


def _refresh_controller(args):
    root, capture = args.root.resolve(), args.capture.resolve()
    data = root / "tests/compat"
    suffix, expected = CONTROLLER_PROFILES[args.profile]
    report = json.loads((capture / "qualification.json").read_bytes())
    if (report.get("passed") is not True or report.get("failures") != []
            or report.get("independent_writer_reproduction_required") is not True):
        raise ValueError("capture did not pass complete independent reproduction and both reader directions")
    parents = report["source_git"]["parents"]
    if len(parents) != 1:
        raise ValueError("capture must bind exactly one frozen producer parent")
    producer = parents[0]
    drift = subprocess.check_output(
        ["git", "-C", str(root), "diff", "--name-only", producer, "--",
         "src", "tools", "tests/compat/support"], text=True).splitlines()
    if drift:
        raise ValueError(("capture production, tool or support source differs", drift))
    if set(report["selected_cases"]) != expected or len(report["selected_cases"]) != len(expected):
        raise ValueError("capture profile must select exactly its declared current cases")
    prior, historical_path = data / f"goldens-{suffix}.jsonl", data / f"historical-{suffix}.jsonl"
    old_raw = prior.read_bytes()
    before_rows = [row for row in rows(prior, binary=True) if row["case"] in expected]
    before = {row["case"]: row for row in before_rows}
    if len(before_rows) != len(expected):
        raise ValueError("duplicate or missing selected baseline row")
    current_rows = rows(capture / "goldens.jsonl", binary=True)
    current = {row["case"]: row for row in current_rows}
    if set(before) != expected or set(current) != expected or len(current_rows) != len(expected):
        raise ValueError("capture and baseline must contain exactly one row for every selected case")
    historical = b"".join(line for line in historical_path.read_bytes().splitlines(keepends=True)
                           if json.loads(line)["case"] in expected)
    if (capture / "historical.jsonl").read_bytes() != historical:
        raise ValueError("historical captures changed; this operation may refresh current rows only")
    if (capture / "absent-participants.jsonl").read_bytes():
        raise ValueError("these profiles require actual retained writers and both real readers, not absences")
    declarations = source_members(root)
    changes = {case: validate_delta(args.profile, before[case], current[case], root, declarations)
               for case in sorted(expected)}
    if args.profile == "upgrade-attestation" and len({wire(row) for row in current.values()}) != 1:
        raise ValueError("first and repeated attestations must emit the same exact current bytes")
    archive = data / "history-controls" / ("superseded-" + args.profile + "-" + sha(old_raw)[:16])
    if archive.exists():
        raise ValueError("old current bytes already archived; inspect partial adoption")
    if args.report.exists():
        raise ValueError("adoption report already exists")
    old_manifest = data / f"capture-{suffix}.json"
    if not old_manifest.is_file():
        raise ValueError("full original capture metadata must exist for preservation")
    old_manifest_raw = old_manifest.read_bytes()
    baseline_head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    baseline_git = {"head": baseline_head, "files": {}}
    pre_profile = {}
    for path, raw in ((prior, old_raw), (old_manifest, old_manifest_raw)):
        relative = str(path.relative_to(root))
        committed = subprocess.check_output(["git", "-C", str(root), "show", baseline_head + ":" + relative])
        baseline_git["files"][relative] = {"sha256": sha(committed), "bytes": len(committed)}
        pre_profile[relative] = {"sha256": sha(raw), "bytes": len(raw), "matches_baseline_git": raw == committed}
    preceding = None
    if args.profile == "upgrade-attestation":
        if args.preceding_adoption_report is None:
            raise ValueError("attestation profile requires the preceding verified intent adoption report")
        previous_raw = args.preceding_adoption_report.read_bytes()
        previous = json.loads(previous_raw)
        current_manifest = json.loads(old_manifest_raw)
        if (previous.get("cases") != 1 or previous.get("capture_source_parent") != producer
                or previous.get("current_goldens_sha256") != sha(old_raw)
                or set(current_manifest.get("selected_cases", ())) != {"upgrade-intent/armed"}
                or current_manifest.get("source_git", {}).get("parents") != [producer]
                or current_manifest.get("files", {}).get(prior.name) != sha(old_raw)):
            raise ValueError("preceding intent adoption does not bind the exact pre-profile file and producer")
        verified = current_manifest["verified_producer"]
        primaries = [row for row in json.loads(args.primary_audit.read_bytes())
                     if row["action_key"] == verified["action_key"]]
        if (len(primaries) != 1 or primaries[0]["returncode"] != 0
                or not primaries[0]["cleanup_complete"] or not primaries[0]["source_git"]["git_objects_verified"]
                or any(primaries[0][key] != verified[key] for key in
                       ("action_key", "request_sha256", "receipt_sha256", "source_parent"))
                or verified["source_parent"] != producer):
            raise ValueError("preceding intent adoption has no exact verified primary in this action's audit input")
        preceding = {"report_path": str(args.preceding_adoption_report), "report_sha256": sha(previous_raw),
                     "report_bytes": len(previous_raw), "capture_source_parent": producer,
                     "current_goldens_sha256": sha(old_raw), "verified_producer": verified,
                     "superseded_current_archive": previous["archive"]}
    elif args.preceding_adoption_report is not None:
        raise ValueError("qualification profile does not consume a preceding rollout adoption report")
    source_paths = ["tools/fleet/upgrade_client.py"]
    if args.profile == "qualification-receipt":
        source_paths += ["tools/fleet/qualify_rollout.py", "tests/compat/support/compat_generation_adapters.py"]
    source_identities = {path: {"bytes": len((root / path).read_bytes()), "sha256": sha((root / path).read_bytes())}
                         for path in source_paths}
    with tempfile.TemporaryDirectory(prefix="adopt-controller-") as temporary:
        staged = Path(temporary)
        # This child runs directly inside the already admitted strict-adopter action.
        subprocess.run([sys.executable, str(root / "tests/compat/support/adopt_compat_capture.py"),
                        "--capture", str(capture), "--destination", str(staged),
                        "--suffix", suffix, "--primary-audit", str(args.primary_audit)], check=True)
        if (staged / historical_path.name).read_bytes() != historical:
            raise ValueError("staged historical bytes changed")
        if (staged / f"absent-participants-{suffix}.jsonl").exists():
            raise ValueError("staged absence cannot replace actual codec observations")
        adopted_rows = rows(staged / prior.name, binary=True)
        if ({row["case"] for row in adopted_rows} != expected or len(adopted_rows) != len(expected)
                or any(wire(row) != wire(current[row["case"]]) for row in adopted_rows)):
            raise ValueError("staged current rows disagree with the exact observed capture")
        adopted = {json.loads(line)["case"]: line for line in (staged / prior.name).read_bytes().splitlines(keepends=True)}
        replacement = b"".join(adopted.get(json.loads(line)["case"], line)
                               for line in old_raw.splitlines(keepends=True))
        retained = [line for line in old_raw.splitlines(keepends=True) if json.loads(line)["case"] not in expected]
        if [line for line in replacement.splitlines(keepends=True) if json.loads(line)["case"] not in expected] != retained:
            raise ValueError("unselected current row bytes changed")
        provenance = {"profile": args.profile, "selected_cases": sorted(expected),
                      "credit": "superseded current fixture; no supported historical-generation credit",
                      "reason": ("Only actual updater source identity changed" if args.profile == "upgrade-attestation"
                                 else "Real current qualification builder requires the supplied park fixture member"),
                      "baseline_git_identity": baseline_git,
                      "exact_pre_profile_working_tree": pre_profile,
                      "preceding_verified_adoption": preceding,
                      "capture_source_parent": producer, "differences": changes,
                      "preserved_files": {prior.name: sha(old_raw), old_manifest.name: sha(old_manifest.read_bytes())},
                      "historical_sha256": sha(historical_path.read_bytes()), "source_identities": source_identities}
        if prior.read_bytes() != old_raw or old_manifest.read_bytes() != old_manifest_raw:
            raise ValueError("pre-profile current files changed before preservation")
        archive.mkdir(parents=True)
        for name, raw in ((prior.name, old_raw), (old_manifest.name, old_manifest_raw)):
            archived = archive / name
            archived.write_bytes(raw)
            readback = archived.read_bytes()
            if readback != raw or sha(readback) != provenance["preserved_files"][name]:
                raise ValueError("archived complete current file failed readback before replacement")
        (archive / "provenance.json").write_text(sorted_json(provenance) + "\n")
        for path in archive.iterdir():
            path.chmod(0o444)
        adoption = json.loads((staged / old_manifest.name).read_bytes())
        adoption["retained_current_cases"] = [row["case"] for row in rows(prior, binary=True) if row["case"] not in expected]
        adoption["superseded_current_archive"] = str(archive.relative_to(root))
        adoption["files"] = {prior.name: sha(replacement), historical_path.name: sha(historical_path.read_bytes())}
        for path, raw in ((prior, replacement), (old_manifest, (sorted_json(adoption) + "\n").encode())):
            path.chmod(0o644)
            path.write_bytes(raw)
            path.chmod(0o444)
    result = {"profile": args.profile, "cases": len(expected), "capture_source_parent": producer,
              "archive": str(archive.relative_to(root)), "source_identities": source_identities,
              "historical_rows_byte_exact": len(historical.splitlines()), "differences": changes,
              "unselected_current_rows_byte_exact": len(retained), "current_goldens_sha256": sha(prior.read_bytes()),
              "baseline_git_identity": baseline_git, "exact_pre_profile_working_tree": pre_profile,
              "preceding_verified_adoption": preceding, "archive_readback_verified_before_replacement": True}
    with args.report.open("x") as stream:
        stream.write(sorted_json(result) + "\n")
    print(sorted_json(result))


def refresh(args):
    if args.profile in CONTROLLER_PROFILES:
        return _refresh_controller(args)
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
