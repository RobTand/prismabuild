"""Authenticate retained SDK5 supplier bytes without installing or replaying work."""

import argparse
import base64
import csv
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import zipfile


SOURCE = "027103d9a8417e06c7f13356e58779a313cd7088"
EXPORT = "2617cf36f98ca20c608cd6003d8960077099a723a7c8f37809caaf0facc5c438"


def identity(raw):
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def audit(supplier, helper, cas, queue):
    receipt_path = cas / "actions/v3" / EXPORT[:2] / (EXPORT + ".json")
    receipt_raw = receipt_path.read_bytes()
    receipt = json.loads(receipt_raw)
    descriptor = receipt["result"]
    result_path = cas / "blobs" / descriptor["sha256"][:2] / descriptor["sha256"]
    result = result_path.read_bytes()
    assert identity(result) == descriptor, "export result differs from CAS receipt"
    manifest = json.loads(result)
    assert (supplier / "export-manifest.json").read_bytes() == result
    done = json.loads((queue / "done" / (EXPORT + ".json")).read_bytes())
    request = json.loads((cas / "requests" / EXPORT[:2] / (EXPORT + ".json")).read_bytes())
    assert receipt["action_key"] == done["action_key"] == request["action_key"] == EXPORT
    assert done["status"] == "executed" and done["detail"]["returncode"] == 0
    assert done["checkout_snapshot"]["parent"] == SOURCE
    assert request["inputs"] == receipt["producer"]["inputs"]
    stdout = done["detail"]["stdout"]
    publication = json.loads(stdout.splitlines()[-1])
    assert publication["receipt"] == receipt
    assert stdout.startswith(result.decode()), "export log/result mismatch"
    custody = manifest["exported_custody"]["files"]
    for name, expected in custody.items():
        assert identity((supplier / name).read_bytes()) == expected, name

    archive = subprocess.check_output(["git", "archive", "--format=tar", SOURCE])
    with tarfile.open(fileobj=io.BytesIO(archive)) as tree:
        source = {member.name: tree.extractfile(member).read()
                  for member in tree.getmembers() if member.isfile()}
    checked = 0
    for name, raw in source.items():
        delivered = helper / name
        assert not delivered.is_symlink(), name
        assert delivered.read_bytes() == raw, name
        checked += 1
    marker = json.loads((helper / ".pinned-source.json").read_bytes())
    assert marker["commit"] == SOURCE
    assert marker["git_archive_sha256"] == identity(archive)["sha256"]
    for name in manifest["source_artifacts"]:
        assert (supplier / ("source-" + name.replace("/", "__"))).read_bytes() == source[name]

    wheel_path = supplier / manifest["wheel"]["name"]
    with zipfile.ZipFile(wheel_path) as wheel:
        names = wheel.namelist()
        assert len(names) == len(set(names)), "duplicate wheel members"
        record_name, = [n for n in names if n.endswith(".dist-info/RECORD")]
        for name, digest, size in csv.reader(io.StringIO(wheel.read(record_name).decode())):
            if not digest:
                assert name == record_name and not size
                continue
            raw = wheel.read(name)
            encoded = base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b"=").decode()
            assert digest == "sha256=" + encoded and int(size) == len(raw), name
        package_names = {n for n in names if n.startswith("prismabuild/")}
        expected_names = {n.removeprefix("src/") for n in source
                          if n.startswith("src/prismabuild/") and n.endswith(".py")}
        assert package_names == expected_names, "wheel package membership differs from source"
        for name in package_names:
            assert wheel.read(name) == source["src/" + name], name
        prefix = record_name.removesuffix("RECORD")
        metadata = wheel.read(prefix + "METADATA").decode()
        tags = wheel.read(prefix + "WHEEL").decode()
        assert "Name: prismabuild\n" in metadata and "Version: 0.1.0\n" in metadata
        assert "Requires-Python: >=3.12\n" in metadata
        assert "Root-Is-Purelib: true\n" in tags and "Tag: py3-none-any\n" in tags

    direct_raw = (supplier / "install-direct_url.json").read_bytes()
    direct = json.loads(direct_raw)
    assert direct["vcs_info"]["vcs"] == "git"
    assert direct["vcs_info"]["commit_id"] == SOURCE and "dir_info" not in direct
    installed_rows = list(csv.reader(io.StringIO((supplier / "install-RECORD").read_text())))
    installed_names = {row[0] for row in installed_rows}
    assert package_names <= installed_names
    verified = 0
    for name, digest, size in installed_rows:
        if name in package_names:
            raw = source["src/" + name]
        elif name.endswith(".dist-info/direct_url.json"):
            raw = direct_raw
        else:
            continue
        encoded = base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b"=").decode()
        assert digest == "sha256=" + encoded and int(size) == len(raw), name
        verified += 1

    return {
        "schema": "prismabuild.pb1485.retained_audit.v1",
        "source_commit": SOURCE,
        "supplier_root": str(supplier),
        "helper_root": str(helper),
        "export_action": EXPORT,
        "export_receipt_raw": identity(receipt_raw),
        "export_receipt_identity": receipt["receipt_sha256"],
        "export_result": descriptor,
        "export_terminal_status": done["status"],
        "export_returncode": done["detail"]["returncode"],
        "custody_files_verified": len(custody),
        "wheel": identity(wheel_path.read_bytes()),
        "wheel_package_files_verified": len(package_names),
        "exported_install_record_rows": len(installed_rows),
        "exported_install_record_rows_verified_against_source_or_metadata": verified,
        "direct_url": direct,
        "helper_tracked_files_verified": checked,
        "source_archive_recomputed": identity(archive),
        "scope": "Static authentication of retained bytes and CAS linkage only. No install, import, producer replay, ARM portability, live helper adoption or GPU qualification.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("supplier", "helper", "cas", "queue"):
        parser.add_argument("--" + name + "-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.supplier_root, args.helper_root, args.cas_root, args.queue_root),
                     indent=2, sort_keys=True))
