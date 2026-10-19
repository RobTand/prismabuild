"""Exact historical bridge bytes and current installed-reader fixture bindings."""
from io import BytesIO
import json
from pathlib import Path
import tarfile

from prismabuild.digest_primitives import raw_sha256


ROOT = Path(__file__).resolve().parents[1]
RECEIPT_LEAVES = {
    "digest_primitives.py": "src/prismabuild/digest_primitives.py",
    "generation_receipt.py": "src/prismabuild/generation_receipt.py",
}


def receipt_source_artifact(name):
    metadata = json.loads((ROOT / "tests/fixtures/receipt-readers-r2.tar.json").read_bytes())
    expected = metadata["corpus"][name]
    with tarfile.open(fileobj=BytesIO((ROOT / "tests/fixtures/receipt-readers-r2.tar.gz").read_bytes()),
                      mode="r:gz") as archive:
        with archive.extractfile(name) as stream:
            raw = stream.read(expected["bytes"] + 1)
    assert len(raw) == expected["bytes"] and raw_sha256(raw) == expected["sha256"]
    return raw


def prepared_receipt_bridge():
    return receipt_source_artifact("standalone-receipt-bridge.py")


def bind_current_receipt_reader(publication):
    generation, install, _, _, _, _ = publication
    path = generation / "tools/upgrade_client.py"
    path.write_bytes((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    receipt_path = generation / "RUNTIME_VERSION.json"
    receipt = json.loads(receipt_path.read_bytes())
    receipt["files"]["tools/upgrade_client.py"] = raw_sha256(path.read_bytes())
    for name, member in RECEIPT_LEAVES.items():
        raw = (ROOT / member).read_bytes()
        target = generation / member
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        receipt["files"][member] = raw_sha256(raw)
        (install / name).write_bytes(raw)
    receipt_path.write_text(json.dumps(receipt))
    return publication


def publish_updater(publication, source):
    generation = publication[0]
    target = generation / 'tools/upgrade_client.py'
    target.write_bytes(source)
    path = generation / 'RUNTIME_VERSION.json'
    receipt = json.loads(path.read_bytes())
    receipt['files']['tools/upgrade_client.py'] = raw_sha256(source)
    path.write_text(json.dumps(receipt))


def publish_receipt_closure(publication):
    generation = publication[0]
    path = generation / 'RUNTIME_VERSION.json'
    receipt = json.loads(path.read_bytes())
    for member in RECEIPT_LEAVES.values():
        target = generation / member
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = (ROOT / member).read_bytes()
        target.write_bytes(raw)
        receipt['files'][member] = raw_sha256(raw)
    path.write_text(json.dumps(receipt))


def remove_receipt_closure(publication):
    generation = publication[0]
    path = generation / 'RUNTIME_VERSION.json'
    receipt = json.loads(path.read_bytes())
    for member in RECEIPT_LEAVES.values():
        receipt['files'].pop(member, None)
        (generation / member).unlink(missing_ok=True)
    path.write_text(json.dumps(receipt))
