"""Current receipt consumers keep their real owner and one source capture."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import sys

import pytest

from prismabuild.compatibility_window import WindowError, client_protocol
import queue_space_support as support
from queue_space_host import host_sample

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from record_compat import accepted, invoke


@pytest.mark.parametrize("case", ["runtime-receipt/implicit", "movement-publication/initial"])
def test_current_generation_consumers_keep_the_receipt_document(case, tmp_path):
    rows = [json.loads(line) for line in (ROOT / "tests/compat/goldens-generations-and-snapshots.jsonl").read_text().splitlines()]
    golden = next(row for row in rows if row["case"] == case)
    result = invoke(ROOT, case, tmp_path / "store", raw=base64.b64decode(golden["raw"]))
    assert accepted(result)["generation"] == "aaaaaaaaaaaa-100-777777777777"


@pytest.mark.parametrize("state", ["inodes", "full"])
def test_a_spool_case_ignores_the_hosts_inodes_and_bytes(state, tmp_path, monkeypatch):
    """The isolated interpreter reads the shared sample, not a low simulated host (#2551)."""
    monkeypatch.setattr(support, "HOST_PRELUDE", support.statvfs_prelude(host_sample(state)))
    assert accepted(invoke(ROOT, "spool-record/reservation", tmp_path / "store"))["raw"]


def test_capability_source_rejects_duplicate_selected_declarations(tmp_path):
    updater = tmp_path / "tools/fleet/upgrade_client.py"
    updater.parent.mkdir(parents=True)
    updater.write_bytes(b"OPTIONAL_MEMBERS = {}\nCLIENT_UPGRADE_PROTOCOL = 2\nCLIENT_UPGRADE_PROTOCOL = 2\n")
    with pytest.raises(WindowError, match="ambiguous capability declaration"):
        client_protocol(tmp_path)


def test_updater_digest_binds_the_one_captured_source_read(tmp_path, monkeypatch):
    updater = tmp_path / "tools/fleet/upgrade_client.py"
    updater.parent.mkdir(parents=True)
    raw = b"OPTIONAL_MEMBERS = {}\nCLIENT_UPGRADE_PROTOCOL = 2\n"
    updater.write_bytes(raw)
    original = Path.read_bytes
    reads = []

    def capture(path):
        if path == updater:
            reads.append(path)
            return raw if len(reads) == 1 else raw + b"# replaced\n"
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", capture)
    observed = client_protocol(tmp_path)
    assert observed["updater_sha256"] == hashlib.sha256(raw).hexdigest()
    assert reads == [updater]
