"""A recompute's returned receipt describes its execution, not the cache winner."""
from pathlib import Path
import sys

import pytest

from prismabuild import core as pb
from test_core import _action


def test_recompute_returns_its_own_result_and_preserves_canonical(tmp_path, monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, determinism="stochastic", argv=[
        sys.executable, "-c",
        "from pathlib import Path; p=Path('counter'); "
        "n=int(p.read_text())+1 if p.exists() else 1; "
        "p.write_text(str(n)); Path('result.bin').write_text(str(n))",
    ])
    cas_root = tmp_path / "cas"
    first = pb.run_local_action(action, cas_root=cas_root, checkout_root=checkout,
                                recompute=True)
    second = pb.run_local_action(action, cas_root=cas_root, checkout_root=checkout,
                                 recompute=True)
    cas = pb.PrismaBuildCAS(cas_root)
    # Verify using the receipt returned by this execution, not action lookup.
    assert cas.result_path(second["receipt"], action).read_bytes() == b"2"
    assert Path(str(second["payload_path"])).read_bytes() == b"2"
    assert first["receipt"] != second["receipt"]
    assert second["status"] == "execution_result_published"
    receipt = second["receipt"]
    assert isinstance(receipt, dict)
    digest = str(receipt["receipt_sha256"])
    assert cas.lookup_execution(action, digest) == second["receipt"]
    with pytest.raises(FileNotFoundError):
        cas.lookup_execution(action, "f" * 64)
    assert cas.lookup(action) == first["receipt"]
    cached = pb.run_local_action(action, cas_root=cas_root, checkout_root=checkout)
    assert cached["status"] == "cache_hit"
    assert cas.result_path(cached["receipt"], action).read_bytes() == b"1"
    # A valid receipt under the wrong digest may not substitute for this run.
    path = cas._execution_receipt_path(digest)
    path.chmod(0o644)
    path.write_bytes(pb._canonical_file_bytes(first["receipt"]))
    path.chmod(0o444)
    with pytest.raises(pb.CASTamperError, match="requested digest"):
        cas.lookup_execution(action, digest)
