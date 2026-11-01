"""Current replay writers keep bundle caches inside their private stores."""
import pytest

from record_compat_contract import GOLDENS, ROOT, decoded
from record_compat import write


@pytest.mark.parametrize("case", (
    "checkout-snapshot/clean", "checkout-snapshot/dirty", "checkout-recovery-plan/banked",
))
def test_replay_uses_its_own_sealed_bundle_cache(tmp_path, monkeypatch, case):
    # The nightly workflow supplies these values; Git environment identity
    # overrides the temporary repository's configured fixture identity.
    for role in ("AUTHOR", "COMMITTER"):
        monkeypatch.setenv(f"GIT_{role}_NAME", "PrismaBuild CI")
        monkeypatch.setenv(f"GIT_{role}_EMAIL", "ci@example.invalid")
        monkeypatch.setenv(f"GIT_{role}_DATE", "2042-03-04T05:06:07+00:00")
    parent_cache = tmp_path / "parent-cache"
    monkeypatch.setenv("PRISMABUILD_BUNDLE_CACHE_ROOT", str(parent_cache))
    store = tmp_path / "replay"
    assert write(ROOT, case, store) == decoded(GOLDENS[case])
    assert (store / "sealed-bundles").is_dir()
    assert not parent_cache.exists()
