"""Explicit wrapper provenance and immutable staging across submitting boxes."""
import hashlib
from pathlib import Path
import sys
import types
from concurrent.futures import ThreadPoolExecutor

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import dispatch_tessera_ladder as ladder


@pytest.fixture
def dispatch(tmp_path, monkeypatch):
    checkout = tmp_path / "checkout"
    encoder = checkout / "tessera" / "encoder.py"
    encoder.parent.mkdir(parents=True)
    encoder.write_text("ENCODER = 1\n")
    monkeypatch.setattr(ladder, "CHECKOUT", checkout)
    monkeypatch.setattr(ladder, "PYTHON", sys.executable)
    monkeypatch.setattr(ladder, "SH", tmp_path / "fleet")
    actions = []
    def submit(action, **kwargs):
        # At the transport boundary every wrapper entry is actually readable
        # and matches the closure sealed by the producer.
        closure = ladder.pb.build_code_closure(
            Path(kwargs["checkout_root"]),
            [entry["path"] for entry in action["code_closure"]["files"]])
        assert closure == action["code_closure"]
        actions.append(action)
        return types.SimpleNamespace(action_key=action["action_key"],
                                     describe=lambda: "queued")
    monkeypatch.setattr(ladder.fleet_submit, "submit", submit)
    def run(source=None, dry_run=False):
        argv = ["ladder", "--shards", "1", "--transport", "pool"]
        if source is not None:
            argv.extend(["--wrapper", str(source)])
        if dry_run:
            argv.append("--dry-run")
        monkeypatch.setattr(sys, "argv", argv)
        return ladder.main()
    return checkout, actions, run


def test_explicit_sources_are_sealed_without_overwriting_each_other(dispatch, tmp_path):
    checkout, actions, run = dispatch
    sources = [tmp_path / "box-a.py", tmp_path / "box-b.py"]
    for index, source in enumerate(sources):
        source.write_text(f"print({index})\n")
        assert run(source) in (None, 0)
    assert len(actions) == 2
    assert actions[0]["action_key"] != actions[1]["action_key"]
    paths = []
    for source, action in zip(sources, actions):
        provenance = action["params"]["wrapper_source"]
        assert provenance["source_path"] == str(source.resolve())
        assert provenance["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
        paths.append(provenance["staged_path"])
        assert action["task"]["argv"][1] == paths[-1]
        assert (checkout / paths[-1]).read_bytes() == source.read_bytes()
    assert paths[0] != paths[1]
    assert not (checkout / ladder.WRAPPER).exists()


def test_default_source_is_the_shared_checkout(dispatch):
    checkout, actions, run = dispatch
    source = checkout / ladder.WRAPPER
    source.write_text("print('shared')\n")
    assert run() in (None, 0)
    assert actions[0]["params"]["wrapper_source"]["source_path"] == str(source)


def test_dry_run_previews_explicit_source_without_staging(dispatch, tmp_path, capsys):
    checkout, actions, run = dispatch
    source = tmp_path / "probe.py"
    source.write_text("print('probe')\n")
    before = sorted(checkout.rglob("*"))
    assert run(source, dry_run=True) in (None, 0)
    preview = capsys.readouterr().out
    assert sorted(checkout.rglob("*")) == before
    assert actions == []
    assert run(source) in (None, 0)
    assert actions[0]["action_key"][:16] in preview


def test_missing_source_is_a_named_refusal(dispatch, tmp_path, capsys):
    checkout, actions, run = dispatch
    assert run(tmp_path / "missing.py") == 1
    assert "ladder wrapper:" in capsys.readouterr().err
    assert actions == []
    assert not (checkout / "prismabuild-wrappers").exists()


def test_corrupt_staged_digest_is_not_silently_replaced(dispatch, tmp_path, capsys):
    checkout, actions, run = dispatch
    source = tmp_path / "probe.py"
    source.write_text("print('probe')\n")
    assert run(source) in (None, 0)
    staged = checkout / actions[0]["params"]["wrapper_source"]["staged_path"]
    staged.write_text("corrupt\n")
    assert run(source) == 1
    assert "digest mismatch" in capsys.readouterr().err
    assert staged.read_text() == "corrupt\n"
    assert len(actions) == 1


def test_concurrent_identical_staging_converges(dispatch, tmp_path):
    checkout, _, _ = dispatch
    source = tmp_path / "probe.py"
    source.write_text("print('probe')\n")
    with ThreadPoolExecutor(max_workers=8) as workers:
        outcomes = list(workers.map(
            lambda _: ladder.prepare_wrapper(source, dry_run=False), range(16)))
    assert all(outcome == outcomes[0] for outcome in outcomes)
    staged = checkout / outcomes[0][0]["staged_path"]
    assert staged.read_bytes() == source.read_bytes()
    assert list(staged.parent.iterdir()) == [staged]
