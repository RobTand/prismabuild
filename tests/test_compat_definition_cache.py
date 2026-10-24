"""Share source indexes by content while observing changes at the same path."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
import compat_references as references


def test_identical_source_is_indexed_once_and_changed_source_reselects_the_owner(tmp_path, monkeypatch):
    current = "prismabuild.submission:parse_progress_phases"
    prior = "pbrun:parse_progress_phases"
    body = b'def parse_progress_phases():\n    return "controller-content-index-fixture"\n'
    roots = [tmp_path / name for name in ("first", "second")]
    for root in roots:
        source = root / "src/prismabuild/submission.py"
        source.parent.mkdir(parents=True)
        source.write_bytes(body)
        fallback = root / "tools/pbrun.py"
        fallback.parent.mkdir(parents=True)
        fallback.write_bytes(b"def parse_progress_phases():\n    return None\n")
    parsed = []
    parse = references.ast.parse

    def observe_parse(source, *args, **kwargs):
        if source == body:
            parsed.append(source)
        return parse(source, *args, **kwargs)

    monkeypatch.setattr(references.ast, "parse", observe_parse)
    assert references.generation_reference(roots[0], current) == current
    assert references.generation_reference(roots[1], current) == current
    assert len(parsed) == 1
    (roots[1] / "src/prismabuild/submission.py").write_bytes(b"# owner moved out of this module\n")
    assert references.generation_reference(roots[1], current) == prior
    assert references.generation_reference(roots[0], current) == current


def test_session_closures_are_extracted_once_and_removed_at_configuration_cleanup(monkeypatch):
    from contextlib import ExitStack
    import json
    from types import SimpleNamespace

    import pytest
    import record_compat
    from compat_session import retained_for_session

    directory = ROOT / "tests/compat/generations"
    window = json.loads((directory / "window.json").read_bytes())
    prepared = json.loads((directory / "prepared.json").read_bytes())
    expected_extractions = len(window["generations"]) + len(prepared)
    opened = []
    open_archive = record_compat.tarfile.open

    def observe_archive(*args, **kwargs):
        opened.append(args[0])
        return open_archive(*args, **kwargs)

    monkeypatch.setattr(record_compat.tarfile, "open", observe_archive)
    with ExitStack() as cleanup:
        config = SimpleNamespace(stash=pytest.Stash(), add_cleanup=cleanup.callback)
        retained_for_session(config, directory)
        sources = retained_for_session(config, directory)
        assert len(opened) == expected_extractions
        closure = sources[0][1]
    assert not closure.exists()
