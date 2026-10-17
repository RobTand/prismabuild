"""Receipt capture owns shared structure without granting filesystem authority."""
import json
from pathlib import Path

import pytest

from prismabuild.generation_receipt import GenerationReceipt, SourceDeclarations
from prismabuild.digest_primitives import raw_sha256
from generation_fixture import generation_receipt


@pytest.mark.parametrize("commit", ["a" * 40, int("1" * 40)])
def test_shared_valid_fixture_reproduces_original_bytes_and_field_order(commit):
    files = {"tools/a.py": "b" * 64, "src/unicode_\u03bb.py": "c" * 64}
    original = {"schema": "prismaquant.prismabuild.runtime_version.v1",
                "generation": "g", "commit": commit, "files": files}
    built = generation_receipt("g", commit, files)
    assert json.dumps(built).encode() == json.dumps(original).encode()
    assert list(built) == list(original)
    assert built["files"] is files


def test_capture_and_construction_are_pure_and_member_reads_are_selected(tmp_path, monkeypatch):
    data = b"chosen\n"
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps({"schema": "prismaquant.prismabuild.runtime_version.v1",
                               "generation": "g", "commit": "a" * 40,
                               "files": {"chosen": raw_sha256(data), "unselected": False}}))
    reads = []
    original = Path.read_text

    def record_read(self, *args, **kwargs):
        reads.append(self)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", record_read)
    receipt = GenerationReceipt.read(path)
    path.unlink()
    receipt.require_generation("g")
    assert receipt.require_commit() == "a" * 40
    assert receipt.matches_bytes("chosen", data)
    assert not receipt.matches_bytes("chosen", b"changed")
    assert reads == [path]
    assert GenerationReceipt({"metadata": True}).document == {"metadata": True}
    assert not (tmp_path / "unselected").exists()
    with pytest.raises(ValueError, match="no sha256"):
        receipt.member_digest("unselected")


def test_retained_wrapper_question_does_not_add_a_commit_requirement():
    receipt = GenerationReceipt.capture(json.dumps({"schema": "prismaquant.prismabuild.runtime_version.v1",
                                                   "generation": "g", "files": {}}))
    assert receipt.require_generation("g")["files"] == {}
    with pytest.raises(ValueError, match="invalid generation receipt"):
        receipt.require_commit()


@pytest.mark.parametrize("commit", ["a" * 40, int("1" * 40)])
def test_commit_question_preserves_the_existing_integrity_readers_coercion(commit):
    assert GenerationReceipt({"commit": commit}).require_commit() == commit


@pytest.mark.parametrize("document", [None, [], {}, {"schema": "wrong"},
    {"schema": "prismaquant.prismabuild.runtime_version.v1", "generation": "g", "files": []}])
def test_generation_question_rejects_malformed_structure(document):
    with pytest.raises(ValueError, match="invalid generation receipt"):
        GenerationReceipt(document).require_generation("g")


@pytest.mark.parametrize("name", ["", "/outside", "../outside", "a/../../outside"])
def test_member_paths_cannot_escape_the_generation(tmp_path, name):
    with pytest.raises(ValueError, match="unsafe generation member path"):
        GenerationReceipt.member_path(tmp_path, name)


@pytest.mark.parametrize("source", [
    b"P = 1\nP = 2\n", b"if True:\n P = 1\n", b"P = int('1')\n",
    b"P = True\n", b"P = '1'\n", b"P = 0\n", b"P = -1\n", b"P = 1\nP += 1\n",
    b"P = 1\ndef f():\n P = 2\n", b"P = 1\ndef P(): pass\n",
    b"P = 1\ndel P\n", b"P = 1\nfrom other import P\n",
    b"P = 1\ndef f(P): pass\n", b"P = 1\ntry: pass\nexcept Exception as P: pass\n",
    b"this is invalid !\n",
])
def test_ambiguous_or_invalid_protocol_declarations_fail_closed(source):
    with pytest.raises(ValueError):
        SourceDeclarations.capture(source).protocol("P")


@pytest.mark.parametrize("source", [b"P = 1\n", b"P: int = 1\n", b"P=1 # a declaration\n"])
def test_protocol_declarations_are_concrete_module_literals(source):
    assert SourceDeclarations.capture(source).protocol("P") == 1


def test_capability_discovery_never_executes_target_source(tmp_path):
    marker = tmp_path / "executed"
    source = f"P = 1\nopen({str(marker)!r}, 'w').write('executed')\n".encode()
    assert SourceDeclarations.capture(source).protocol("P") == 1
    assert not marker.exists()
    assert SourceDeclarations.capture(b"# P = 1\nTEXT = 'P = 1'\n").protocol("P") is None
