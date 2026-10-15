"""Runtime comparisons retain complete observations from one receipt read."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import generation_receipt as receipts


REPOSITORY = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(("commit", "generation"), [
    ("test", "first"), (" commit ", "\tgeneration\n"), (" ", " "),
])
def test_complete_nonempty_strings_retain_their_exact_values(tmp_path, commit, generation):
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps({"commit": commit, "generation": generation, "extra": True}))
    assert receipts.observe(path) == receipts.RuntimeIdentity(commit, generation)


@pytest.mark.parametrize("value", [
    {}, {"commit": "known"}, {"generation": "known"},
    {"commit": "", "generation": "known"}, {"commit": "known", "generation": ""},
    {"commit": None, "generation": "known"}, {"commit": "known", "generation": None},
    {"commit": 42, "generation": "known"}, {"commit": "known", "generation": 42},
    {"commit": True, "generation": "known"}, {"commit": "known", "generation": True},
    {"commit": ["known"], "generation": "known"},
    {"commit": "known", "generation": {"name": "known"}},
])
def test_partial_or_nonstring_identity_is_incomplete(tmp_path, value):
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(value))
    assert receipts.observe(path) == receipts.ReceiptUnavailable("incomplete")


@pytest.mark.parametrize("raw", [b"{", b"\xff", b"null", b"[]", b'"receipt"', b"42"])
def test_malformed_receipts_remain_explicitly_unavailable(tmp_path, raw):
    path = tmp_path / "receipt.json"
    path.write_bytes(raw)
    assert receipts.observe(path) == receipts.ReceiptUnavailable("malformed")


def test_missing_and_unreadable_paths_have_distinct_observations(tmp_path):
    assert receipts.observe(tmp_path / "absent.json") == receipts.ReceiptUnavailable("missing")
    assert receipts.observe(tmp_path) == receipts.ReceiptUnavailable("unreadable")


@pytest.mark.parametrize(("loaded", "published", "expected"), [
    (receipts.RuntimeIdentity("commit", "first"), receipts.RuntimeIdentity("commit", "first"),
     receipts.Match(receipts.RuntimeIdentity("commit", "first"))),
    (receipts.RuntimeIdentity("commit", "first"), receipts.RuntimeIdentity("commit", "second"),
     receipts.Drift(receipts.RuntimeIdentity("commit", "first"), receipts.RuntimeIdentity("commit", "second"))),
    (receipts.RuntimeIdentity("first", "generation"), receipts.RuntimeIdentity("second", "generation"),
     receipts.Drift(receipts.RuntimeIdentity("first", "generation"), receipts.RuntimeIdentity("second", "generation"))),
])
def test_known_comparison_carries_its_identity_evidence(loaded, published, expected):
    assert receipts.compare(loaded, published) == expected


@pytest.mark.parametrize("reason", ["missing", "unreadable", "malformed", "incomplete"])
@pytest.mark.parametrize("unknown_side", ["loaded", "published", "both"])
def test_unavailable_observation_never_proves_match_or_drift(reason, unknown_side):
    identity = receipts.RuntimeIdentity("commit", "generation")
    unavailable = receipts.ReceiptUnavailable(reason)
    loaded = identity if unknown_side == "published" else unavailable
    published = identity if unknown_side == "loaded" else unavailable
    assert receipts.compare(loaded, published) == receipts.Unknown(loaded, published)


def test_atomic_publications_cannot_create_an_identity_from_two_reads(tmp_path):
    script = tmp_path / "atomic_observations.py"
    script.write_text(f'''
import json, os, sys, threading
from pathlib import Path
sys.path.insert(0, {str(REPOSITORY / "src")!r})
from prismabuild import generation_receipt as receipts
path = Path({str(tmp_path / "receipt.json")!r})
replacement = path.with_suffix(".next")
ready = threading.Barrier(2, timeout=5)
published = threading.Barrier(2, timeout=5)
values = [{{"commit": "a", "generation": "first"}},
          {{"commit": "b", "generation": "second"}}]
errors = []
reads = []
def writer():
    try:
        for value in values:
            ready.wait()
            replacement.write_text(json.dumps(value))
            os.replace(replacement, path)
            published.wait()
    except BaseException as error:
        errors.append(repr(error))
        ready.abort()
        published.abort()
thread = threading.Thread(target=writer, daemon=True)
thread.start()
def audit(event, arguments):
    if event == "open" and arguments[0] == str(path):
        reads.append(str(path))
        ready.wait()
        published.wait()
sys.addaudithook(audit)
loaded = receipts.RuntimeIdentity("a", "second")
observations = [receipts.observe(path), receipts.observe(path)]
thread.join(5)
assert not thread.is_alive(), "atomic publisher did not finish"
assert errors == [], errors
assert len(reads) == 2, reads
assert observations == [receipts.RuntimeIdentity(**value) for value in values], observations
assert all(isinstance(receipts.compare(loaded, observation), receipts.Drift)
           for observation in observations), observations
print("two atomic publications, two reads, no mixed-pair MATCH")
''')
    child = subprocess.run([sys.executable, "-B", str(script)], capture_output=True,
                           text=True, timeout=15)
    assert child.returncode == 0, child.stdout + child.stderr
    assert child.stdout.strip() == "two atomic publications, two reads, no mixed-pair MATCH"
