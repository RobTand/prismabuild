"""Actual queue protocol writers and supported-generation consumers."""
import pytest

from record_compat_contract import RecordContract, generations, writer_sources, pytest_generate_tests


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "queue"


@pytest.mark.parametrize("case", ["q-item/plain", "q-attempt/executed", "q-outcome/executed"])
def test_submission_phase_fields_are_additive_for_retained_readers(case, generations, tmp_path):
    import json
    from record_compat import accepted, invoke
    from record_compat_contract import ROOT, GOLDENS, decoded
    original = decoded(GOLDENS[case])
    record = json.loads(original)
    record["phases"] = {"cas_stage": 1.25, "queue_publication": 2.0,
                        "transition_lock_wait": 15.5}
    current = json.dumps(record).encode()
    for generation, old, manifest in generations:
        # Old readers accept the additive diagnostic. New readers continue to
        # accept the original record with no diagnostic field.
        accepted(invoke(old, case, tmp_path / generation / "old", raw=current, manifest=manifest))
        accepted(invoke(ROOT, case, tmp_path / generation / "new", raw=original))


def test_timed_supersession_preserves_both_reader_directions(tmp_path):
    import json
    import subprocess
    import tarfile
    from prismabuild import pool
    from record_compat import accepted, invoke, write
    from record_compat_contract import ROOT, GOLDENS, decoded
    case = "publication-ending/initial"
    old_raw = decoded(GOLDENS[case])
    reference = json.loads(old_raw)
    queue = pool.PoolQueue(tmp_path / "timed-writer")
    original = {"action_key": reference["action_key"],
                "published_unix": reference["published_unix"],
                "phases": {"queue_publication": 2.0, "transition_lock_wait": 15.5}}
    ending = queue._persist_publication_supersession(original, successor=reference["successor"])
    current = ending.path.read_bytes()
    assert set(json.loads(current)) == set(reference)
    assert current == old_raw
    # The retained archive generations predate publication endings. Exercise
    # the actual main decoder from the issue's pinned base instead.
    archive_path = tmp_path / "main-source.tar"
    with archive_path.open("wb") as stream:
        subprocess.run(["git", "archive", "48c47c0540483461201dc8b5f8996976390fdd9d",
                        "src/prismabuild", "tools/fleet"], cwd=ROOT, stdout=stream, check=True)
    old = tmp_path / "main-source"
    with tarfile.open(archive_path) as archive:
        archive.extractall(old, filter="data")
    prior = write(old, case, tmp_path / "old-writer")
    assert prior == old_raw
    accepted(invoke(old, case, tmp_path / "old-reader", raw=current))
    accepted(invoke(ROOT, case, tmp_path / "new-reader", raw=prior))
