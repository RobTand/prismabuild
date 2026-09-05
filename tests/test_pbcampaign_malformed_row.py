"""A malformed row must not cost the campaign the keys it already submitted.

``pbcampaign`` submits row by row and prints twelve characters of each key on
stderr as it goes. The full records, which are what ``pbwait`` is given after
``--detach``, are printed only once every row has been attempted. So a
conversion that raised while a later row was being prepared took the earlier
rows' records with it: the work was queued on the fleet and nobody held its
names.

Both halves are checked here. A manifest whose field values cannot become
``pbrun`` flags is refused before the first submission, and a preparation
failure that reaches the submission loop anyway is one refused row beside
complete records for the rest.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import pbcampaign  # noqa: E402
import pbrun  # noqa: E402

KEY = "a" * 64


def _manifest(tmp_path: Path, rows) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


class _Recorder:
    """A stand-in ``pbrun.main`` that submits nothing and prints one key."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self) -> int:
        self.calls.append(list(sys.argv))
        print(json.dumps({
            "status": "submitted", "action_key": KEY, "transport": "slurm",
            "published_unix": 100.0 + len(self.calls),
        }))
        return 0


def _campaign(argv, recorder, monkeypatch: pytest.MonkeyPatch):
    """Run ``main`` with ``pbrun`` replaced, and return what it printed."""

    monkeypatch.setattr(pbrun, "main", recorder)
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = pbcampaign.main(argv)
        except SystemExit as raised:
            # A ``SystemExit`` carrying a string is how this tool refuses. The
            # interpreter prints that string to stderr and exits 1, so both
            # halves are reproduced here rather than only the object.
            code = raised.code
            if not isinstance(code, int):
                err.write(f"{code}\n")
                code = 1
    return code, out.getvalue(), err.getvalue()


def test_a_word_where_a_count_belongs_is_refused_before_the_first_submission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The manifest's contract is that it is refused whole, or spent whole.

    main: ``load_manifest`` names the row and the field, and no row is
    submitted.
    branch: the refusal reaches the operator as ``pbcampaign``'s own message
    and exit 1, not as a traceback.
    """

    manifest = _manifest(tmp_path, [
        {"argv": ["/bin/true"]},
        {"argv": ["/bin/true"], "demand": {"cpu": "eight"}},
        {"argv": ["/bin/true"]},
    ])

    with pytest.raises(pbcampaign.ManifestError) as refused:
        pbcampaign.load_manifest(manifest, transport="slurm")
    assert "row 1" in str(refused.value)
    assert "demand['cpu']" in str(refused.value)
    assert "'eight'" in str(refused.value)

    recorder = _Recorder()
    code, out, err = _campaign(
        ["--transport", "slurm", "--detach", str(manifest)], recorder,
        monkeypatch)

    assert recorder.calls == []
    assert code == 1
    assert "row 1" in err
    assert out == ""


def test_every_other_field_shape_is_refused_at_load_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same conversion fault wears several field names.

    branch: ``env`` that is not an object, an ``env`` value that is not text,
    and a bare string where a list of tags belongs. The last one is the
    quietest: a string iterates one character at a time, so ``"tags": "x86"``
    would seal three tags rather than refuse.
    """

    for row, named in (
        ({"argv": ["/bin/true"], "env": "PYTHONPATH=src"}, "env"),
        ({"argv": ["/bin/true"], "env": {"PYTHONPATH": ["src"]}}, "env"),
        ({"argv": ["/bin/true"], "tags": "x86"}, "tags"),
        ({"argv": ["/bin/true"], "demand": {"cpu": 1.5}}, "demand"),
        ({"argv": ["/bin/true"], "timeout_s": "soon"}, "timeout_s"),
        ({"argv": ["/bin/true"], "deterministic": "no"}, "deterministic"),
        ({"argv": ["/bin/true"], "priority": "high"}, "priority"),
        ({"argv": ["/bin/true"], "cwd": ["/home/rob"]}, "cwd"),
    ):
        manifest = _manifest(tmp_path, [{"argv": ["/bin/true"]}, row])
        with pytest.raises(pbcampaign.ManifestError) as refused:
            pbcampaign.load_manifest(manifest, transport="slurm")
        assert "row 1" in str(refused.value), row
        assert named in str(refused.value), row

        recorder = _Recorder()
        code, out, err = _campaign(
            ["--transport", "slurm", "--detach", str(manifest)], recorder,
            monkeypatch)
        assert recorder.calls == [], row
        assert code == 1
        assert out == ""


def test_a_preparation_failure_in_the_loop_is_one_refused_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A conversion nothing validates yet must still cost only its own row.

    main: the campaign submits every other row and reports the bad one as
    refused.
    branch: ``--detach`` prints the complete record of each submitted row, so
    the keys of work that is now queued reach the operator.
    """

    manifest = _manifest(tmp_path, [
        {"argv": ["/bin/true"]},
        {"argv": ["/bin/false"]},
        {"argv": ["/bin/true"]},
    ])
    built = pbcampaign.pbrun_argv

    def _fails_on_the_middle_row(row):
        if row["argv"] == ["/bin/false"]:
            raise ValueError("invalid literal for int() with base 10: 'eight'")
        return built(row)

    monkeypatch.setattr(pbcampaign, "pbrun_argv", _fails_on_the_middle_row)

    recorder = _Recorder()
    code, out, err = _campaign(
        ["--transport", "slurm", "--detach", str(manifest)], recorder,
        monkeypatch)

    assert len(recorder.calls) == 2
    assert code == 1
    printed = [json.loads(line) for line in out.splitlines() if line.strip()]
    assert len(printed) == 2
    for record in printed:
        assert record["action_key"] == KEY
        assert record["status"] == "submitted"
        assert record["transport"] == "slurm"
        assert record["published_unix"] in {101.0, 102.0}
    assert "row 1 refused" in err
    assert "invalid literal for int()" in err
