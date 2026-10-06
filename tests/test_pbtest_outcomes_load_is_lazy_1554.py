"""pbtest loads its recorder on first use, from its own generation (#1554).

``pbtest.py`` used to ``exec_module`` its sibling ``pbtest_outcomes.py`` at
import time, which the import scanner rejects (a tool must not run code when
it is imported).  The recorder is now read on first use through an object that
stands where the module stood.  The rule that motivated the original load
stays: a pbtest test can itself run under an older sealed recorder, whose
``sys.modules`` entry must not be mistaken for the sibling this source seals.

Each case runs in a fresh interpreter, because another test may already have
imported ``pbtest`` into this process.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

FLEET = Path(__file__).resolve().parents[1] / "tools" / "fleet"


def _run(program: str) -> dict:
    completed = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True,
        timeout=120, cwd=str(FLEET.parent))
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


PREAMBLE = f"""
import json, sys, types
sys.path.insert(0, {str(FLEET)!r})
"""


def test_importing_pbtest_does_not_load_the_recorder():
    result = _run(PREAMBLE + """
import pbtest
before = pbtest._OutcomesOwner._module is None
prefix = pbtest.pbtest_outcomes.PREFIX
after = pbtest._OutcomesOwner._module
print(json.dumps({"lazy_before_use": before, "loaded_after_use": after is not None,
                  "prefix": prefix, "file": after.__file__}))
""")
    assert result["lazy_before_use"] is True
    assert result["loaded_after_use"] is True
    assert result["prefix"] == "pbtest-outcomes: "
    assert Path(result["file"]) == FLEET / "pbtest_outcomes.py"


def test_an_older_outer_recorder_in_sys_modules_is_not_used():
    result = _run(PREAMBLE + """
outer = types.ModuleType("pbtest_outcomes")
outer.OUTER_MARKER = True
outer.PREFIX = "outer-recorder: "
sys.modules["pbtest_outcomes"] = outer
import pbtest
print(json.dumps({"prefix": pbtest.pbtest_outcomes.PREFIX,
                  "marker": getattr(pbtest.pbtest_outcomes, "OUTER_MARKER", None)}))
""")
    assert result["prefix"] == "pbtest-outcomes: ", "the outer shard's recorder was read"
    assert result["marker"] is None


def test_the_owner_reads_patches_and_deletes_the_one_sibling_module():
    result = _run(PREAMBLE + """
import pbtest
owner = pbtest.pbtest_outcomes
owner.MARK = 7
seen = pbtest._OutcomesOwner._module.MARK
del owner.MARK
print(json.dumps({"seen": seen,
                  "gone": not hasattr(pbtest._OutcomesOwner._module, "MARK"),
                  "same": owner.parse is pbtest._OutcomesOwner._module.parse}))
""")
    assert result == {"seen": 7, "gone": True, "same": True}


def test_a_generation_switch_after_import_does_not_change_the_recorder(tmp_path):
    """The recorder is the generation pbtest was imported from (#1554 review).

    The published tools path is a link a publication repoints.  Import pbtest
    through a link to this generation, repoint the link at a directory holding
    a different ``pbtest_outcomes.py`` before the recorder is first used, and
    the recorder must still be this generation's.
    """
    other = tmp_path / "generation-b"
    other.mkdir()
    (other / "pbtest_outcomes.py").write_text('PREFIX = "SWITCHED-GENERATION: "\n')
    link = tmp_path / "current"
    link.symlink_to(FLEET)
    flip = tmp_path / "flip"
    flip.symlink_to(other)
    result = _run(f"""
import json, os, sys
sys.path.insert(0, {str(link)!r})
import pbtest
before = pbtest._OutcomesOwner._module is None
os.replace({str(flip)!r}, {str(link)!r})
prefix = pbtest.pbtest_outcomes.PREFIX
print(json.dumps({{"lazy": before, "prefix": prefix,
                  "file": pbtest._OutcomesOwner._module.__file__}}))
""")
    assert result["lazy"] is True
    assert result["prefix"] == "pbtest-outcomes: ", "a later generation's recorder was read"
    assert Path(result["file"]) == FLEET / "pbtest_outcomes.py"
