import pytest

import test_no_tautological_tests as guard


def test_guard_reports_every_planted_tautology(tmp_path, monkeypatch):
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "test_planted.py").write_text(
        "class Planted:\n"
        "    def test_self_comparison(self):\n"
        "        self.assertEqual(x, x)\n"
        "    def test_constant(self):\n"
        "        assert True\n"
        "    def test_swallowed(self):\n"
        "        try:\n"
        "            assert observed == expected\n"
        "        except AssertionError:\n"
        "            pass\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "__file__", str(tmp_path / "test_no_tautological_tests.py"))

    with pytest.raises(AssertionError) as failure:
        guard.test_suite_has_no_tautologies()

    assert str(failure.value) == (
        "Tautological tests (D-32):\n"
        "nested/test_planted.py:3: test_self_comparison: self_compare\n"
        "nested/test_planted.py:5: test_constant: constant\n"
        "nested/test_planted.py:9: test_swallowed: swallowed"
    )
