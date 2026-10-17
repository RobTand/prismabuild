"""Exact old/current funding bytes through the original C31 consumer controls."""
import pytest

from prismabuild.record_kinds import KINDS
import record_compat_contract as contract
from record_compat_contract import RecordContract, generations, writer_sources


FUNDING_KINDS = tuple(kind for kind in KINDS
                      if kind.name in ("q-funding", "q-output-funding", "out-batch",
                                       "out-descriptor", "out-batch-ref"))


@pytest.fixture(scope="module", autouse=True)
def funding_wire_observations(request):
    """Give the additional controls their own existing C31 recorder."""
    evidence = contract.WriterEvidence()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(contract, 'WRITER_EVIDENCE', evidence)
        try:
            yield
        finally:
            for record in evidence.records.values():
                record['proof_scope'] = 'tests/test_funding_wire_compatibility.py'
            reporter = request.config.pluginmanager.getplugin('terminalreporter')
            if reporter is not None:
                evidence.pytest_terminal_summary(reporter)


def pytest_generate_tests(metafunc):
    if metafunc.cls is not TestFundingWire:
        return
    if "case" in metafunc.fixturenames:
        metafunc.parametrize("case", [kind.name + "/" + variant
                                     for kind in FUNDING_KINDS
                                     for variant in kind.variants])
    elif "kind" in metafunc.fixturenames:
        metafunc.parametrize("kind", FUNDING_KINDS,
                             ids=[kind.name for kind in FUNDING_KINDS])


class TestFundingWire(RecordContract):
    pass
