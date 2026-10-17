"""Exact old/current funding bytes through the original C31 consumer controls."""
from prismabuild.record_kinds import KINDS
from record_compat_contract import RecordContract, generations, writer_sources


FUNDING_KINDS = tuple(kind for kind in KINDS
                      if kind.name in ("q-funding", "q-output-funding"))


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
