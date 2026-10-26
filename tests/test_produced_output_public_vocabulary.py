"""The permanent produced-output public constants bind their one concrete owner."""
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import client, output_contract, produced_output


@pytest.mark.parametrize('name,value', [
    ('PRODUCED_OUTPUT_REF_SCHEMA_V1', 'prismabuild.produced_output_ref.v1'),
    ('OUTPUT_TEMPLATES_SUBDIR', 'produced-output-templates'),
    ('OUTPUT_SCOPES_SUBDIR', 'produced-output-scopes'),
    ('OUTPUT_BATCHES_SUBDIR', 'produced-output-batches'),
    ('OUTPUT_FRAGMENTS_SUBDIR', 'produced-output-fragments'),
])
def test_the_public_output_constant_is_the_owner_object(name, value):
    namespace = {}
    exec('from prismabuild.produced_output import *', namespace)
    assert name in produced_output.__all__
    assert namespace[name] == value
    assert vars(produced_output)[name] is vars(output_contract)[name]
    assert namespace[name] is vars(output_contract)[name]
    assert client.SDK_VERSION == 7
