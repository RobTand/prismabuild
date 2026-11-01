"""Use PB's stdlib digest owner without importing the runtime package.

The recorder also runs in Python subprocesses with a replaced PYTHONPATH.
Load the owner's exact source next to this repository instead of requiring an
installed PB package. No alternate digest or JSON recipe lives here.
"""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    'tia_digest_owner', Path(__file__).resolve().parents[2] / 'src/prismabuild/digest_primitives.py')
_owner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_owner)
raw_sha256 = _owner.raw_sha256
compact_ascii_json_bytes = _owner.compact_ascii_json_bytes
sorted_json = _owner.sorted_json
