"""Execute Python-compatible Bazel macros with explicit rule bindings."""
from pathlib import Path
import re
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


def starlark(path, **bindings):
    # Execute the real macros, replacing only Bazel's rule/package primitives.
    source = (ROOT / path).read_text()
    source = re.sub(r'^load\([^\n]*\)\n', '', source, flags=re.MULTILINE)
    namespace = {'struct': SimpleNamespace, **bindings}
    exec(compile(source, path, 'exec'), namespace)
    return namespace
