"""The broker scope identity agrees three ways by contract (issue #1301).

The issuer (``tools/fleet/resource_broker.py::scope_id``, a stdlib-only root
daemon) cannot share a code owner with the package: importing the package
into the broker would drag the world into a root process, and the installed
package ships without the fleet tools. So the issuer keeps its spelling, the
package owns its mirror (``produced_output._broker_scope_id``, called by
``resource_scope._adopt_created_scope`` rather than restated), and this test
pins the byte agreement across the trust boundary.

No filesystem, no broker, no cgroups: the validator method runs against a
stub carrying only the fields it reads.
"""

from __future__ import annotations

from pathlib import Path
import sys
import types

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import produced_output as po  # noqa: E402
from prismabuild import resource_scope as rs  # noqa: E402

import resource_broker as broker  # noqa: E402


VECTORS = [
    ("f" * 64, "a" * 32),
    ("0" * 64, "0" * 32),
    ("action-key-with-dashes_and_underscores.v1", "nonce-1"),
    ("", ""),
    ("ab", "c"),  # concat-boundary pair, see below
    ("a", "bc"),
    ("unicode-αβγ", "unicode-δεζ"),
    ("k" * 1000, "n" * 1000),
]


def test_issuer_and_package_mirror_agree_byte_for_byte() -> None:
    for key, nonce in VECTORS:
        assert broker.scope_id(key, nonce) == po._broker_scope_id(key, nonce)


def test_names_keep_the_wire_shape() -> None:
    name = broker.scope_id("k" * 64, "n" * 32)
    assert name.startswith("prismabuild-job")
    assert name.endswith(".slice")
    assert len(name) == len("prismabuild-job") + 32 + len(".slice")


def test_validator_accepts_what_the_broker_issues() -> None:
    key, nonce = "e" * 64, "b" * 32
    stub = types.SimpleNamespace(
        action_key=key,
        nonce=nonce,
        gpu_memory_max_bytes=None,
        _explicit_gpu_budget=False,
    )
    issued = broker.scope_id(key, nonce)
    response = {
        "scope_id": issued,
        "token": "c" * 64,
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / issued),
    }
    rs.ResourceScope._adopt_created_scope(stub, response)
    assert stub.unit == issued


def test_validator_refuses_a_forged_scope() -> None:
    key, nonce = "e" * 64, "b" * 32
    stub = types.SimpleNamespace(
        action_key=key,
        nonce=nonce,
        gpu_memory_max_bytes=None,
        _explicit_gpu_budget=False,
    )
    forged = broker.scope_id("f" * 64, nonce)
    response = {
        "scope_id": forged,
        "token": "c" * 64,
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / forged),
    }
    try:
        rs.ResourceScope._adopt_created_scope(stub, response)
    except OSError:
        pass
    else:
        raise AssertionError("the validator accepted a foreign scope id")
