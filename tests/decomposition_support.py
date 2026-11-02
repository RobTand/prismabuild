"""A stand-in for what pbrun's Stage A freezes, shared by the decomposition tests.

Moved out of ``tests/conftest.py`` (#2582) so that only the tests that build
a decomposition parent depend on ``prismabuild.decomposition``.
"""

from __future__ import annotations


# --------------------------------------------------------------------------
# Decomposition: a stand-in for what pbrun's Stage A freezes
#
# A decomposition parent is keyed on the half of a sealed action that its
# children all share, which ``pbrun.template_action_common`` produces off a
# real checkout.  The batcher, the refusal surface and the merge do not need a
# real tree to be tested against, but they do need that half to be *shaped*
# like the real one, because that is what ``freeze_common`` validates.  So this
# is the miniature: the same six sections, plausible values, and nothing a
# caller cannot vary when the point of the test is that varying it moves the
# parent.
# --------------------------------------------------------------------------

#: Two digests that stand for a source tree and a data manifest.  Named rather
#: than inlined so a test that means "another tree" says so.
DECOMPOSITION_SNAPSHOT = "a" * 64
DECOMPOSITION_MANIFEST = "b" * 64


def action_common(
    *,
    snapshot: str = DECOMPOSITION_SNAPSHOT,
    cwd: str = ".",
    manifest: str | None = None,
    variables: dict[str, str] | None = None,
    **params: object,
) -> dict[str, object]:
    """The sealed half of an action, as a template would hand it over."""

    sealed: dict[str, object] = {
        "cwd": cwd,
        "demand": {"cpu": 1, "mem_gb": 4},
        "placement": {"required_tags": []},
        "retry_policy": {"max_attempts": 1, "retry_safe": False},
        "checkout_snapshot": {
            "input": {"id": "pbrun.checkout-snapshot", "sha256": snapshot,
                      "bytes": 4096},
        },
        **params,
    }
    if manifest is not None:
        sealed["data_manifest"] = {
            "input": {"id": "pbcampaign.data-manifest", "sha256": manifest,
                      "bytes": 512},
            "mount_prefix": "/mnt/shared",
            "entry_count": 1,
            "total_bytes": 512,
        }
    return {
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "working_directory": ".",
        },
        "params": sealed,
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": snapshot,
                    "bytes": 4096}],
        "code_closure": {"schema": "prismaquant.prismabuild.code_closure.v1",
                         "files": [], "closure_sha256": "c" * 64},
        "environment": {
            "variables": {"PATH": "/usr/local/bin:/usr/bin:/bin",
                          **(variables or {})},
            "toolchain": {},
        },
        "execution_scope": {"kind": "platform_keyed",
                            "platform_key": "linux-aarch64-sm121"},
    }


def frozen_common(
    request: dict,
    *,
    snapshot: str = DECOMPOSITION_SNAPSHOT,
    cwd: str = ".",
    variables: dict[str, str] | None = None,
    **sealed: object,
) -> dict:
    """Freeze a logical request against the shared synthetic action fixture."""
    from prismabuild import decomposition

    common = request["common"]
    if common["gpu_memory_gb"] is not None:
        sealed.setdefault("gpu_memory_gb", common["gpu_memory_gb"])
    return decomposition.freeze_common(
        common,
        action_common=action_common(
            snapshot=snapshot,
            cwd=cwd,
            manifest=None if common["data_manifest"] is None else DECOMPOSITION_MANIFEST,
            variables={**common["env"], **(variables or {})},
            **sealed,
        ),
    )

