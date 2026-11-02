"""A new test module, added the documented way (docs/bazel.md). Probe for #2593; closed unmerged."""
from prismabuild import digest_primitives


def test_a_new_module_is_selected_and_runs():
    assert digest_primitives.raw_sha256(b'') == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
