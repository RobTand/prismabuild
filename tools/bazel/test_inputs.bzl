"""Inputs a test reads that no static evidence names, each with its reason.

tools/bazel/build_files.py derives every other input from the source. A test
listed here reads files through a root it receives as a parameter, so the path
analysis can't see them. The remote executor makes such a read fail rather than
pass, which is how an entry is found. Keep each entry to the files named, and
the reason true.
"""

TEST_INPUTS = {
    "//tests:test_allowance_suite_filesystem_isolation": struct(
        data = ["//tests:test_a_producers_exports_run_on_its_allowance_lib"],
        reason = "it runs tests/test_a_producers_exports_run_on_its_allowance.py in a child pytest, which needs that module's imports.",
    ),
    "//tests:test_pbtest_digest_owner_shard_1553": struct(
        data = ["//tests:test_core_lib"],
        reason = "it runs a tests/test_core.py node through pbtest, which needs that module's imports.",
    ),
    "//tests:test_duplication_baseline": struct(
        data = ["//src/prismabuild:files", "//tools:files"],
        reason = "tools/duplication_inventory.py scans src/prismabuild and tools under the root its caller passes.",
    ),
    "//tests:test_compat_definition_cache": struct(
        data = ["//tests:files/compat/generations"],
        reason = "tests/compat/support/record_compat.py reads generation records from the directory its caller passes.",
    ),
    "//tests:test_wire_inventory": struct(
        data = ["//src/prismabuild:files", "//tools:files", "//tests:files", "//:files/fleet"],
        reason = "tests/compat/support/wire_inventory.py scans src, tools, tests and fleet under the root its caller passes.",
    ),
}
