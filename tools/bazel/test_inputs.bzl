"""Inputs a test reads that no static evidence names, each with its reason.

tools/bazel/build_files.py derives every other input from the source. A test
listed here reads files through a root it receives as a parameter, so the path
analysis can't see them. The remote executor makes such a read fail rather than
pass, which is how an entry is found. Keep each entry to the files named, and
the reason true.
"""

TEST_INPUTS = {
    "//tests:test_bazel_test_sizing": struct(
        data = ["//tests:BUILD.bazel"],
        reason = "it checks that each measured size names a generated pb_py_test, reading the BUILD file which the source generator excludes from inferred inputs.",
    ),
    "//tests:test_allowance_suite_filesystem_isolation": struct(
        data = ["//tests:test_a_producers_exports_run_on_its_allowance_lib"],
        reason = "it runs tests/test_a_producers_exports_run_on_its_allowance.py in a child pytest, which needs that module's imports.",
    ),
    "//tests:test_import_cycles": struct(
        data = ["//src/prismabuild:files", "//tools:files"],
        reason = "tools/ci/import_cycles.py scans src/prismabuild and tools under the repository root by default.",
    ),
    "//tests:test_pbtest_digest_owner_shard_1553": struct(
        data = ["//tests:test_core_lib"],
        reason = "it runs a tests/test_core.py node through pbtest, which needs that module's imports.",
    ),
    "//tests:test_duplication_baseline": struct(
        data = ["//src/prismabuild:files", "//tools:files"],
        reason = "tools/duplication_inventory.py scans src/prismabuild and tools under the root its caller passes.",
    ),
    "//tests:test_claim_wait_contract": struct(
        data = ["//src/prismabuild:files"],
        reason = "it parses src/prismabuild modules (pool.py, adaptive_cpu.py, adaptive_gpu.py, ...) from a directory derived from a module's __file__.",
    ),
    "//tests:test_compat_namespace_preflight": struct(
        data = ["//tests:test_current_receipt_compat_composition_lib"],
        reason = "it runs tests/test_current_receipt_compat_composition.py in a child pytest, which needs that module's imports.",
    ),
    "//tests:test_compat_definition_cache": struct(
        data = ["//tests:files/compat/generations"],
        reason = "tests/compat/support/record_compat.py reads generation records from the directory its caller passes.",
    ),
    "//tests:test_wire_inventory": struct(
        data = ["//src/prismabuild:files", "//tools:files", "//tools/bazel:files", "//tests:files", "//:files/fleet"],
        reason = "tests/compat/support/wire_inventory.py scans src, tools, tests and fleet under the root its caller passes.",
    ),
}
