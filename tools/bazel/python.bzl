"""Macros the generated BUILD files use: one library per module, one test per file.

tools/bazel/build_files.py writes the targets. These macros add what every
module or test shares, so the generated files hold only per-module evidence.
"""

load("@pb_pip//:requirements.bzl", "all_requirements")
load("@rules_python//python:py_library.bzl", "py_library")
load("@rules_python//python:py_test.bzl", "py_test")
load("//tools/bazel:nonhermetic.bzl", "nonhermetic")
load("//tools/bazel:test_inputs.bzl", "TEST_INPUTS")
load("//tools/bazel:test_sizing.bzl", "TEST_SIZING")

# What every hosted gate shard has: both conftest files, pyproject.toml's
# pytest options, the per-test budgets, and every package the gate installs.
_TEST_DATA = [
    "//:pyproject.toml",
    "//tools:ci/gate_budget.json",
    # The gate installs py-spy; its binary isn't part of the library.
    "@pb_pip//py_spy:data",
]
_TEST_DEPS = [
    "//:conftest",
    "//tests:conftest",
    "//tools/bazel:runner",
    "//tools:ci/gate_run",
] + all_requirements

def _unique(labels):
    """A generated label and a shared one may name the same target once each."""
    unique = {}
    for label in labels:
        unique[str(native.package_relative_label(label))] = label
    return list(unique.values())

def pb_py_library(name, srcs, deps = [], data = [], **kwargs):
    py_library(name = name, srcs = srcs, deps = deps, data = data, **kwargs)

def pb_py_test(name, path, library, args = [], tags = [], deps = [], data = [], **kwargs):
    """Run the test file at repository path `path`, whose module is `library`.

    deps and data are the test's own inputs, which never propagate: modules it
    names in strings, the tree reads of production modules it imports itself
    (tools/bazel/build_files.py), and test_inputs.bzl's entry.
    """
    label = "//%s:%s" % (native.package_name(), name)
    extra = TEST_INPUTS.get(label)
    extra_tags, exec_properties, env = nonhermetic(name)
    sizing = TEST_SIZING.get(label)
    if sizing:
        exec_properties.update(sizing.exec_properties)
    py_test(
        name = name,
        main = "//tools/bazel:python_runner.py",
        srcs = ["//tools/bazel:python_runner.py"],
        args = [path] + args,
        deps = _unique([library] + deps + _TEST_DEPS),
        data = _unique(_TEST_DATA + data + (extra.data if extra else [])),
        tags = tags + extra_tags,
        exec_properties = exec_properties,
        env = env,
        **kwargs
    )
