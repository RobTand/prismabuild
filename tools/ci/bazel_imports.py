"""Pure PrismaBuild import checks on Bazel library edges and source ownership."""
from pathlib import PurePosixPath

SOURCE_ROOTS = ('src/prismabuild/', 'tools/')
RATCHETED_LISTS = ('conftest_source_imports', 'test_to_test_imports')


def is_source(path):
    return path.startswith(SOURCE_ROOTS)


def is_conftest(path):
    return PurePosixPath(path).name == 'conftest.py'


def is_test_module(path):
    return path.startswith('tests/') and PurePosixPath(path).name.startswith('test_')


def bazel_imports(edges: dict, owners: dict) -> dict:
    """Project library dependency edges onto their owned source files.

    Test wrappers and data files have no library ownership here. A generated
    cycle library owns several modules; its members import within that group.
    """
    def path(label):
        return label.removeprefix('//').replace(':', '/').lstrip('/')

    found = {key: set() for key in RATCHETED_LISTS}
    for rule, sources in owners.items():
        targets = set().union(*(owners.get(dep, set()) for dep in edges.get(rule, ())))
        if len(sources) > 1:
            targets |= sources
        for source in sources:
            for target in targets - {source}:
                left, right = path(source), path(target)
                edge = f'{left} -> {right}'
                if left.startswith('tests/') and is_test_module(right):
                    found['test_to_test_imports'].add(edge)
                if is_conftest(left) and is_source(right):
                    found['conftest_source_imports'].add(edge)
    return {key: sorted(values) for key, values in found.items()}


def bazel_regressions(live: dict, baseline: dict) -> list[str]:
    return [f'new {key} entry: {entry}' for key in RATCHETED_LISTS
            for entry in sorted(set(live[key]) - set(baseline[key]))]
