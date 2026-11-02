#!/usr/bin/env python3
"""How many gate tests a change to one source module selects (PB #2582).

Bazel, and the gate's own selector, run a test again when anything in its
static dependency closure changes. A test's closure starts from the test file
and every ``conftest.py`` that applies to it: the root one and each one from
``tests/`` down to the test's directory. It then follows module dependencies
transitively. A source module is a Python file under ``src/prismabuild/`` or
``tools/``. For each source module this counts the gate tests whose closure
contains it, as a fraction of the gate population that
``tools/ci/gate_select.py`` defines. A module that half the gate depends on
selects half the gate whenever it changes, and invalidates the cached results
of all those tests.

Two kinds of evidence are available:

- ``imports`` is the default, and the ratchet holds it. It counts import
  statements anywhere in a file, including function-local imports and
  imports in embedded Python snippets. It also counts literal
  ``import_module`` and ``__import__`` names, the modules ``pytest_plugins``
  names, and string literals that name a
  ``.py`` file. A ``.py`` literal matches every module path that ends with
  it. A matched source module is followed like an import, because a test that
  runs a tool by path needs the tool's own imports. Any other matched file is
  a leaf: a test file named by path is read or matched by name, not run.
  Names resolve the way the gate's selector resolves them
  (``gate_select.module_names``).
- ``gate`` is the selector's own ``gate_select.dependencies``, which the
  Bazel BUILD generator also uses. It adds module names written as strings
  and every path reference, including a whole-repository ``**``. It takes
  tens of seconds, so it is reported, not ratcheted.

``tests/test_import_fanout.py`` holds the live result against
``tests/fixtures/import_fanout_baseline.json``. A new test-to-test import, a
new source import in a conftest, or a summary figure worse than the baseline
by more than its tolerance fails. So does a baseline that no longer records
the current state: a gain beyond a tolerance, or an entry that has gone.
``--write-baseline`` records the gain and refuses to record a loss.
"""
from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import math
from pathlib import Path, PurePosixPath
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / 'tests/fixtures/import_fanout_baseline.json'
SOURCE_ROOTS = ('src/prismabuild/', 'tools/')
SCANNED_ROOTS = ('tests', 'src/prismabuild', 'tools')
#: How far a summary figure may move without a baseline change. Adding an
#: ordinary test shifts the fractions by about 0.1 percentage points, so this
#: absorbs routine work while a structural regression still fails.
PERCENT_TOLERANCE = 1.0
MODULE_TOLERANCE = 2

SELECT_SPEC = importlib.util.spec_from_file_location('gate_select', Path(__file__).with_name('gate_select.py'))
gate_select = importlib.util.module_from_spec(SELECT_SPEC)
SELECT_SPEC.loader.exec_module(gate_select)

# The start of an import statement on its own line. The statement is then
# read to its logical end and parsed, so parentheses, continuations and
# comments are Python's own business.
STATEMENT_START = re.compile(r'^[ \t]*((?:from[ \t]+\.*[\w.]*[ \t]+import|import[ \t]+[A-Za-z_]))', re.M)
# An import inside a one-line snippet, such as ``"import sys; import pbrun"``
# handed to ``python -c``: it follows a quote, a semicolon or an escaped
# newline rather than starting a line.
SNIPPET_IMPORT = re.compile(r'''(?:['";]|\\n)[ \t]*(?:from[ \t]+([A-Za-z_][\w.]*)[ \t]+import[ \t]+([\w., \t]+)'''
                            r'''|import[ \t]+([\w., \t]+))''')
# ``pytest_plugins`` names modules pytest imports for every test under the conftest.
PLUGINS = re.compile(r'^pytest_plugins[ \t]*=', re.M)
DYNAMIC_IMPORT = re.compile(r'''\b(?:import_module|__import__)\(\s*(?:name\s*=\s*)?['"]([A-Za-z_][\w.]*)['"]''')
PY_LITERAL = re.compile(r'''['"]((?:[\w.-]+/)*[\w-][\w.-]*\.py)['"]''')


def is_source(path: str) -> bool:
    return path.startswith(SOURCE_ROOTS)


def is_conftest(path: str) -> bool:
    return PurePosixPath(path).name == 'conftest.py'


def is_test_module(path: str) -> bool:
    return path.startswith('tests/') and PurePosixPath(path).name.startswith('test_')


def python_files(root: Path) -> list[str]:
    """Every module the graph may name: the scanned trees and the root conftest."""
    found = {path.relative_to(root).as_posix()
             for top in SCANNED_ROOTS if (root / top).is_dir()
             for path in (root / top).rglob('*.py')
             if '__pycache__' not in path.relative_to(root).parts}
    if (root / 'conftest.py').is_file():
        found.add('conftest.py')
    return sorted(found)


def _logical_statement(source: str, start: int) -> str:
    """The text of one logical line from ``start``, as Python would join it."""
    depth, index = 0, start
    while index < len(source):
        char = source[index]
        if char == '#':
            newline = source.find('\n', index)
            index = len(source) if newline < 0 else newline
            continue
        if char == '\\' and source.startswith('\n', index + 1):
            index += 2
            continue
        if char in '([{':
            depth += 1
        elif char in ')]}':
            depth -= 1
        elif char == ';' or (char == '\n' and depth <= 0):
            break
        index += 1
    return source[start:index]


def _package_prefix(path: str, level: int) -> str:
    package = list(PurePosixPath(path.removeprefix('src/')).parent.parts)
    return '.'.join(package[:len(package) - level + 1])


def imported_names(source: str, path: str = '') -> set[str]:
    """Dotted names a file imports, by statement, snippet or literal call."""
    names = set()
    for match in STATEMENT_START.finditer(source):
        try:
            tree = ast.parse(_logical_statement(source, match.start(1)))
        except SyntaxError:
            continue  # prose that happens to start with "import"
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                prefix = node.module or ''
                if node.level:
                    prefix = '.'.join(part for part in (_package_prefix(path, node.level), prefix) if part)
                if prefix:
                    names.add(prefix)
                names.update(f'{prefix}.{alias.name}' if prefix else alias.name
                             for alias in node.names if alias.name != '*')
    for match in SNIPPET_IMPORT.finditer(source):
        module, members, plain = match.groups()
        listed = [part.split()[0] for part in (members or plain).split(',') if part.split()]
        if module:
            names.add(module)
            names.update(f'{module}.{member}' for member in listed if member.isidentifier())
        else:
            names.update(name for name in listed if re.fullmatch(r'[A-Za-z_][\w.]*', name))
    names.update(DYNAMIC_IMPORT.findall(source))
    for match in PLUGINS.finditer(source):
        try:
            tree = ast.parse(_logical_statement(source, match.start()))
        except SyntaxError:
            continue
        names.update(node.value for node in ast.walk(tree)
                     if isinstance(node, ast.Constant) and isinstance(node.value, str))
    return names


def named_files(source: str) -> set[str]:
    """``.py`` paths written as string literals: files read or run by path."""
    return set(PY_LITERAL.findall(source))


class ImportGraph:
    """Module edges, and the files each module names as leaves, for one tree."""

    def __init__(self, root: Path, evidence: str = 'imports'):
        if evidence not in {'imports', 'gate'}:
            raise ValueError('evidence must be imports or gate')
        self.root, self.evidence = Path(root), evidence
        self.files = python_files(self.root)
        self.known = set(self.files)
        self.aliases: dict[str, set[str]] = {}
        for path in self.files:
            for alias in gate_select.module_names(path):
                self.aliases.setdefault(alias, set()).add(path)
        self.by_name: dict[str, set[str]] = {}
        for path in self.files:
            self.by_name.setdefault(PurePosixPath(path).name, set()).add(path)
        self.edges: dict[str, set[str]] = {}
        self.leaves: dict[str, set[str]] = {}
        for path in self.files:
            source = (self.root / path).read_text(encoding='utf-8')
            if evidence == 'imports':
                self.edges[path], self.leaves[path] = self._import_evidence(source, path)
            else:
                self.edges[path], self.leaves[path] = self._gate_evidence(source, path)

    def resolve(self, name: str) -> set[str]:
        """Every file a dotted name may load, the package ``__init__`` included."""
        parts, found = name.split('.'), set()
        for end in range(1, len(parts) + 1):
            found.update(self.aliases.get('.'.join(parts[:end]), ()))
        return found

    def _suffix_matches(self, literal: str) -> set[str]:
        tail = PurePosixPath(literal)
        return {path for path in self.by_name.get(tail.name, ())
                if path == literal or path.endswith('/' + literal.lstrip('./'))}

    def _import_evidence(self, source: str, path: str) -> tuple[set[str], set[str]]:
        edges = set().union(*(self.resolve(name) for name in imported_names(source, path)))
        named = set().union(*(self._suffix_matches(literal) for literal in named_files(source)))
        edges |= {file for file in named if is_source(file)}
        return edges - {path}, named - edges - {path}

    def _gate_evidence(self, source: str, path: str) -> tuple[set[str], set[str]]:
        test_side = path.startswith('tests/') or path == 'conftest.py'
        imports, references = gate_select.dependencies(source, path, production=not test_side)
        if not test_side:
            # The Bazel generator also gives production code every file it names by path.
            references = references | gate_select.gate_paths.references(ast.parse(source), path)
        edges = set().union(*(self.resolve(name) for name in imports))
        if '**' in references:
            return edges - {path}, set(self.files)
        # An exact path to a module is a library edge, as the Bazel generator makes it.
        edges.update(reference for reference in references if reference in self.known)
        patterns = {reference for reference in references if '**' in reference}
        leaves = {file for file in self.files if gate_select.gate_paths.matches(file, patterns)} if patterns else set()
        if not test_side:
            # Production code that names a test file reads it; it does not import it.
            leaves |= {file for file in edges if file.startswith('tests/') or file == 'conftest.py'}
            edges = {file for file in edges if not (file.startswith('tests/') or file == 'conftest.py')}
        return edges - {path}, leaves - {path}

    def conftests_for(self, test: str) -> list[str]:
        """The root conftest and each conftest from ``tests/`` down to the test's directory."""
        chain = ['conftest.py'] if 'conftest.py' in self.edges else []
        parts = PurePosixPath(test).parent.parts
        for end in range(1, len(parts) + 1):
            candidate = '/'.join(parts[:end]) + '/conftest.py'
            if candidate in self.edges:
                chain.append(candidate)
        return chain

    def source_closures(self) -> tuple[list[str], dict[str, int]]:
        """Each module's transitive source dependencies, as a bit mask over source modules.

        A fixed point over the edges: cycles need no special case, and a
        leaf contributes its own bit without its imports.
        """
        sources = [path for path in self.files if is_source(path)]
        bit = {path: 1 << index for index, path in enumerate(sources)}
        own = {path: bit.get(path, 0) | sum(bit.get(leaf, 0) for leaf in self.leaves[path])
               for path in self.files}
        masks = dict(own)
        changed = True
        while changed:
            changed = False
            for path in self.files:
                mask = own[path]
                for target in self.edges[path]:
                    mask |= masks[target]
                if mask != masks[path]:
                    masks[path], changed = mask, True
        return sources, masks


def gate_population(root: Path) -> list[str]:
    """The gate's full-mode test files, by the gate's own definition."""
    root = Path(root)
    names = [path for path in python_files(root) if path.startswith('tests/')]
    plan = gate_select.create_plan(
        root, [], durations=json.loads((root / 'tools/ci/gate_durations.json').read_text()),
        quarantine=json.loads((root / 'tools/ci/gate_quarantine.json').read_text()),
        index={name: (set(), set()) for name in names}, mode='full')
    return [row['path'] for row in plan['selected']]


def nearest_rank(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)] if ordered else 0.0


def measure(root: Path = ROOT, evidence: str = 'imports', population: list[str] | None = None) -> dict:
    """The fan-out of every source module over the gate population, and its summary."""
    graph = ImportGraph(root, evidence)
    population = gate_population(root) if population is None else population
    sources, masks = graph.source_closures()
    dependents = dict.fromkeys(sources, 0)
    for test in population:
        mask = masks[test]
        for conftest in graph.conftests_for(test):
            mask |= masks[conftest]
        index = 0
        while mask:
            if mask & 1:
                dependents[sources[index]] += 1
            mask >>= 1
            index += 1
    fractions = [count / len(population) for count in dependents.values()] if population else []
    conftests = [path for path in graph.files if is_conftest(path)]
    conftest_mask = 0
    for conftest in conftests:
        conftest_mask |= masks[conftest]
    return {
        'evidence': evidence,
        'tests': len(population),
        'source_modules': len(sources),
        'median_selected_pct': round(100 * nearest_rank(fractions, 0.5), 1),
        'p90_selected_pct': round(100 * nearest_rank(fractions, 0.9), 1),
        'modules_over_half': sum(fraction > 0.5 for fraction in fractions),
        'conftest_source_modules': bin(conftest_mask).count('1'),
        'conftest_source_imports': sorted(f'{conftest} -> {target}' for conftest in conftests
                                          for target in graph.edges[conftest] if is_source(target)),
        'test_to_test_imports': sorted(f'{path} -> {target}' for path in graph.files
                                       if path.startswith('tests/')
                                       for target in graph.edges[path] if is_test_module(target)),
        'dependents': dict(sorted(dependents.items(), key=lambda item: (-item[1], item[0]))),
    }


RATCHETED_LISTS = ('conftest_source_imports', 'test_to_test_imports')
RATCHETED_FIGURES = {'median_selected_pct': PERCENT_TOLERANCE, 'p90_selected_pct': PERCENT_TOLERANCE,
                     'modules_over_half': MODULE_TOLERANCE, 'conftest_source_modules': 0}


def regressions(live: dict, baseline: dict) -> list[str]:
    """What the live graph adds to the baseline. Each one fails the ratchet."""
    found = []
    for key in RATCHETED_LISTS:
        for entry in sorted(set(live[key]).difference(baseline[key])):
            found.append(f'new {key} entry: {entry}')
    for key, tolerance in RATCHETED_FIGURES.items():
        if live[key] > baseline[key] + tolerance:
            found.append(f'{key} rose from {baseline[key]} to {live[key]} (tolerance {tolerance})')
    return found


def stale_entries(live: dict, baseline: dict) -> list[str]:
    """Gains the baseline does not record yet. Each one must shrink it in the same change."""
    found = []
    for key in RATCHETED_LISTS:
        for entry in sorted(set(baseline[key]).difference(live[key])):
            found.append(f'{key} entry is gone: {entry}')
    for key, tolerance in RATCHETED_FIGURES.items():
        if live[key] < baseline[key] - tolerance:
            found.append(f'{key} fell from {baseline[key]} to {live[key]}')
    return found


def baseline_record(live: dict) -> dict:
    return {key: live[key] for key in ('evidence', *RATCHETED_FIGURES, *RATCHETED_LISTS)}


def fanout_summary(live: dict, top: int) -> str:
    lines = [f"evidence {live['evidence']}: {live['tests']} gate tests, {live['source_modules']} source modules",
             f"single-module change selects: median {live['median_selected_pct']}%, "
             f"p90 {live['p90_selected_pct']}%; {live['modules_over_half']} modules select over half",
             f"conftest chain reaches {live['conftest_source_modules']} source modules "
             f"({len(live['conftest_source_imports'])} direct imports)",
             f"test-to-test imports: {len(live['test_to_test_imports'])}"]
    for path, count in list(live['dependents'].items())[:top]:
        lines.append(f'  {count:5d}  {100 * count / max(live["tests"], 1):5.1f}%  {path}')
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n', 1)[0])
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--evidence', choices=('imports', 'gate'), default='imports')
    parser.add_argument('--top', type=int, default=0, help='also list the N modules with the most dependents')
    parser.add_argument('--json', type=Path, help='write the full measurement here')
    parser.add_argument('--write-baseline', action='store_true',
                        help='record a gain in the committed baseline; refuses a regression')
    args = parser.parse_args(argv)
    live = measure(args.root, args.evidence)
    print(fanout_summary(live, args.top))
    if args.json:
        args.json.write_text(json.dumps(live, indent=2) + '\n', encoding='utf-8')
    if args.write_baseline:
        if args.evidence != 'imports':
            parser.error('the baseline holds the imports evidence')
        path = args.root / BASELINE.relative_to(ROOT)
        if path.exists():
            worse = regressions(live, json.loads(path.read_text(encoding='utf-8')))
            if worse:
                print('refusing to record a regression:\n  ' + '\n  '.join(worse), file=sys.stderr)
                return 1
        path.write_text(json.dumps(baseline_record(live), indent=2) + '\n', encoding='utf-8')
    return 0


if __name__ == '__main__':
    sys.exit(main())
