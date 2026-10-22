"""Collected tests must get retained source from the committed tree (#1999)."""
from __future__ import annotations

import ast
import io
import re
import shlex
import tokenize
from pathlib import Path
from itertools import product

from prismabuild.digest_primitives import raw_sha256

import pytest

import history_inputs

HISTORY_TOOLS = {
    "compat/support/compat_sources.py": "Maintenance capture deliberately resolves historical compatibility sources.",
    "compat/support/rebind_source_reviews.py": "Maintenance rebinding compares reviewed sources across explicit revisions.",
    "compat/support/record_compat.py": "Compatibility capture records the selected historical source identity.",
    "c07_review_audit.py": "Standalone source audit compares the reviewed C07 baseline with current sources.",
    "lifecycle_source_audit.py": "Standalone lifecycle audit compares explicit historical source trees.",
}
HISTORY_VERBS = {"show", "archive", "cat-file", "ls-tree", "rev-list", "log", "diff"}


class _HistoryScope:
    def __init__(self, parent=None):
        self.parent = parent
        self.bindings = {}

    def bind(self, name, value):
        self.bindings.setdefault(name, []).append(value)

    def lookup(self, name):
        scope = self
        while scope is not None:
            if name in scope.bindings:
                values = scope.bindings[name]
                return scope, values[0] if len(values) == 1 else None
            scope = scope.parent
        return self, None


class _HistoryBindings(ast.NodeVisitor):
    """Keep lexical owners separate, including parameters and rebinding."""

    def __init__(self, tree):
        self.scope = _HistoryScope()
        self.root = self.scope
        self.scopes = {}
        self.owners = {}
        self.functions = {}
        self.visit(tree)

    def visit(self, node):
        self.scopes[node] = self.scope
        return super().visit(node)

    def visit_Name(self, node):
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.scope.bind(node.id, None)

    def visit_Assign(self, node):
        self.visit(node.value)
        for target in node.targets:
            if isinstance(target, ast.Name):
                self.scope.bind(target.id, node.value)
                self.scopes[target] = self.scope
            else:
                self.visit(target)

    def visit_AnnAssign(self, node):
        if isinstance(node.target, ast.Name):
            self.scope.bind(node.target.id, node.value)
        else:
            self.visit(node.target)
        if node.value is not None:
            self.visit(node.value)

    def visit_Subscript(self, node):
        if isinstance(node.ctx, (ast.Store, ast.Del)) and isinstance(node.value, ast.Name):
            self.scope.bind(node.value.id, None)
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.attr in {'append', 'extend', 'insert', 'pop', 'remove', 'clear', 'sort', 'reverse'}:
                self.scope.bind(node.func.value.id, None)
        self.generic_visit(node)

    def visit_ExceptHandler(self, node):
        if node.name:
            self.scope.bind(node.name, None)
        self.generic_visit(node)

    def visit_MatchAs(self, node):
        if node.name:
            self.scope.bind(node.name, None)
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            self.scope.bind(alias.asname or alias.name.split('.')[0], alias.name)

    def visit_ImportFrom(self, node):
        for alias in node.names:
            self.scope.bind(alias.asname or alias.name, f"{'.' * node.level}{node.module or ''}.{alias.name}")

    def visit_FunctionDef(self, node):
        self.scope.bind(node.name, node)
        self.functions[node] = self.scope
        for value in [*node.decorator_list, *node.args.defaults,
                      *(v for v in node.args.kw_defaults if v is not None)]:
            self.visit(value)
        parent = self.scope
        # A method does not close over its class namespace.
        self.scope = _HistoryScope(parent.parent if self.owners.get(parent) == 'class' else parent)
        self.owners[self.scope] = node
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs,
                    *([node.args.vararg] if node.args.vararg else []),
                    *([node.args.kwarg] if node.args.kwarg else [])]:
            self.scope.bind(arg.arg, None)
        for child in node.body:
            self.visit(child)
        self.scope = parent

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node):
        self.scope.bind(node.name, node)
        for child in [*node.decorator_list, *node.bases, *node.keywords]:
            self.visit(child)
        parent = self.scope
        self.scope = _HistoryScope(parent)
        self.owners[self.scope] = 'class'
        for child in node.body:
            self.visit(child)
        self.scope = parent

    def visit_Lambda(self, node):
        parent = self.scope
        self.scope = _HistoryScope(parent)
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
            self.scope.bind(arg.arg, None)
        self.visit(node.body)
        self.scope = parent

    def visit_Global(self, node):
        for name in node.names:
            self.scope.bind(name, None)
            self.root.bind(name, None)

    visit_Nonlocal = visit_Global


class _HistoryGuard:
    LAUNCHERS = {'run', 'call', 'check_call', 'check_output', 'Popen', 'getoutput', 'getstatusoutput',
                 'system', 'popen', 'execl', 'execle', 'execlp', 'execlpe',
                 'execv', 'execve', 'execvp', 'execvpe', 'posix_spawn', 'posix_spawnp',
                 'spawnl', 'spawnle', 'spawnlp', 'spawnlpe',
                 'spawnv', 'spawnve', 'spawnvp', 'spawnvpe'}
    WRAPPERS = {'git', '_git', '_git_entry', 'lifecycle_audit_git'}
    # Known verbs locate the command after a wrapper's optional repository.
    # inspect() separately proves their operands; recognition is not permission.
    SETUP_VERBS = {'init', 'config', 'add', 'commit', 'status', 'clone', 'fetch',
                   'checkout', 'switch', 'branch', 'update-ref', 'symbolic-ref',
                   'remote', 'reset', 'clean', 'rm', 'mv', 'tag', 'bundle',
                   'ls-files', 'check-ignore', 'check-attr', 'hash-object',
                   'write-tree', 'commit-tree', 'update-index', 'read-tree',
                   'merge-base', 'rev-parse', 'worktree', 'submodule', 'sparse-checkout',
                   'index-pack', 'gc', 'count-objects'}
    VALUE_OPTIONS = {'-n', '--max-count', '--format', '--pretty', '--output', '--prefix'}
    FLAG_OPTIONS = {'-p', '-t', '-e', '-s', '-r', '-z', '--name-only', '--name-status',
                    '--stat', '--numstat', '--shortstat', '--raw', '--quiet',
                    '--exit-code', '--no-patch', '--oneline', '--no-color',
                    '--count', '--full-tree', '--abbrev-commit', '--no-renames', '--cached', '--staged'}

    def __init__(self, source, root=None, path=None, modules=None):
        self.source = source.decode() if isinstance(source, bytes) else source
        self.root = root.resolve() if root is not None else None
        self.path = path
        self.modules = modules if modules is not None else {}
        self.tree = ast.parse(source)
        self.lexical = _HistoryBindings(self.tree)
        self.heads = {}
        self.overrides = {}
        self.parents = {child: node for node in ast.walk(self.tree) for child in ast.iter_child_nodes(node)}

    def canonical(self, node, seen=frozenset()):
        if node in seen:
            return ''
        seen = seen | {node}
        if isinstance(node, ast.Name):
            _, value = self.lexical.scopes.get(node, self.lexical.root).lookup(node.id)
            if isinstance(value, (ast.Name, ast.Attribute)):
                return self.canonical(value, seen)
            if isinstance(value, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.id not in self.WRAPPERS:
                return '<local>.' + node.id
            return value if isinstance(value, str) else node.id
        if isinstance(node, ast.Attribute):
            return self.canonical(node.value, seen) + '.' + node.attr
        return ''

    def imported(self, node):
        """Read repository constants as syntax; never import their modules."""
        name = self.canonical(node)
        if self.root is None or '.' not in name:
            return None
        module, _, attribute = name.rpartition('.')
        if module.startswith('.'):
            if self.path is None:
                return None
            level = len(module) - len(module.lstrip('.'))
            base = self.path.parent
            for _ in range(level - 1):
                base = base.parent
            bases = [base]
            module = module.lstrip('.')
        else:
            bases = [self.root / 'tests', self.root, self.root / 'src']
        for base in bases:
            target = base.joinpath(*module.split('.')) if module else base
            for path in [target.with_suffix('.py'), target / '__init__.py']:
                if not path.resolve().is_relative_to(self.root) or not path.is_file():
                    continue
                if path not in self.modules:
                    self.modules[path] = _HistoryGuard(path.read_bytes(), self.root, path, self.modules)
                guard = self.modules[path]
                _, value = guard.lexical.root.lookup(attribute)
                if isinstance(value, ast.AST):
                    return guard, value
        return None

    def binding(self, node):
        scope = self.lexical.scopes.get(node, self.lexical.root)
        if (scope, node.id) in self.overrides:
            return self.overrides[scope, node.id]
        owner, value = scope.lookup(node.id)
        return value if owner is scope and isinstance(value, ast.AST) else None

    def text(self, node, seen=frozenset()):
        if node is None or node in seen:
            return '?'
        seen = seen | {node}
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if self.canonical(node) == 'sys.executable':
            return '<python>'
        if imported := self.imported(node):
            guard, value = imported
            return guard.text(value, seen)
        if isinstance(node, ast.Name):
            return self.text(self.binding(node), seen)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.text(node.left, seen) + self.text(node.right, seen)
        if isinstance(node, ast.JoinedStr):
            return ''.join(self.text(part, seen) for part in node.values)
        if isinstance(node, ast.FormattedValue) and node.conversion == -1 and node.format_spec is None:
            return self.text(node.value, seen)
        # Only these transformations preserve a captured HEAD verbatim.
        if isinstance(node, ast.Attribute) and node.attr == 'stdout':
            return self.text(node.value, seen)
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr in {'strip', 'decode'} and not node.args and not node.keywords:
                return self.text(node.func.value, seen)
            commands = self.commands(node)
            if len(commands) == 1:
                argv, repo = commands[0]
                if argv == ['rev-parse', 'HEAD'] and repo is not None:
                    marker = f'\0HEAD{id(node)}\0'
                    self.heads[marker] = (repo, node)
                    return marker
        return '?'

    def sequence(self, node, seen=frozenset()):
        if node is None or node in seen:
            return None
        seen = seen | {node}
        if imported := self.imported(node):
            guard, value = imported
            sequence = guard.sequence(value, seen)
            return [item if isinstance(item, str) else guard.text(item) for item in sequence] if sequence is not None else None
        if isinstance(node, ast.Name):
            return self.sequence(self.binding(node), seen)
        if isinstance(node, (ast.List, ast.Tuple)):
            items = []
            for item in node.elts:
                if isinstance(item, ast.Starred):
                    expanded = self.sequence(item.value, seen)
                    items.extend(expanded if expanded is not None else [item])
                else:
                    items.append(item)
            return items
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = self.sequence(node.left, seen), self.sequence(node.right, seen)
            return left + right if left is not None and right is not None else None
        if isinstance(node, ast.Call) and self.canonical(node.func) == 'shlex.split' and len(node.args) == 1 and not node.keywords:
            return self.shell_words(self.text(node.args[0]))
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return self.shell_words(node.value)
        return None

    @staticmethod
    def shell_words(command):
        # shlex does not interpret expansions or shell operators. Reject them.
        if any(char in command for char in {';', '|', '&', '<', '>', '`', '$', chr(92), chr(10)}):
            return None
        try:
            return shlex.split(command)
        except ValueError:
            return None

    def repository(self, node, seen=frozenset()):
        if node in seen:
            return None
        seen = seen | {node}
        if node is None or isinstance(node, ast.Constant) and node.value is None:
            return ('cwd',)
        if isinstance(node, ast.Call) and self.canonical(node.func) == 'str' and len(node.args) == 1:
            return self.repository(node.args[0], seen)
        if isinstance(node, ast.Name):
            scope = self.lexical.scopes.get(node, self.lexical.root)
            owner, value = scope.lookup(node.id)
            values = owner.bindings.get(node.id, [])
            if owner is not scope or len(values) > 1:
                return None
            if isinstance(value, (ast.Name, ast.Constant)):
                return self.repository(value, seen)
            return (id(owner), node.id)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return ('path', node.value)
        return None

    def program(self, node, seen=frozenset()):
        if isinstance(node, str):
            return node.rsplit('/', 1)[-1]
        if node is None or node in seen:
            return '?'
        seen = seen | {node}
        name = self.canonical(node)
        if name == 'sys.executable':
            return '<python>'
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value.rsplit('/', 1)[-1]
        if imported := self.imported(node):
            guard, value = imported
            return guard.program(value, seen)
        if isinstance(node, ast.Name):
            scope = self.lexical.scopes.get(node, self.lexical.root)
            owner, value = scope.lookup(node.id)
            if isinstance(value, ast.AST):
                return self.program(value, seen)
        if isinstance(node, (ast.List, ast.Tuple, ast.BinOp)):
            sequence = self.sequence(node)
            if sequence:
                return self.program(sequence[0], seen) if isinstance(sequence[0], ast.AST) else sequence[0].rsplit('/', 1)[-1]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            return self.program(node.right, seen)
        if isinstance(node, ast.Call):
            if name in {'str', 'pathlib.Path', 'Path', 'shutil.which'} and node.args:
                return self.program(node.args[0], seen)
            if isinstance(node.func, ast.Attribute) and node.func.attr in {'resolve', 'absolute'}:
                return self.program(node.func.value, seen)
        return '?'

    def forwarded(self, node):
        scope = self.lexical.scopes.get(node)
        owner = self.lexical.owners.get(scope)
        if not isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)) or owner.name not in self.WRAPPERS or not owner.args.vararg:
            return False
        arg = node.args[0] if node.args else None
        if not isinstance(arg, ast.List) or not arg.elts or self.text(arg.elts[0]) != 'git':
            return False
        tail = arg.elts[-1]
        prefix, _ = self.git_arguments(arg.elts[1:-1], None)
        if prefix or len(scope.bindings.get(owner.args.vararg.arg, [])) != 1:
            return False
        return (isinstance(tail, ast.Starred) and isinstance(tail.value, ast.Name)
                and tail.value.id == owner.args.vararg.arg
                and all(not isinstance(item, ast.Starred) for item in arg.elts[:-1]))

    def contexts(self, node):
        loops = []
        parent = self.parents.get(node)
        while parent is not None:
            if isinstance(parent, ast.For) and isinstance(parent.target, ast.Name):
                values = self.sequence(parent.iter)
                if values is not None:
                    scope = self.lexical.scopes[parent]
                    loops.append([((scope, parent.target.id), value) for value in values])
            parent = self.parents.get(parent)
        # Unsupported or excessive combinations keep their unresolved values.
        if not loops or any(len(values) > 32 for values in loops):
            return [{}]
        return [dict(rows) for rows in product(*loops)] if len(loops) < 3 else [{}]

    def git_program(self, node):
        if isinstance(node, str):
            return node.rsplit('/', 1)[-1] == 'git'
        program = self.program(node)
        return program == 'git' or (program == '?' and 'git' in self.canonical(node).lower())

    @staticmethod
    def assignment(word):
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z_0-9]*=', word))

    def environment_safe(self, node, seen=frozenset()):
        if node is None or isinstance(node, ast.Constant) and node.value is None:
            return True
        if node in seen:
            return False
        if isinstance(node, ast.Name):
            value = self.binding(node)
            return value is not None and self.environment_safe(value, seen | {node})
        return (isinstance(node, ast.Dict) and all(
            key is not None and self.text(key) not in {'?', 'GIT_DIR', 'GIT_WORK_TREE'}
            for key in node.keys))

    def argv_commands(self, argv, repo, executable=None):
        if not argv:
            return [(['?'], repo)] if self.git_program(executable) else []
        program = executable if executable is not None else argv[0]
        name = self.program(program)
        if name == 'env':
            rest = list(argv[1:])
            while rest and self.assignment(self.text(rest[0]) if isinstance(rest[0], ast.AST) else rest[0]):
                value = rest.pop(0)
                word = self.text(value) if isinstance(value, ast.AST) else value
                if word.split('=', 1)[0] in {'GIT_DIR', 'GIT_WORK_TREE'}:
                    repo = None
            return self.argv_commands(rest, repo)
        if name in {'sh', 'bash'} and len(argv) >= 3:
            flag = self.text(argv[1]) if isinstance(argv[1], ast.AST) else argv[1]
            script = self.text(argv[2]) if isinstance(argv[2], ast.AST) else argv[2]
            if flag in {'-c', '-lc'} and script != '?':
                return self.shell_commands(script, repo)
        if self.git_program(program):
            return [self.git_arguments(argv[1:], repo)]
        return []

    def shell_commands(self, text, repo):
        # Substitutions are out of scope. Do not split their internal commands.
        substitution = '$(' in text or '`' in text
        lexer = shlex.shlex(text, posix=True, punctuation_chars='' if substitution else ';&|\n')
        lexer.whitespace = ' \t\r' if not substitution else lexer.whitespace
        lexer.whitespace_split = True
        lexer.commenters = ''
        words = []
        commands = []
        try:
            for word in lexer:
                if not substitution and word and all(char in ';&|\n' for char in word):
                    commands.append(words)
                    words = []
                else:
                    words.append(word)
        except ValueError:
            words.append('?')
        commands.append(words)
        if len(commands) > 1:
            repo = None  # Earlier shell commands may change cwd or captured output.
        unsafe = any(char in text for char in {'$', '`', '<', '>', chr(92)})
        result = []
        for command in commands:
            command_repo = repo
            while command and self.assignment(command[0]):
                if command.pop(0).split('=', 1)[0] in {'GIT_DIR', 'GIT_WORK_TREE'}:
                    command_repo = None
            for argv, identity in self.argv_commands(command, command_repo):
                result.append((['?'] if unsafe else argv, identity))
        return result

    def commands(self, node):
        name = self.canonical(node.func)
        leaf = name.rsplit('.', 1)[-1]
        keywords = {kw.arg: kw.value for kw in node.keywords}
        repo = self.repository(keywords.get('cwd'))
        if not self.environment_safe(keywords.get('env')):
            repo = None
        if isinstance(node.func, ast.Name) and (name in self.WRAPPERS or leaf in self.WRAPPERS):
            args = list(node.args)
            if len(args) > 1 and self.text(args[0]) not in HISTORY_VERBS | self.SETUP_VERBS and not self.text(args[0]).startswith('-'):
                operand = self.repository(args.pop(0))
                repo = (repo, '-C', operand) if repo is not None and operand is not None else None
            return [self.git_arguments(args, repo)]
        if leaf not in self.LAUNCHERS:
            return []
        if not (name.startswith(('subprocess.', 'os.')) or '.subprocess.' in name):
            return []
        arg = node.args[0] if node.args else keywords.get('args', keywords.get('cmd'))
        executable = keywords.get('executable')
        shell = keywords.get('shell')
        if isinstance(shell, ast.Constant) and shell.value is True:
            script = self.text(arg)
            if script == '?':
                values = self.sequence(arg)
                if values:
                    script = self.text(values[0]) if isinstance(values[0], ast.AST) else values[0]
            return self.shell_commands(script, repo) if script != '?' else []
        if leaf.startswith(('exec', 'spawn', 'posix_spawn')):
            offset = 1 if leaf.startswith('spawn') else 0
            executable = node.args[offset] if len(node.args) > offset else keywords.get('path', keywords.get('file'))
            rest = node.args[offset + 1:]
            environment = node.args[-1] if leaf.endswith('e') or leaf.startswith('posix_spawn') else None
            if not self.environment_safe(environment):
                repo = None
            if leaf.startswith(('execl', 'spawnl')):
                argv = list(rest[:-1] if leaf.endswith('e') else rest)
            else:
                argv = self.sequence(rest[0]) if rest else self.sequence(keywords.get('argv', keywords.get('args')))
        else:
            if leaf in {'system', 'popen', 'getoutput', 'getstatusoutput'}:
                return self.shell_commands(self.text(arg), repo)
            if executable is None and isinstance(arg, ast.Call) and self.canonical(arg.func) == 'shlex.split' and arg.args:
                text = self.text(arg.args[0])
                if text != '?':
                    commands = self.shell_commands(text, repo)
                    if len(arg.args) != 1 or arg.keywords:
                        return [(['?'], repo)] if commands else []
                    return commands
            argv = self.sequence(arg)
            if argv is None and executable is None:
                text = self.text(arg)
                if text == '?' and isinstance(arg, ast.Name):
                    scope = self.lexical.scopes.get(arg, self.lexical.root)
                    _, value = scope.lookup(arg.id)
                    if isinstance(value, ast.AST) and self.shell_commands(self.text(value), None):
                        return [(['?'], repo)]
                if text != '?':
                    return self.shell_commands(text, repo)
        return self.argv_commands(argv, repo, executable if executable is not None else arg if not argv else None)

    def git_arguments(self, argv, repo):
        words = [item if isinstance(item, str) else self.text(item) for item in argv]
        index = 0
        while index < len(words) and words[index].startswith('-'):
            option = words[index]
            if option in {'-C', '--git-dir', '--work-tree', '-c'}:
                if index + 1 >= len(words):
                    return ['?'], repo
                value = argv[index + 1]
                operand = self.repository(value) if isinstance(value, ast.AST) else ('path', value)
                repo = (repo, '-C', operand) if option == '-C' and repo is not None and operand is not None else None
                index += 2
            elif option in {'--no-pager', '--no-optional-locks'}:
                index += 1
            else:
                return ['?'], repo
        return words[index:], repo

    def safe_revision(self, revision, repo, launch):
        revision = revision.split(':', 1)[0]
        for part in re.split(r'\.\.\.?', revision):
            if part == 'HEAD':
                continue
            captured = self.heads.get(part)
            if repo is None or captured is None or captured[0] != repo:
                return False
            capture = captured[1]
            scope = self.lexical.scopes[capture]
            if self.lexical.scopes.get(launch) is not scope:
                return False
            start = (capture.lineno, capture.col_offset)
            end = (launch.lineno, launch.col_offset)
            if start >= end or any(
                isinstance(node, ast.Call) and self.canonical(node.func) == 'os.chdir'
                and self.lexical.scopes[node] is scope
                and start < (node.lineno, node.col_offset) < end
                for node in ast.walk(self.tree)
            ):
                return False
        return True

    def inspect(self, argv, repo, launch):
        if not argv:
            return []
        verb, *operands = argv
        if verb in {'init', 'config', 'add', 'status', 'ls-files', 'clean', 'rm', 'mv',
                    'check-ignore', 'check-attr', 'hash-object', 'write-tree',
                    'index-pack', 'count-objects', 'gc'}:
            return []
        if verb == 'commit':
            while operands:
                operand = operands.pop(0)
                if operand in {'-m', '-qm', '-am', '-qam', '-aqm', '-F', '--message', '--file'} and operands:
                    operands.pop(0)
                elif operand not in {'-q', '--quiet', '-a', '--all', '--allow-empty',
                                     '--no-gpg-sign', '--no-verify', '--amend'}:
                    return [operand]
            return []
        if verb == 'rev-parse':
            if operands and all(value in {'--show-toplevel', '--show-prefix', '--git-dir',
                                         '--is-shallow-repository', '--is-inside-work-tree'} for value in operands):
                return []
            return [] if all(value in {'HEAD', '--verify', '--short', '--abbrev-ref',
                                       '--symbolic-full-name'} for value in operands) else operands

        if verb not in HISTORY_VERBS:
            return ['unproved Git command']
        if verb == 'cat-file' and operands and operands[0] in {'blob', 'tree', 'commit', 'tag'}:
            operands.pop(0)
        revisions = []
        while operands:
            operand = operands.pop(0)
            if operand == '--':
                break
            if operand in self.VALUE_OPTIONS:
                if not operands:
                    return ['missing option value']
                operands.pop(0)
            elif operand.startswith('-'):
                option = operand.split('=', 1)[0]
                if option not in self.FLAG_OPTIONS and not ('=' in operand and option in self.VALUE_OPTIONS):
                    return [operand]
            else:
                revisions.append(operand)
                if not self.safe_revision(operand, repo, launch):
                    return revisions
                if verb in {'archive', 'cat-file', 'ls-tree'}:
                    return []
        return []

    def pragma_suppressions(self, launches):
        suppressed, errors = set(), set()
        # Compound statements span other statements; only the smallest owning
        # statement can own a trailing comment. AST columns are UTF-8 bytes.
        statements = [node for node in ast.walk(self.tree) if isinstance(node, ast.stmt)
                      and not hasattr(node, 'body')]
        owners = {}
        for launch in launches:
            parent = self.parents.get(launch)
            while parent is not None and parent not in statements:
                parent = self.parents.get(parent)
            owners[launch] = parent
        lines = self.source.splitlines()
        for token in tokenize.generate_tokens(io.StringIO(self.source).readline):
            if token.type != tokenize.COMMENT or not (match := re.match(r'#\s*pb-history-ok\b(.*)', token.string)):
                continue
            line, column = token.start
            prefix = lines[line - 1][:column]
            position = (line, len(prefix.encode()))
            standalone = not prefix.strip()
            if standalone:
                attached = [node for node in launches if node.lineno == line + 1]
            else:
                candidates = [stmt for stmt in statements
                              if (stmt.lineno, stmt.col_offset) <= position and stmt.end_lineno >= line]
                owner = max(candidates, key=lambda stmt: (stmt.lineno, stmt.col_offset), default=None)
                attached = [node for node in launches if owners[node] is owner and owner is not None]
            on_line = [node for node in launches if node.lineno <= line <= node.end_lineno]
            if not standalone and any(node in attached for node in on_line):
                attached = [node for node in attached if node in on_line]
            reason = match[1]
            if not reason.startswith(':') or len(reason[1:].strip()) < 15:
                errors.add((line, 'pragma', ('pb-history-ok needs a reason of at least 15 characters',)))
            if len(on_line) > 1 or len(attached) > 1:
                errors.add((line, 'pragma', ('pb-history-ok is ambiguous between Git launches',)))
            elif not attached or attached[0] in suppressed:
                errors.add((line, 'pragma', ('pb-history-ok is not attached to an unsuppressed Git launch',)))
            else:
                suppressed.add(attached[0])
        return suppressed, errors

    def sites(self):
        launches = {}
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                commands = []
                for context in self.contexts(node):
                    self.overrides = context
                    commands.extend(self.commands(node))
                if commands:
                    launches[node] = commands
        self.overrides = {}
        suppressed, found = self.pragma_suppressions(launches)
        for node, commands in launches.items():
            if node in suppressed or self.forwarded(node):
                continue
            for argv, repo in commands:
                if bad := self.inspect(argv, repo, node):
                    found.add((node.lineno, argv[0] if argv else '?', tuple(bad)))
        return [(line, verb, list(bad)) for line, verb, bad in sorted(found)]


def history_read_sites(source, root=None, path=None):
    """Reject accidental Git history reads without a same-repository HEAD proof.

    Git programs resolve from literal argv, lexical bindings, repository imports,
    literal shell strings, sh/bash -c/-lc, env and assignment prefixes. Shell
    boundaries include newlines, ;, &&, ||, | and simple & splitting. shell=True
    inspects the script even with an executable override; direct launches use
    the replacement executable.

    Unresolved programs are out of scope unless their name or attribute contains
    "git". functools.partial bindings, asyncio subprocess APIs, command
    substitution and background & chains beyond simple splitting are also out
    of scope. TrustLadder's history-free checkouts are the enforcement backstop:
    its environment text states that a checkout has no history before its base.

    Captured HEAD requires identical literal or lexically resolved cwd and every
    -C operand, a provable launch environment without GIT_DIR/GIT_WORK_TREE, and
    no intervening os.chdir in the same function. Relative paths are not resolved.
    Unknown revisions or unproved repository identity fail closed.

    A trailing # pb-history-ok: <reason> attaches by token column to its owning
    launch statement. A directly preceding pragma must be a standalone comment.
    Each pragma suppresses one launch once; ambiguous and stale pragmas fail.
    Reasons need at least 15 characters. No source is executed or imported.
    """
    return _HistoryGuard(source, root, path).sites()


def history_tree_reads(root):
    found = {}
    for path in sorted((root / 'tests').rglob('*.py')):
        relative = path.relative_to(root / 'tests').as_posix()
        if relative in HISTORY_TOOLS:
            continue
        source = path.read_bytes()
        sites = history_read_sites(source, root, path)
        if sites:
            found[str(path.relative_to(root))] = sites
    return found


def test_historical_inputs_come_from_the_committed_tree():
    assert all(reason.strip() for reason in HISTORY_TOOLS.values())
    assert not history_tree_reads(Path(__file__).resolve().parents[1])


@pytest.mark.parametrize("verb", sorted(HISTORY_VERBS))
@pytest.mark.parametrize("operand", ["'deadbeef:source'", '"main:source"',
    'f"{PIN}:{path}"', 'PIN + ":" + path', 'f"{revision}:{path}"'])
@pytest.mark.parametrize("prefix", ['"git", ', "'git', '-C', root, "])
def test_history_guard_catches_retained_revisions(verb, operand, prefix):
    source = ('PIN = "deadbeef"\ndef read(revision=PIN):\n'
              f'    return subprocess.check_output([{prefix}{verb!r}, {operand}])\n')
    assert history_read_sites(source)


@pytest.mark.parametrize("verb", sorted(HISTORY_VERBS))
def test_history_guard_allows_head(verb):
    assert not history_read_sites(f'subprocess.run(["git", "-C", root, {verb!r}, "HEAD"])')


def test_history_guard_checks_pinned_git_wrappers():
    assert history_read_sites('PIN = "deadbeef"\n_git(root, "show", PIN + ":file")')
    assert history_read_sites('_git(root, "show", f"{revision}:file")')
    assert not history_read_sites('source_head = _git(root, "rev-parse", "HEAD").stdout.strip()\n'
                                 '_git(root, "diff", f"{source_head}...HEAD")')
    assert not history_read_sites('subprocess.run(["git", "cat-file", "blob", "HEAD:file"])')
    assert history_read_sites('subprocess.run(["git", "show", "HEAD", "main"])')



@pytest.mark.parametrize("command", [
    'subprocess.run("git show deadbeef:file", shell=True)',
    'subprocess.check_output(shlex.split("git show deadbeef:file"))',
    'subprocess.Popen("git archive deadbeef", shell=True)',
    'os.system("git show deadbeef:file")',
    'os.popen("git show deadbeef:file")',
    'subprocess.getoutput("git show deadbeef:file")',
    'subprocess.getstatusoutput("git show deadbeef:file")',
    'os.execv("git", ["git", "show", "deadbeef:file"])',
    'os.execv("git", ["disguise", "show", "deadbeef:file"])',
    'subprocess.run(["disguise", "show", "deadbeef:file"], executable="git")',
    'os.execlp("git", "git", "show", "deadbeef:file")',
    'subprocess.run(["git"] + ["show", "deadbeef:file"])',
    'COMMAND=["git","show","deadbeef:file"]\ndef read():\n subprocess.run(COMMAND)',
    'COMMAND="git show deadbeef:file"\ndef read():\n subprocess.run(COMMAND)',
    'COMMAND=["git"]+["show", "HEAD"]\ndef read():\n subprocess.run(COMMAND)',
    'from constants import SHOW\nsubprocess.run(["git", SHOW, "deadbeef:file"])',
    'from constants import GIT\nsubprocess.run([GIT, "show", "deadbeef:file"])',
    'from subprocess import check_output as capture\ncapture("git show deadbeef:file", shell=True)',
    'import subprocess as sp\nsp.run(args=["git", "show", "deadbeef:file"])',
    'subprocess.run(["git", "show", "HEAD", *revisions])',
    'subprocess.run(["git", "show", revision()])',
    'subprocess.run(["git", "checkout", "deadbeef"])',
    'subprocess.run(["git", "rev-parse", "deadbeef"])',
    'subprocess.run(shlex.split("git show HEAD", posix=False))',
    'PIN="HEAD"\nsubprocess.run(["git","show",f"{PIN!r}"])',
    'launch=subprocess.run\nlaunch(["git","show","deadbeef:file"])',
    'from helpers import _git as invoke\ninvoke(root,"show","deadbeef:file")',
    'def _git(*args):\n return subprocess.run(["git","show","deadbeef:file",*args])\n_git("HEAD")',
    'subprocess.run("git show HEAD; git show deadbeef:file", shell=True)',
    'subprocess.run("git show HEAD$(git rev-parse main)", shell=True)',
    'subprocess.run("git show \'HEAD", shell=True)',
    'subprocess.run(["git", "show", "--all"])',
    'subprocess.run(["git", "log", "--branches=main"])',
    'subprocess.run(["git", "show", "--unknown", "HEAD"])',
    'subprocess.run(["git", "cat-file", "--batch"], input=query)',
    'PIN="HEAD"\nPIN="deadbeef"\nsubprocess.run(["git", "show", PIN])',
    'PIN="deadbeef"\ndef unrelated(PIN="HEAD"):\n pass\nsubprocess.run(["git", "show", PIN + ":file"])',
    'PIN="HEAD"\ndef read():\n subprocess.run(["git", "show", PIN])',
    'def read(PIN="HEAD"):\n subprocess.run(["git", "show", PIN])',
    'class Other:\n PIN="HEAD"\nsubprocess.run(["git", "show", PIN])',
    'PIN="deadbeef"\nclass Other:\n PIN="HEAD"\nsubprocess.run(["git", "show", PIN])',
    'source_head=_git(other_root,"rev-parse","HEAD").stdout.strip()\n_git(root,"diff",f"{source_head}...HEAD")',
    'source_head=_git(root,"rev-parse","HEAD").stdout.strip()\nsource_head="deadbeef"\n_git(root,"diff",f"{source_head}...HEAD")',
    'def other():\n source_head=_git(root,"rev-parse","HEAD").stdout.strip()\n_git(root,"diff",f"{source_head}...HEAD")',
])
def test_history_guard_denies_unproved_launches(command):
    assert history_read_sites(command)


@pytest.mark.parametrize("command", [
    'subprocess.check_output(["git", "-C", temporary_repo, "show", "HEAD:file"])',
    'subprocess.run(["git", "-C", temporary_repo, "diff", "HEAD", "--", "file"])',
    'subprocess.Popen(["git", "-C", temporary_repo, "cat-file", "blob", "HEAD:file"])',
    'subprocess.run("git show HEAD:file", shell=True)',
    'subprocess.check_output(shlex.split("git show HEAD:file"))',
    'os.system("git show HEAD:file")',
    'os.popen("git show HEAD:file")',
    'os.execv("git", ["git", "show", "HEAD:file"])',
    'os.execlp("git", "git", "show", "HEAD:file")',
    'subprocess.run(["git"] + ["show", "HEAD:file"])',
    'PIN="HEAD"\nsubprocess.run(["git", "show", PIN + ":file"])',
    'PIN="deadbeef"\ndef read():\n PIN="HEAD"\n subprocess.run(["git", "show", PIN + ":file"])',
    'source_head=_git(root,"rev-parse","HEAD").stdout.strip()\n_git(root,"diff",f"{source_head}...HEAD")',
    'source_head=subprocess.check_output(["git","-C",str(root),"rev-parse","HEAD"], text=True).strip()\nsubprocess.run(["git","-C",root,"diff",f"{source_head}...HEAD"])',
    'command=["git","show","HEAD"]\ncommand[2]="deadbeef:file"\nsubprocess.run(command)',
    'from constants import COMMAND\nsubprocess.run(COMMAND)',
    'subprocess.run([prefix + "git","show","deadbeef:file"])',
    'subprocess.run([sys.executable, "-c", program])',
])
def test_history_guard_accepts_proved_launches(command):
    assert not history_read_sites(command)



@pytest.mark.parametrize("separator", [';', '&&', '||', '|'])
@pytest.mark.parametrize("launch", ['subprocess.run', 'subprocess.Popen', 'subprocess.check_output',
                                     'subprocess.getoutput', 'os.system', 'os.popen'])
def test_history_guard_shell_command_boundaries(separator, launch):
    assert history_read_sites(f'{launch}("echo ready {separator} /usr/bin/git show deadbeef:file")')
    assert not history_read_sites(f'{launch}("echo ready {separator} git show HEAD:file")')


@pytest.mark.parametrize("launch", [
    'os.execve("/usr/bin/git", ["alias", "show", "deadbeef:file"], env)',
    'os.execle("git", "alias", "show", "deadbeef:file", env)',
    'os.spawnv(os.P_WAIT, "git", ["alias", "show", "deadbeef:file"])',
    'os.spawnvpe(os.P_WAIT, "git", ["alias", "show", "deadbeef:file"], env)',
    'os.spawnl(os.P_WAIT, "git", "alias", "show", "deadbeef:file")',
    'os.spawnlpe(os.P_WAIT, "git", "alias", "show", "deadbeef:file", env)',
    'os.posix_spawn("git", ["alias", "show", "deadbeef:file"], env)',
    'subprocess.run([settings.GIT, "show", revision])',
    'subprocess.run(git_command)',
    'subprocess.getoutput(cmd="git show deadbeef:file")',
    'subprocess.run([GIT_PROGRAM, "show", revision])',
    'subprocess.run(["alias", "show", revision], executable=git_executable)',
    'GIT="/usr/bin/git"\ndef read():\n subprocess.run([GIT, "show", revision])',
])
def test_history_guard_identifies_git_programs(launch):
    assert history_read_sites(launch)


@pytest.mark.parametrize("command", [
    'subprocess.run(command)', 'os.system(command)', 'os.popen(command)',
    'subprocess.run([executable, "show", "deadbeef:file"])',
    'subprocess.run(["python", "show", "deadbeef:file"])',
    'subprocess.run(["git", "show", "deadbeef:file"], executable="python")',
    'os.execv(executable, args)', 'os.spawnv(os.P_WAIT, executable, args)',
    'GIT="python"\nsubprocess.run([GIT, "show", revision])',
])
def test_history_guard_unresolved_programs_are_out_of_scope(command):
    assert not history_read_sites(command)


@pytest.mark.parametrize("command, finding", [
    ('from constants import PROGRAM\nsubprocess.run([PROGRAM, "show", "deadbeef:file"])', True),
    ('import constants as c\nsubprocess.run([c.PROGRAM, c.VERB, "HEAD:file"])', False),
    ('from constants import COMMAND\nsubprocess.run(COMMAND)', True),
    ('from constants import PROGRAM as launch\nsubprocess.run([launch, "show", revision])', True),
    ('from constants import OTHER\nsubprocess.run([OTHER, "show", revision])', False),
])
def test_history_guard_resolves_repository_imports(tmp_path, command, finding):
    directory = tmp_path / 'tests'
    directory.mkdir()
    (directory / 'constants.py').write_text(
        'PROGRAM="/usr/bin/git"\nVERB="show"\nCOMMAND=[PROGRAM, VERB, "deadbeef:file"]\n'
        'OTHER="python"\nraise RuntimeError("Scanning must never execute this module")\n')
    path = directory / 'example.py'
    path.write_text(command)
    assert bool(history_tree_reads(tmp_path)) is finding


@pytest.mark.parametrize("source", [
    '# pb-history-ok: Reads objects supplied by the fixture bundle.\nsubprocess.run(["git", "show", "fixture:file"])',
    'subprocess.run(["git", "show", "fixture:file"])  # pb-history-ok: Reads objects supplied by the fixture bundle.',
])
def test_history_guard_valid_pragma(source):
    assert not history_read_sites(source)


@pytest.mark.parametrize("comment", [
    '# pb-history-ok:', '# pb-history-ok: short', '# pb-history-ok',
])
def test_history_guard_invalid_pragma(comment):
    assert history_read_sites(comment + '\nsubprocess.run(["git", "show", "HEAD:file"])')


@pytest.mark.parametrize("source", [
    '# pb-history-ok: Reads objects supplied by the fixture bundle.\nsubprocess.run(["python", "proof.py"])',
    '# pb-history-ok: Reads objects supplied by the fixture bundle.\nvalue = 1',
    '# pb-history-ok: Reads objects supplied by the fixture bundle.\n\nsubprocess.run(["git", "show", "HEAD:file"])',
])
def test_history_guard_stale_pragma(source):
    assert history_read_sites(source)



def test_history_guard_pragma_does_not_exempt_later_launches():
    source = ('# pb-history-ok: Reads objects supplied by the fixture bundle.\n'
              'subprocess.run(["git", "show", "fixture:file"])\n'
              'subprocess.run(["git", "show", "deadbeef:file"])\n')
    assert [site[0] for site in history_read_sites(source)] == [3]


def test_history_guard_shell_head_capture_needs_a_single_repository():
    source = ('head = subprocess.check_output("echo ready && git rev-parse HEAD").strip()\n'
              'subprocess.run(["git", "diff", f"{head}...HEAD"])')
    assert history_read_sites(source)



@pytest.mark.parametrize("revision, finding", [("deadbeef:file", True), ("HEAD:file", False)])
@pytest.mark.parametrize("template", [
    'subprocess.run({script!r}, shell=True, executable="/bin/bash")',
    'from subprocess import run as launch\nlaunch({script!r}, shell=True, executable="/bin/bash")',
    'COMMAND={script!r}\nsubprocess.run(COMMAND, shell=True, executable="/bin/bash")',
    'subprocess.run([{script!r}], shell=True, executable="/bin/bash")',
    'subprocess.Popen({script!r}, shell=True, executable="/bin/sh")',
    'subprocess.check_output({script!r}, shell=True, executable="python")',
    'subprocess.run(["sh", "-c", {script!r}])',
    'subprocess.run(["/bin/sh", "-c", {script!r}])',
    'subprocess.run(["bash", "-c", {script!r}])',
    'subprocess.run(["bash", "-lc", {script!r}])',
    'subprocess.run(["/bin/bash", "-lc", {script!r}])',
    'subprocess.run(["alias", "-c", {script!r}], executable="/bin/sh")',
    'os.execv("/bin/bash", ["alias", "-lc", {script!r}])',
    'subprocess.run({nested!r}, shell=True)',
    'subprocess.run(["env", "git", "show", {revision!r}])',
    'subprocess.run(["/usr/bin/env", "LANG=C", "HOME=/fixture", "git", "show", {revision!r}])',
    'os.execv("/usr/bin/env", ["alias", "LANG=C", "git", "show", {revision!r}])',
    'subprocess.run({env_script!r}, shell=True)',
    'subprocess.run({plain_env!r}, shell=True)',
    'subprocess.run({simple_background!r}, shell=True)',
    'os.system({assigned!r})',
    'subprocess.getoutput({newlines!r})',
    'subprocess.run(["bash", "-lc", {assigned!r}])',
])
def test_history_guard_plausible_shell_forms(template, revision, finding):
    script = f"git show {revision}"
    source = template.format(script=script, revision=revision,
                             nested=f"/bin/bash -lc '{script}'",
                             env_script=f"env LANG=C HOME=/fixture {script}",
                             plain_env=f"env {script}",
                             simple_background=f"echo ready & {script}",
                             assigned=f"LANG=C HOME=/fixture {script}",
                             newlines=f"echo ready\nLANG=C env HOME=/fixture {script}\necho done")
    assert bool(history_read_sites(source)) is finding


@pytest.mark.parametrize("source", [
    'subprocess.run(["git", "show", "deadbeef:file"], executable="python")',
    'subprocess.run(shlex.split("git show deadbeef:file"), executable="python")',
    'subprocess.run(["git", "show", "deadbeef:file"], shell=False, executable="/bin/sh")',
    'subprocess.run(["sh", "-c", "echo git show deadbeef:file"])',
    'subprocess.run(["bash", "-lc", script])',
    'subprocess.run(["env", "LANG=C", "python", "show", "deadbeef:file"])',
    'subprocess.run("LANG=C echo git show deadbeef:file", shell=True)',
    'subprocess.run("echo \'git show deadbeef:file\\nmore\'", shell=True)',
])
def test_history_guard_shell_non_git_controls(source):
    assert not history_read_sites(source)


@pytest.mark.parametrize("source", [
    'from functools import partial\nlaunch=partial(subprocess.run, ["git","show","deadbeef:file"])\nlaunch()',
    'functools.partial(subprocess.run, ["git","show","deadbeef:file"])()',
    'asyncio.create_subprocess_exec("git", "show", "deadbeef:file")',
    'asyncio.create_subprocess_shell("git show deadbeef:file")',
    'subprocess.run("echo $(git show deadbeef:file)", shell=True)',
    'os.system("echo `git show deadbeef:file`")',
    'subprocess.run("echo $(echo ready && git show deadbeef:file)", shell=True)',
    'subprocess.run("(git show deadbeef:file) & wait", shell=True)',
])
def test_history_guard_documented_exotic_forms_are_out_of_scope(source):
    assert not history_read_sites(source)


@pytest.mark.parametrize("source, bad_lines, pragma_lines", [
    ('subprocess.run(["git","show","fixture:file"])  # pb-history-ok: Reads fixture-created repository objects.\n'
     'subprocess.run(["git","show","deadbeef:file"])', [2], []),
    ('subprocess.run(["python","proof.py"])  # pb-history-ok: Reads fixture-created repository objects.\n'
     'subprocess.run(["git","show","deadbeef:file"])', [2], [1]),
    ('subprocess.run(["git","show","a:file"]); subprocess.run(["git","show","b:file"])  '
     '# pb-history-ok: Reads fixture-created repository objects.', [1, 1], [1]),
    ('# pb-history-ok: Reads fixture-created repository objects.\n'
     'subprocess.run(["git","show","a:file"]); subprocess.run(["git","show","b:file"])', [2, 2], [1]),
    ('subprocess.run(["git","show","a:file"]); subprocess.run(["python","proof.py"])  '
     '# pb-history-ok: Reads fixture-created repository objects.', [1], [1]),
    ('subprocess.run(["python","proof.py"]); subprocess.run(["git","show","a:file"])  '
     '# pb-history-ok: Reads fixture-created repository objects.', [], []),
    ('value="é"; subprocess.run(["python","proof.py"]); subprocess.run(["git","show","a:file"])  '
     '# pb-history-ok: Reads fixture-created repository objects.', [], []),
    ('subprocess.run(  # pb-history-ok: Reads fixture-created repository objects.\n'
     ' ["git","show","a:file"])\nsubprocess.run(["git","show","b:file"])', [3], []),
    ('subprocess.run(\n ["git","show","a:file"])  # pb-history-ok: Reads fixture-created repository objects.\n'
     'subprocess.run(["git","show","b:file"])', [3], []),
    ('subprocess.run(["git","show","a:file"])  # pb-history-ok: Reads fixture-created repository objects.\n'
     '# pb-history-ok: Reads fixture-created repository objects.\n'
     'subprocess.run(["git","show","b:file"])', [], []),
    ('# pb-history-ok: Reads fixture-created repository objects.\n'
     'subprocess.run(["git","show","a:file"])  # pb-history-ok: Reads fixture-created repository objects.', [], [2]),
])
def test_history_guard_pragma_precision(source, bad_lines, pragma_lines):
    sites = history_read_sites(source)
    assert sorted(line for line, verb, _ in sites if verb != 'pragma') == bad_lines
    assert sorted(line for line, verb, _ in sites if verb == 'pragma') == pragma_lines


@pytest.mark.parametrize("capture, use, finding", [
    ('["git","-C","repo","rev-parse","HEAD"], cwd="/a"',
     '["git","-C","repo","diff",head], cwd="/b"', True),
    ('["git","-C","repo","rev-parse","HEAD"], cwd="/a"',
     '["git","-C","repo","diff",head], cwd="/a"', False),
    ('["git","-C","a","-C","repo","rev-parse","HEAD"]',
     '["git","-C","b","-C","repo","diff",head]', True),
    ('["git","-C","a","-C","repo","rev-parse","HEAD"]',
     '["git","-C","a","-C","repo","diff",head]', False),
    ('["git","-C","/repo","rev-parse","HEAD"], cwd="/a"',
     '["git","-C","/repo","diff",head], cwd="/b"', True),
    ('["git","rev-parse","HEAD"], env={"GIT_DIR":"/a"}',
     '["git","diff",head], env={"GIT_DIR":"/b"}', True),
    ('["git","rev-parse","HEAD"], env={"GIT_WORK_TREE":"/repo"}',
     '["git","diff",head], env={"GIT_WORK_TREE":"/repo"}', True),
    ('["git","rev-parse","HEAD"]', '["git","diff",head], env=environment', True),
    ('["git","rev-parse","HEAD"], env=environment', '["git","diff",head]', True),
    ('["git","rev-parse","HEAD"], env={"LANG":"C"}',
     '["git","diff",head], env={"LANG":"C"}', False),
    ('["git","rev-parse","HEAD"], env=None', '["git","diff",head]', False),
    ('["git","rev-parse","HEAD"], cwd=None', '["git","diff",head]', False),
    ('["git","rev-parse","HEAD"], env={**os.environ}', '["git","diff",head]', True),
    ('["git","rev-parse","HEAD"]', '["git","diff",head], env={"GIT_DIR":"/repo"}', True),
    ('["env","GIT_DIR=/repo","git","rev-parse","HEAD"]',
     '["env","GIT_DIR=/repo","git","diff",head]', True),
    ('"GIT_WORK_TREE=/repo git rev-parse HEAD", shell=True',
     '["git","diff",head]', True),
    ('["git","--work-tree","/repo","rev-parse","HEAD"]',
     '["git","--work-tree","/repo","diff",head]', True),
    ('["git","--git-dir","/repo","rev-parse","HEAD"]',
     '["git","--git-dir","/repo","diff",head]', True),
])
def test_history_guard_captured_head_repository_proof(capture, use, finding):
    source = f'head=subprocess.check_output({capture}).strip()\nsubprocess.run({use})'
    assert bool(history_read_sites(source)) is finding


@pytest.mark.parametrize("prefix, between, suffix, finding", [
    ('', '', '', False),
    ('', 'os.chdir("/other")\n', '', True),
    ('', 'from os import chdir as move\nmove("/other")\n', '', True),
    ('os.chdir("/before")\n', '', '', False),
    ('', '', '\nos.chdir("/after")', False),
    ('', 'def unrelated():\n os.chdir("/other")\n', '', False),
])
def test_history_guard_captured_head_chdir(prefix, between, suffix, finding):
    body = (prefix + 'head=subprocess.check_output(["git","rev-parse","HEAD"]).strip()\n'
            + between + 'subprocess.run(["git","diff",head])' + suffix)
    source = 'def check():\n' + '\n'.join(' ' + line for line in body.splitlines())
    assert bool(history_read_sites(source)) is finding


@pytest.mark.parametrize("use, finding", [('alias', False), ('other', True)])
def test_history_guard_resolves_repository_aliases(use, finding):
    source = ('root="/fixture"\nalias=root\nother="/other"\n'
              'head=subprocess.check_output(["git","-C",root,"rev-parse","HEAD"]).strip()\n'
              f'subprocess.run(["git","-C",{use},"diff",head])')
    assert bool(history_read_sites(source)) is finding


def test_every_committed_history_input_has_a_verified_blob():
    pins = set()
    for revision, row in history_inputs.history_manifest()["revisions"].items():
        for path, pin in row["files"].items():
            history_inputs.source_bytes(revision, path)
            pins.add(pin["sha256"])
    assert pins == history_inputs.history_blobs().keys()


@pytest.mark.parametrize("damage", ["bytes", "sha256"])
def test_history_inputs_refuse_damaged_bytes(monkeypatch, damage):
    revision, row = next(iter(history_inputs.history_manifest()["revisions"].items()))
    path, pin = next(iter(row["files"].items()))
    raw = history_inputs.source_bytes(revision, path)
    damaged = raw + b"!" if damage == "bytes" else bytes([raw[0] ^ 1]) + raw[1:]
    monkeypatch.setattr(history_inputs, "history_blobs", lambda: {pin["sha256"]: damaged})
    with pytest.raises(AssertionError, match="byte count differs" if damage == "bytes" else "digest differs"):
        history_inputs.source_bytes(revision, path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    reads = history_tree_reads(parser.parse_args().root.resolve())
    for path, sites in reads.items():
        print(path, sites)
    raise SystemExit(bool(reads))
