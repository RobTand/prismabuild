"""Git identity proofs for reviewed sources and sealed generation snapshots."""
from pathlib import Path
import re
import subprocess
from .compatibility_window import WindowError
from .digest_primitives import raw_sha256
from .generation_receipt import GenerationReceipt
from .publication_contract import publication_sources


def git(checkout, *args):
    try:
        return subprocess.check_output(['git', '-C', str(checkout), *args])
    except subprocess.CalledProcessError as exc:
        raise WindowError('release Git identity unavailable') from exc


def resolve(checkout, revision):
    return git(checkout, 'rev-parse', '--verify', revision + '^{commit}').decode().strip()


def sources(checkout, commit):
    paths = git(checkout, 'ls-tree', '-rz', '--name-only', commit).decode().split('\0')
    return publication_sources(filter(None, paths))


def closure_stamp_only(changed, subdirectory='.'):
    return [name for name in changed if not (
        re.fullmatch(r'\.pbrun-closure\.[0-9a-f]+\.json', Path(name).name)
        and str(Path(name).parent) == (subdirectory or '.'))]


def release_source_identity(*, checkout, requested_commit, main_ref):
    requested = resolve(checkout, requested_commit)
    main = resolve(checkout, main_ref)
    chain = git(checkout, 'rev-list', '--first-parent', main).decode().splitlines()
    if requested not in chain:
        raise WindowError('requested release is not on main first-parent chain')
    before, after = sources(checkout, requested), sources(checkout, main)
    published = set(before) | set(before.values()) | set(after) | set(after.values())
    changed = set(git(checkout, 'diff', '--no-renames', '--name-only', '-z', requested, main).decode().split('\0'))
    if published & changed:
        raise WindowError('later main changes publication runtime source: ' + ', '.join(sorted(published & changed)))
    return {'requested_commit': requested, 'main_commit': main}


def release_snapshot_identity(*, checkout, requested_commit, generation):
    root = Path(generation)
    requested = resolve(checkout, requested_commit)
    try:
        receipt_path = root / 'RUNTIME_VERSION.json'
        if root.is_symlink() or root.stat().st_mode & 0o222 or receipt_path.is_symlink() or receipt_path.stat().st_mode & 0o222:
            raise WindowError('generation receipt must be sealed')
        raw = receipt_path.read_bytes()
        receipt = GenerationReceipt.capture(raw)
        document = receipt.require_generation(root.name)
        snapshot = receipt.require_commit()
        if document.get('dirty') is not False:
            raise WindowError('generation is dirty')
        parents = git(checkout, 'rev-list', '--parents', '-n', '1', snapshot).decode().split()
        if parents != [snapshot, requested]:
            raise WindowError('snapshot must have exactly one requested source parent')
        changes = git(checkout, 'diff', '--no-renames', '--name-status', requested, snapshot).decode().splitlines()
        if len(changes) > 1:
            raise WindowError('snapshot has more than one closure stamp difference')
        for change in changes:
            status, name = change.split('\t')
            if status != 'A' or closure_stamp_only([name]):
                raise WindowError('snapshot has a non-closure source difference')
            mode = git(checkout, 'ls-tree', snapshot, '--', name).decode().split()[0]
            if mode != '100644':
                raise WindowError('snapshot closure mode differs')
        # Sealing covers directory custody as well as individual member bytes;
        # a symlinked parent must not redirect a receipt member outside G.
        for path in root.rglob('*'):
            if path.is_symlink() or path.stat().st_mode & 0o222:
                raise WindowError('generation contains an unsealed path: ' + str(path))
        mapping = sources(checkout, snapshot)
        if set(receipt.files) != set(mapping):
            raise WindowError('generation publication membership differs')
        for member, source in mapping.items():
            path = receipt.member_path(root, member)
            if (path.is_symlink() or path.stat().st_mode & 0o222
                    or not receipt.matches_file(member, path)
                    or raw_sha256(git(checkout, 'show', snapshot + ':' + source)) != receipt.member_digest(member)):
                raise WindowError('generation receipt member differs: ' + member)
        return {'requested_commit': requested, 'snapshot_commit': snapshot,
                'receipt_sha256': raw_sha256(raw)}
    except (OSError, ValueError, KeyError) as exc:
        raise WindowError('invalid sealed generation receipt/member: ' + str(exc)) from exc


def portable_release_command(*, checkout, command):
    root = Path(checkout).resolve()
    result = []
    for token in command:
        path = Path(token)
        if path.is_absolute():
            try:
                token = path.relative_to(root).as_posix()
            except ValueError:
                pass
        result.append(token)
    return result
