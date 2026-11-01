"""Worker-only programs for owned pytest attempt scratch."""

#: Loads ``owned_tree`` (the owner lock, retention clock and removal both
#: attempt programs below use), so the shard's interpreter needs no
#: PrismaBuild on its path.  ``@SOURCE@`` becomes the module's exact bytes,
#: zlib-compressed and base85-encoded: the whole program is one ``python -c``
#: argument, which Linux caps at ``MAX_ARG_STRLEN`` (128 KiB), and the
#: largest program already used 94% of that before this module.
OWNED_TREE_PROGRAM = """\
import base64 as _pb_base64
import zlib as _pb_zlib
_pb_owned_tree = {}
exec(compile(_pb_zlib.decompress(_pb_base64.b85decode(@SOURCE@)),
             "<pbtest owned_tree>", "exec"), _pb_owned_tree)
"""


ROOTS_PROGRAM = """\
_pb_temp_roots = {}
exec(compile(_pb_zlib.decompress(_pb_base64.b85decode(@ROOT_SOURCE@)),
             "<pbtest scratch roots>", "exec"), _pb_temp_roots)
"""


#: The worker-only source a sealed ``--basetemp`` executes before anything
#: else (#1469).  The coordinator never runs it and never needs the root to
#: exist; the worker refuses an unusable root here, before pytest, rather
#: than letting a qualification problem surface mid-run.  ``@ROOT@`` becomes
#: the sealed root.  The attempt's ``<root>/<key>/<attempt>`` holds an owner
#: lock beside pytest's leaf: exit 0 removes the attempt directory (and the
#: key directory it empties); any other ending starts its retention clock,
#: and a later attempt under the same root reaps it once the window passes.
#: The root itself is never removed or changed.
BASETEMP_PROGRAM = """\
import os as _pb_os
import re as _pb_re
import stat as _pb_stat
import sys as _pb_sys
import tempfile as _pb_tempfile
import uuid as _pb_uuid


def _pb_refuse(reason):
    raise SystemExit("pbtest: cannot use requested --basetemp: " + reason)


_pb_root = @ROOT@
_pb_key = _pb_os.environ.get("PRISMABUILD_ACTION_KEY", "")
if not _pb_key or "/" in _pb_key or _pb_key in (".", ".."):
    _pb_refuse(
        "the worker did not provide a safe PRISMABUILD_ACTION_KEY, so the "
        "action-owned namespace cannot be derived")
_pb_attempt = _pb_os.environ.get("PRISMABUILD_ACTION_NONCE", "")
if _pb_attempt:
    if len(_pb_attempt) != 32 or any(
            character not in "0123456789abcdef" for character in _pb_attempt):
        _pb_refuse(
            "PRISMABUILD_ACTION_NONCE is not a 32-hex attempt identity")
else:
    # No launcher-provided attempt identity on this lane: mint one per
    # execution, so a second attempt of the same action key never shares a
    # namespace -- pytest deletes its basetemp, and the wipe must stay
    # inside this attempt's own directory.
    _pb_attempt = _pb_uuid.uuid4().hex
# A relative root is confined to this attempt's own materialized checkout,
# which is the shard's working directory.
_pb_root = _pb_os.path.abspath(_pb_root)
try:
    _pb_stat_result = _pb_os.lstat(_pb_root)
except OSError as _pb_exc:
    _pb_refuse(f"{_pb_root} ({_pb_exc})")
if _pb_stat.S_ISLNK(_pb_stat_result.st_mode):
    _pb_refuse(
        f"{_pb_root} is a symlink; the sealed root is the real directory "
        "or nothing")
if not _pb_stat.S_ISDIR(_pb_stat_result.st_mode):
    _pb_refuse(f"{_pb_root} is not a directory")
if _pb_stat_result.st_uid != _pb_os.geteuid():
    _pb_refuse(
        f"{_pb_root} is owned by uid {_pb_stat_result.st_uid}, not this "
        f"action's uid {_pb_os.geteuid()}")
_pb_check_capacity(_pb_root)
# pytest receives the action-owned leaf, never the sealed root itself:
# pytest deletes its basetemp at startup, and the sealed root belongs to
# every attempt of the action, not to this one.
_pb_attempt_directory = _pb_os.path.join(_pb_root, _pb_key, _pb_attempt)
_pb_derived = _pb_os.path.join(_pb_attempt_directory, "pytest")
_pb_flags = _pb_os.O_RDONLY | _pb_os.O_DIRECTORY | _pb_os.O_NOFOLLOW | _pb_os.O_CLOEXEC
try:
    _pb_parent = _pb_os.open(_pb_root, _pb_flags)
    _pb_temp_roots["register_pbtest_root"](_pb_parent, basetemp=True)
    try:
        _pb_owned_tree["reap_dead_trees"](_pb_parent, _pb_re.compile("[0-9a-f]{64}"),
                                          inner=_pb_re.compile("[0-9a-f]{32}"))
    except Exception as _pb_exc:
        print("pbtest: basetemp expiry skipped: " + str(_pb_exc), file=_pb_sys.stderr)
    try:
        _pb_os.makedirs(_pb_attempt_directory, exist_ok=True)
    except FileNotFoundError:
        # A finished attempt of this key removed the key directory it had
        # emptied between makedirs' two mkdir calls; make it again.
        _pb_os.makedirs(_pb_attempt_directory, exist_ok=True)
    _pb_key_fd = _pb_os.open(_pb_key, _pb_flags, dir_fd=_pb_parent)
    _pb_held = _pb_os.open(_pb_attempt, _pb_flags, dir_fd=_pb_key_fd)
    try:
        _pb_lock = _pb_owned_tree["take_owner_lock"](_pb_held, _pb_owned_tree["LOCK_NAME"])
    finally:
        _pb_os.close(_pb_held)
    _pb_os.makedirs(_pb_derived, exist_ok=True)
    with _pb_tempfile.TemporaryFile(dir=_pb_derived):
        pass
except (OSError, ValueError) as _pb_exc:
    _pb_refuse(f"{_pb_derived} ({_pb_exc})")
# The pair goes after the collection JSON (sys.argv[1]) and before the
# trailing file arguments pbtest_outcomes checks the argv tail against.
_pb_sys.argv[2:2] = ["--basetemp", _pb_derived]


def _pb_run_and_clean_attempt_base(_pb_run):
    _pb_code = _pb_run()
    try:
        _pb_owned_tree["release_owned_tree"](
            _pb_key_fd, _pb_attempt, _pb_lock, remove=int(_pb_code) == 0)
        if int(_pb_code) == 0:
            _pb_os.rmdir(_pb_key, dir_fd=_pb_parent)
    except OSError as _pb_exc:
        if _pb_os.path.isdir(_pb_attempt_directory):
            print("pbtest: attempt base temp kept for expiry: " + str(_pb_exc),
                  file=_pb_sys.stderr)
    return _pb_code
"""

#: Default shards retain the short external TMPDIR/pb-<suffix>/pytest layout.
#: Handled endings remove scratch, including failed tests (#2449). Killed
#: owners still use the registered, lock-proven recovery path (#1710).
ATTEMPT_TEMP_PROGRAM = """\
import os as _pb_os
import re as _pb_re
import signal as _pb_signal
import sys as _pb_sys
import tempfile as _pb_tempfile


def _pb_run_and_clean_attempt_base(_pb_run):
    return _pb_run()


_pb_tmpdir = _pb_os.environ.get("TMPDIR", "")
_pb_key = _pb_os.environ.get("PRISMABUILD_ACTION_KEY", "")
_pb_attempt = _pb_os.environ.get("PRISMABUILD_ACTION_NONCE", "")
if _pb_attempt and (len(_pb_attempt) != 32 or any(
        character not in "0123456789abcdef" for character in _pb_attempt)):
    raise SystemExit(
        "pbtest: PRISMABUILD_ACTION_NONCE is not a 32-hex attempt identity")
if (_pb_tmpdir and _pb_key and "/" not in _pb_key
        and _pb_key not in (".", "..")):
    _pb_flags = _pb_os.O_RDONLY | _pb_os.O_DIRECTORY | _pb_os.O_CLOEXEC
    try:
        _pb_parent = _pb_os.open(_pb_tmpdir, _pb_flags)
        _pb_temp_roots["register_pbtest_root"](_pb_parent)
        try:
            _pb_owned_tree["reap_dead_trees"](_pb_parent, _pb_re.compile("pb-[a-z0-9_]{8}"))
        except Exception as _pb_exc:
            print("pbtest: base temp expiry skipped: " + str(_pb_exc), file=_pb_sys.stderr)
        # mkdtemp creates an exclusive namespace; no action identity is truncated.
        _pb_attempt_directory = _pb_tempfile.mkdtemp(prefix="pb-", dir=_pb_tmpdir)
        _pb_name = _pb_os.path.basename(_pb_attempt_directory)
        _pb_held = _pb_os.open(_pb_name, _pb_flags | _pb_os.O_NOFOLLOW, dir_fd=_pb_parent)
        try:
            _pb_lock = _pb_owned_tree["take_owner_lock"](_pb_held, _pb_owned_tree["LOCK_NAME"])
        finally:
            _pb_os.close(_pb_held)
        _pb_owned = _pb_os.path.join(_pb_attempt_directory, "pytest")
        _pb_os.mkdir(_pb_owned)
        with _pb_tempfile.TemporaryFile(dir=_pb_owned):
            pass
    except (OSError, ValueError) as _pb_exc:
        raise SystemExit("pbtest: cannot use the attempt base temp: " + str(_pb_exc))
    _pb_sys.argv[2:2] = ["--basetemp", _pb_owned]


    def _pb_run_and_clean_attempt_base(_pb_run):
        def _pb_terminated(signum, frame):
            raise SystemExit(128 + signum)

        _pb_previous = _pb_signal.signal(_pb_signal.SIGTERM, _pb_terminated)
        try:
            return _pb_run()
        finally:
            _pb_signal.signal(_pb_signal.SIGTERM, _pb_previous)
            try:
                _pb_owned_tree["release_owned_tree"](
                    _pb_parent, _pb_name, _pb_lock, remove=True)
            except OSError as _pb_exc:
                print("pbtest: attempt base temp kept for expiry: " + str(_pb_exc),
                      file=_pb_sys.stderr)


"""
