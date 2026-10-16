"""Execute a production compatibility adapter with an attested import boundary."""
from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.machinery
import importlib.util
import json
import marshal
import os
import platform
from pathlib import Path
import runpy
import sys
import sysconfig
import types
from threading import local


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def isolate(root):
    if not sys.flags.isolated or not sys.flags.no_site:
        raise RuntimeError("compatibility consumer requires python -I -S")
    standard = Path(sysconfig.get_path("stdlib")).resolve()
    extensions = Path(sysconfig.get_config_var("DESTSHARED") or standard / "lib-dynload").resolve()
    stdlib = {standard, extensions}
    sys.path[:] = [str(root / "src"), str(root / "tools/fleet"), str(root / "tools")]
    sys.path.extend(str(path) for path in sorted(stdlib))
    sys.dont_write_bytecode = True
    return stdlib


def attest(root, stdlib, manifest):
    for entry in sys.path:
        path = Path(entry).resolve()
        if not path.is_relative_to(root) and path not in stdlib:
            raise RuntimeError(f"search path escaped closure and stdlib: {path}")
    observed, standard_imports = {}, {}
    for name, module in sorted(sys.modules.items()):
        filename = getattr(module, "__file__", None)
        if not filename or name == "__main__" or filename.startswith("<"):
            continue
        path = Path(filename).resolve()
        if path.is_relative_to(root):
            origin = getattr(getattr(module, "__spec__", None), "origin", None)
            if not origin or Path(origin).resolve() != path:
                raise RuntimeError(f"retained module origin disagrees: {name}")
            relative = str(path.relative_to(root))
            actual = sha(path)
            if manifest is not None and manifest["files"].get(relative) != actual:
                raise RuntimeError(f"unattested retained import: {name}: {relative}")
            observed[name] = {"path": str(path), "relative": relative, "sha256": actual}
        elif (not any(path.is_relative_to(base) for base in stdlib)
              or {"site-packages", "dist-packages"}.intersection(path.parts)):
            raise RuntimeError(f"import outside closure and stdlib: {name}: {path}")
        else:
            standard_imports[name] = {"path": str(path), "sha256": sha(path),
                                      "origin": getattr(getattr(module, "__spec__", None), "origin", None)}
    if not observed:
        raise RuntimeError("adapter reached no production imports")
    return observed, standard_imports


def load_module(root, name):
    if name != "docker":
        return importlib.import_module(name)
    path = root / "tools/fleet/docker"
    if not path.is_file():
        path = root / "tools/docker"
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


def private_writes_only(store):
    store = store.resolve()
    live = Path("/mnt/shared/prismabuild-fleet")
    opened = local()

    def path_of(value, dir_fd=None):
        if value is None or isinstance(value, int):
            return None
        path = Path(os.fsdecode(value))
        if not path.is_absolute() and isinstance(dir_fd, int) and dir_fd >= 0:
            path = Path(os.readlink(f"/proc/self/fd/{dir_fd}")) / path
        return path.resolve()

    original_open = os.open

    def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
        prior = getattr(opened, "path", None)
        opened.path = path_of(path, dir_fd)
        try:
            return original_open(path, flags, mode, dir_fd=dir_fd)
        finally:
            opened.path = prior

    os.open = guarded_open
    if original_open in os.supports_dir_fd:
        os.supports_dir_fd.add(guarded_open)

    def audit(event, args):
        if event == "open":
            path = getattr(opened, "path", None) or path_of(args[0])
            if path is None:
                return
            flags = args[2] if isinstance(args[2], int) else 0
            writing = flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
            if writing and path != Path("/dev/null") and not path.is_relative_to(store):
                raise RuntimeError(f"compatibility writer escaped private store: {path}")
            if any(path.is_relative_to(live / name) for name in ("pb-queue", "cas")):
                raise RuntimeError("compatibility adapter attempted a live-store read")
        elif event in ("os.mkdir", "os.remove", "os.rmdir", "os.chmod", "os.chown", "os.utime",
                       "os.rename", "os.link", "os.symlink"):
            if event in ("os.rename", "os.link"):
                operands = ((args[0], args[2]), (args[1], args[3]))
            elif event == "os.symlink":
                operands = ((args[1], args[2]),)
            else:
                index = {"os.mkdir": 2, "os.remove": 1, "os.rmdir": 1,
                         "os.chmod": 2, "os.chown": 3, "os.utime": 3}[event]
                operands = ((args[0], args[index]),)
            for value, descriptor in operands:
                path = path_of(value, descriptor)
                if event == "os.mkdir" and path is not None and store.is_relative_to(path) and path.is_dir():
                    continue
                if path is not None and not path.is_relative_to(store):
                    raise RuntimeError(f"compatibility mutation escaped private store: {path}")
    sys.addaudithook(audit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--adapters", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    manifest = json.loads(args.manifest.read_bytes()) if args.manifest else None
    stdlib = isolate(root)
    request = json.load(sys.stdin)
    args.store.mkdir(parents=True, exist_ok=True)
    (args.store / "tmp").mkdir(exist_ok=True)
    os.environ["TMPDIR"] = str(args.store / "tmp")
    private_writes_only(args.store)
    output = {"isolated": bool(sys.flags.isolated), "no_site": bool(sys.flags.no_site),
              "affinity": sorted(os.sched_getaffinity(0)),
              "pid": os.getpid(),
              "sys_path": list(sys.path), "driver_sha256": sha(Path(__file__)),
              "python": sys.version, "machine": platform.machine(),
              "interpreter": {"path": sys.executable, "resolved_path": str(Path("/proc/self/exe").resolve()),
                              "sha256": sha(Path("/proc/self/exe"))},
              "adapters_sha256": {p.name: sha(p) for p in sorted(args.adapters.parent.glob("compat*adapters.py"))},
              "selected_adapter_sha256": sha(args.adapters),
              "request": request["case"]}
    code = 0
    calls = set()
    setup_calls = set()
    executed_lines, setup_lines = {}, {}
    consumer_started = False
    source_codes, checked_codes, code_hashes = {}, set(), {}
    code_errors = []
    production_prefix = str(root) + "/"

    def begin_consumer():
        nonlocal consumer_started
        if request["op"] != "read" or consumer_started:
            raise RuntimeError("consumer boundary must occur once, after fixture setup")
        setup_calls.update(calls)
        setup_lines.update({path: sorted(lines) for path, lines in executed_lines.items()})
        executed_lines.clear()
        calls.clear()
        consumer_started = True

    request["_consumer_boundary"] = begin_consumer

    def entered(frame, event, arg):
        if event == "call" and frame.f_code.co_filename.startswith(production_prefix):
            actual = frame.f_code
            filename = actual.co_filename
            identity = (filename, actual.co_qualname, actual.co_firstlineno, actual)
            if identity not in checked_codes:
                if filename not in source_codes:
                    compiled = compile(Path(filename).read_bytes(), filename, "exec", dont_inherit=True,
                                       optimize=sys.flags.optimize)
                    pending, candidates = [compiled], {}
                    while pending:
                        code_object = pending.pop()
                        key = (code_object.co_qualname, code_object.co_firstlineno)
                        candidates.setdefault(key, []).append(code_object)
                        pending.extend(value for value in code_object.co_consts if isinstance(value, types.CodeType))
                    source_codes[filename] = candidates
                key = (actual.co_qualname, actual.co_firstlineno)
                if not any(actual == expected and actual.co_filename == expected.co_filename
                           for expected in source_codes[filename].get(key, ())):
                    message = f"executing code differs from compiled immutable source: {filename}:{key}"
                    code_errors.append(message)
                    raise RuntimeError(message)
                checked_codes.add(identity)
                code_hashes.setdefault(str(Path(filename).relative_to(root)), {})[
                    actual.co_qualname + "@" + str(actual.co_firstlineno)] = hashlib.sha256(marshal.dumps(actual)).hexdigest()
            calls.add((str(Path(filename).relative_to(root)), actual.co_name, actual.co_firstlineno, actual.co_qualname))

    def traced(frame, event, arg):
        if frame.f_code.co_filename.startswith(production_prefix):
            if event == "line":
                path = frame.f_code.co_filename[len(production_prefix):]
                executed_lines.setdefault(path, set()).add(frame.f_lineno)
            return traced
        return None

    try:
        adapter = runpy.run_path(str(args.adapters))["operate"]
        sys.setprofile(entered)
        sys.settrace(traced)
        output["result"] = adapter(request, args.store, lambda name: load_module(root, name))
    except Exception as error:
        code = 1
        output["error"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        sys.settrace(None)
        sys.setprofile(None)
        output["executed_lines"] = {path: sorted(lines) for path, lines in executed_lines.items()}
        output["setup_lines"] = setup_lines
        output["calls"] = sorted(calls)
        output["setup_calls"] = sorted(setup_calls)
        output["consumer_boundary"] = consumer_started
        output["executing_code_sha256"] = code_hashes
        output["compiled_source_code_verified"] = bool(checked_codes) and not code_errors
    try:
        output["imports"], output["stdlib_imports"] = attest(root, stdlib, manifest)
    except Exception as error:
        code = 2
        output["attestation_error"] = str(error)
    if code_errors:
        code = 2
        output["attestation_error"] = code_errors[0]
    output["sys_path"] = list(sys.path)
    print(json.dumps(output, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
