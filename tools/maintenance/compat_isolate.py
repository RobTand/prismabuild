"""Execute a production compatibility adapter with an attested import boundary."""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import platform
from pathlib import Path
import runpy
import sys
import sysconfig
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
    observed = {}
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
    if not observed:
        raise RuntimeError("adapter reached no production imports")
    return observed


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
            if writing and not path.is_relative_to(store):
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
    private_writes_only(args.store)
    output = {"isolated": bool(sys.flags.isolated), "no_site": bool(sys.flags.no_site),
              "sys_path": list(sys.path), "driver_sha256": sha(Path(__file__)),
              "python": sys.version, "machine": platform.machine(),
              "adapters_sha256": {p.name: sha(p) for p in sorted(args.adapters.parent.glob("compat*adapters.py"))},
              "request": request["case"]}
    code = 0
    calls = set()

    def entered(frame, event, arg):
        if event == "call" and frame.f_code.co_filename.startswith(str(root) + "/"):
            calls.add((str(Path(frame.f_code.co_filename).relative_to(root)),
                       frame.f_code.co_name, frame.f_code.co_firstlineno, frame.f_code.co_qualname))

    try:
        adapter = runpy.run_path(str(args.adapters))["operate"]
        sys.setprofile(entered)
        output["result"] = adapter(request, args.store, importlib.import_module)
    except Exception as error:
        code = 1
        output["error"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        sys.setprofile(None)
        output["calls"] = sorted(calls)
    try:
        output["imports"] = attest(root, stdlib, manifest)
    except Exception as error:
        code = 2
        output["attestation_error"] = str(error)
    print(json.dumps(output, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
