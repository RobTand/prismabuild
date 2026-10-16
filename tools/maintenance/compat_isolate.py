"""Execute a production compatibility adapter with an attested import boundary."""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
from pathlib import Path
import runpy
import sys
import sysconfig


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def isolate(root):
    if not sys.flags.isolated or not sys.flags.no_site:
        raise RuntimeError("compatibility consumer requires python -I -S")
    stdlib = {Path(sysconfig.get_path(name)).resolve() for name in ("stdlib", "platstdlib")}
    stdlib.update(path / "lib-dynload" for path in tuple(stdlib))
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
    output = {"isolated": bool(sys.flags.isolated), "no_site": bool(sys.flags.no_site),
              "sys_path": list(sys.path), "driver_sha256": sha(Path(__file__)),
              "adapters_sha256": {p.name: sha(p) for p in sorted(args.adapters.parent.glob("compat*adapters.py"))},
              "request": request["case"]}
    code = 0
    calls = set()

    def entered(frame, event, arg):
        if event == "call" and frame.f_code.co_filename.startswith(str(root) + "/"):
            calls.add((str(Path(frame.f_code.co_filename).relative_to(root)),
                       frame.f_code.co_name, frame.f_code.co_firstlineno))

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
