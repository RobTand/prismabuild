"""Private host observations, using real cache writers and cache consumers."""
from __future__ import annotations

import base64
import json
import os


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    write = request["op"] == "write"
    if family == "host-origin":
        cpu = load("prismabuild.adaptive_cpu")
        directory = store / "origins"
        directory.mkdir()
        base = store / "queue/reservations/compat-host"
        base.mkdir(parents=True)
        digest = cpu.box_identity(base)
        path = directory / (digest + ".origin.json")
        if write:
            assert cpu._record_box_origin_if_new(directory, digest, base) == "recorded"
            if variant == "repeated":
                before = path.read_bytes()
                assert cpu._record_box_origin_if_new(directory, digest, base) == "present"
                assert path.read_bytes() == before
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            result = cpu.read_box_origin(directory, digest)
            assert result and result["pid"] == 2, "origin consumer refused the actual namespace PID"
            return result
    else:
        ci = load("prismabuild.container_images")
        identifier = "sha256:" + "a" * 64
        seen = []

        def probe(argv, **kwargs):
            seen.append(argv)
            if variant == "unknown":
                return None
            if "info" in argv:
                return b'"/compat/docker"'
            if "inspect" in argv:
                return (json.dumps({"Id": identifier, "RepoTags": ["compat/image:latest"], "RepoDigests": [],
                                    "Size": 1, "Architecture": "amd64", "Os": "linux",
                                    "Config": {"Entrypoint": ["/bin/sh"]},
                                    "RootFS": {"Type": "layers", "Layers": ["sha256:" + "b" * 64]}}) + "\n").encode()
            return b"" if variant == "empty" else (identifier + "\t<none>\t<none>\n").encode()

        cache = ci.InventoryCache(root=store / "cache", probe=probe, clock=lambda: 100.0)
        path = cache.root / cache.record_name
        if write:
            assert cache.snapshot() is not None and seen, "actual cache writer did not probe its supplied inputs"
        else:
            cache.root.mkdir(mode=0o700)
            path.write_bytes(base64.b64decode(request["raw"]))
            descriptor = ci._open_private_directory(cache.root)
            assert descriptor is not None
            try:
                result = cache._snapshot(descriptor, now=100.0)
            finally:
                os.close(descriptor)
            assert result is not None, "real cache reader refused the inventory"
            assert (result["entries"] is None) == (variant == "unknown")
            if variant == "image":
                assert identifier in result["entries"] and result["image_contents"]["compat/image:latest"] in result["entries"]
            assert not seen, "reading the captured cache unexpectedly refreshed its bytes"
            return result
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
