"""Probe private mount namespaces for stable-path production receipt fixtures."""
import json
import os
import shutil
import subprocess

unshare = shutil.which("unshare")
result = {"unshare": unshare, "bwrap": shutil.which("bwrap"),
          "affinity": sorted(os.sched_getaffinity(0)), "private_mount_namespace": False}
if unshare:
    child = subprocess.run([unshare, "--user", "--map-root-user", "--mount", "--fork", "/bin/true"],
                           capture_output=True, text=True, timeout=10)
    result.update(private_mount_namespace=child.returncode == 0, namespace_exit=child.returncode,
                  namespace_error=child.stderr.strip())
if result["bwrap"]:
    child = subprocess.run([result["bwrap"], "--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev",
                            "--", "/bin/true"], capture_output=True, text=True, timeout=10)
    result.update(bwrap_exit=child.returncode, bwrap_namespace=child.returncode == 0,
                  bwrap_error=child.stderr.strip())
print(json.dumps(result, sort_keys=True))
