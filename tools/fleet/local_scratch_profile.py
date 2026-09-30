"""PB-admitted bounded CPU filesystem profile; a dedicated file is the result.

Submit with published pbrun, CPU/memory/native-thread reservations, explicit
scratch pair and timeout. Never benchmark in a claim/worker poll. This method
includes write fdatasync and warm buffered reads; it does not qualify physical
read bandwidth, pressure, random spill or the full #1182 workload.

-I -S and run_path keep the producer dependency closure stdlib plus the exact
recorder, shared scratch source and sibling Core in the sealed checkout (no
package __init__ imports). Core recipes and self-source capture load once at
startup, outside the measured I/O.
"""
import os
from pathlib import Path
import runpy
import signal
import sys


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    source = Path(__file__).resolve().parents[2] / "src/prismabuild/local_scratch.py"
    scratch = runpy.run_path(str(source))
    key = os.environ.get("PRISMABUILD_ACTION_KEY", "")
    if len(key) != 64 or any(c not in "0123456789abcdef" for c in key):
        raise SystemExit("scratch profiles execute only as PB-admitted action children")
    if not sys.flags.isolated or not sys.flags.no_site:
        raise SystemExit("scratch recorder requires isolated -I -S interpreter")
    envelope = scratch["profile_command"]([sys.executable, "-I", "-S", scratch["RECORDER"], *argv])
    if envelope is None:
        raise SystemExit("scratch recorder requires an explicit envelope")
    pairs = scratch["scratch_pairs"](os.environ)
    if (len(pairs) != 1
            or (pairs[0]["root_env"], pairs[0]["max_env"]) !=
               (scratch["PROFILE_ROOT_ENV"], scratch["PROFILE_MAX_ENV"])
            or pairs[0]["root"] != envelope["root"]
            or pairs[0]["max_bytes"] < envelope["bytes"] or scratch["IO_ENV"] in os.environ):
        raise SystemExit("scratch recorder requires exactly one sufficient pair, capacity-only")
    def terminate(signum, _frame):
        # Unwind bounded scratch cleanup on normal containment withdrawal.
        # SIGKILL/OOM cannot run Python cleanup and never produces a receipt.
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    body = scratch["record_profile"](envelope)
    raw = scratch["_canonical_file_bytes"](body)
    fd = os.open(scratch["PROFILE_RESULT"], os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with os.fdopen(fd, "wb") as result:
        result.write(raw)
        result.flush()
        os.fsync(result.fileno())
    return 0 if body["completed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
