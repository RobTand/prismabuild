"""Private Linux exec-event mediation for the supported Docker executable.

This is a cooperative runtime boundary, not a hostile-code sandbox: copied
clients, raw daemon APIs, ptrace attacks and namespace manipulation are outside
its contract. The stopped executable has not executed any user instructions.
Route it through the existing ownership/qualification shim, including native
launchers whose child argv is read from a sealed input at runtime.
"""
from __future__ import annotations

import ctypes
import os
from pathlib import Path
import signal
import struct
import sys


def launch_argv(argv: list[str], environment: dict[str, str]) -> list[str]:
    """Wrap the execution, never the sealed action or its attestation."""
    client = environment.get("PRISMABUILD_DOCKER_REAL", "/usr/bin/docker")
    if not os.path.isabs(client):
        raise OSError("container class mediation requires an absolute Docker client path")
    # Do not impose Linux tracing on hosts with no supported client installed.
    if not Path(client).exists():
        return argv
    return [sys.executable, str(Path(__file__).resolve()), client, *argv]


_LIBC = ctypes.CDLL(None, use_errno=True)
_LIBC.ptrace.restype = ctypes.c_long


def _ptrace(request: int, pid: int, addr=0, data=0) -> int:
    result = _LIBC.ptrace(ctypes.c_uint(request), ctypes.c_uint(pid),
                          ctypes.c_void_p(addr), ctypes.c_void_p(data))
    if result == -1:
        value = ctypes.get_errno()
        raise OSError(value, os.strerror(value))
    return result


class _IOVec(ctypes.Structure):
    _fields_ = [("base", ctypes.c_void_p), ("length", ctypes.c_size_t)]


def _redirect(pid: int, argv: list[bytes], environment: list[bytes]) -> None:
    """Inject execve at the stopped image entry, via native GETREGSET.

    No syscall-number guessing based on the runner architecture: the new ELF
    image identifies the ABI. Only Linux x86-64 and AArch64 are supported; other
    ABIs fail closed when the configured client is encountered.
    """
    with open(f"/proc/{pid}/exe", "rb") as stream:
        elf = stream.read(20)
    machine = int.from_bytes(elf[18:20], "little")
    if elf[:5] != b"\x7fELF\x02" or elf[5] != 1 or machine not in (62, 183):
        raise OSError("container class exec mediation unavailable for client ABI")
    regs = (ctypes.c_uint64 * 34)()
    vector = _IOVec(ctypes.cast(regs, ctypes.c_void_p), ctypes.sizeof(regs))
    _ptrace(0x4204, pid, 1, ctypes.addressof(vector))  # GETREGSET, NT_PRSTATUS
    sp_index, pc_index = (19, 16) if machine == 62 else (31, 32)
    storage = sum(len(value) + 1 for value in argv + environment)
    storage += 8 * (len(argv) + len(environment) + 2) + 16
    if storage > 120000:
        raise OSError("container class exec mediation argument limit exceeded")
    stack = (int(regs[sp_index]) - storage - 256) & ~15
    payload = bytearray()

    def strings(values):
        pointers = []
        for value in values:
            pointers.append(stack + len(payload))
            payload.extend(value + b"\0")
        return pointers

    arguments = strings(argv)
    variables = strings(environment)
    payload.extend(b"\0" * (-len(payload) % 8))
    arg_pointer = stack + len(payload)
    payload.extend(struct.pack("<" + "Q" * (len(arguments) + 1), *arguments, 0))
    env_pointer = stack + len(payload)
    payload.extend(struct.pack("<" + "Q" * (len(variables) + 1), *variables, 0))
    if len(payload) > 120000:
        raise OSError("container class exec mediation argument limit exceeded")
    with open(f"/proc/{pid}/mem", "r+b", buffering=0) as memory:
        memory.seek(stack)
        memory.write(payload)
        memory.seek(int(regs[pc_index]))
        memory.write(b"\x0f\x05" if machine == 62 else b"\x01\x00\x00\xd4")
    if machine == 62:
        regs[10], regs[14], regs[13], regs[12] = 59, arguments[0], arg_pointer, env_pointer
    else:
        regs[8], regs[0], regs[1], regs[2] = 221, arguments[0], arg_pointer, env_pointer
    _ptrace(0x4205, pid, 1, ctypes.addressof(vector))  # SETREGSET


def _client_arguments(pid: int, client: str) -> list[bytes] | None:
    cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    # Native ELF identity is kernel-owned. For a shebang executable the kernel
    # exposes the interpreter as exe and inserts the exact script pathname in
    # cmdline; resolve only that interpreter slot, not arbitrary argv strings.
    if os.path.samefile(f"/proc/{pid}/exe", client):
        return cmdline[1:]
    with open(client, "rb") as stream:
        shebang = stream.readline(4096)
    if shebang.startswith(b"#!"):
        # Linux separates shebang words with space/tab, not general Unicode or
        # byte whitespace, and passes the optional argument as ONE argv entry.
        line = shebang[2:].split(b"\n", 1)[0].strip(b" \t")
        interpreter = line.replace(b"\t", b" ").split(b" ", 1)[0]
        argument = line[len(interpreter):].lstrip(b" \t")
        if not interpreter or not os.path.isabs(interpreter):
            return None
        # A script pathname in argv is not executable identity: an ordinary
        # native launcher may legitimately have the shim at argv[1]. Require
        # the kernel-owned image to be the actual shebang interpreter as well.
        # For #!/usr/bin/env (including -S), catch the initial env exec stop,
        # BEFORE env performs PATH lookup or processes its options. Do not
        # guess the eventual interpreter from argv/PATH: a later env-selected
        # image is not evidence of this shebang and must not mark it routed.
        try:
            if not os.path.samefile(f"/proc/{pid}/exe", os.fsdecode(interpreter)):
                return None
        except OSError:
            return None
        slot = 2 if argument else 1
        if len(cmdline) > slot:
            path = os.fsdecode(cmdline[slot])
            if not os.path.isabs(path):
                path = f"/proc/{pid}/cwd/{path}"
            try:
                if os.path.samefile(path, client):
                    return cmdline[slot + 1:]
            except OSError:
                pass
    return None


def _run_container_command(client: str, argv: list[str]) -> int:
    shim = Path(__file__).resolve().parents[2] / "tools" / "fleet" / "docker"
    # Capture executor authority before any ordinary child can replace its
    # locator. Preserve the child's other environment, but never let it supply
    # this private capability (including when the original value was absent).
    context_key = b"_PRISMABUILD_CONTAINER_CLASS_CONTEXT="
    trusted_context = context_key + os.fsencode(
        os.environ.get("_PRISMABUILD_CONTAINER_CLASS_CONTEXT", ""))
    leader = os.fork()
    if leader == 0:
        try:
            _ptrace(0, 0)  # TRACEME
            os.kill(os.getpid(), signal.SIGSTOP)
            os.execvpe(argv[0], argv, os.environ)
        except BaseException as error:
            print(f"container class exec mediation unavailable: {error}", file=sys.stderr)
            os._exit(125)
    live = {leader}
    # An injected shim exec is followed once without rewrapping it. While
    # running the shim, fork children may forward its daemon calls, but their
    # first exec consumes that permission. A client/plugin/helper's later exec
    # is an ordinary new invocation, not part of the qualified operation.
    redirecting: set[int] = set()
    shim_tasks: set[int] = set()
    result = 125
    # Keep the supervisor alive during the existing group TERM/grace/KILL
    # protocol, so it reaps tracees. EXITKILL covers abrupt supervisor death.
    def terminate(signum, frame):
        for pid in tuple(live):
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, terminate)
    try:
        while live:
            pid, status = os.waitpid(-1, 0x40000000)  # __WALL, all traced threads
            if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                live.discard(pid)
                redirecting.discard(pid)
                shim_tasks.discard(pid)
                if pid == leader:
                    result = os.waitstatus_to_exitcode(status)
                continue
            live.add(pid)
            event = status >> 16
            sig = os.WSTOPSIG(status)
            if sig == signal.SIGSTOP:
                # Fork children inherit options; repeating is harmless.
                _ptrace(0x4200, pid, 0, 0x100000 | 2 | 4 | 8 | 16)
                sig = 0  # EXITKILL + TRACEFORK/VFORK/CLONE/EXEC
            elif event in (1, 2, 3):
                child = ctypes.c_ulong()
                _ptrace(0x4201, pid, 0, ctypes.addressof(child))
                live.add(child.value)
                if pid in shim_tasks:
                    shim_tasks.add(child.value)
                sig = 0
            elif event == 4:
                former = ctypes.c_ulong()
                _ptrace(0x4201, pid, 0, ctypes.addressof(former))
                if former.value != pid:
                    # A nonleader thread assumes the thread-group leader's TID
                    # at exec. Linux reports the old TID only in GETEVENTMSG;
                    # it will never deliver a separate exit for that old TID.
                    live.discard(former.value)
                    for tasks in (redirecting, shim_tasks):
                        was_member = former.value in tasks
                        tasks.discard(former.value)
                        tasks.discard(pid)
                        if was_member:
                            tasks.add(pid)
                if pid in redirecting:
                    redirecting.remove(pid)
                    shim_tasks.add(pid)
                elif pid in shim_tasks:
                    # A #!/usr/bin/env shim first execs its Python interpreter.
                    # That is still the shim, not its one forwarded daemon call.
                    if _client_arguments(pid, str(shim)) is None:
                        shim_tasks.remove(pid)
                else:
                    # Direct/PATH shim launches need the same pinned context as
                    # redirected client launches. Re-exec once with their
                    # original arguments and pin the executor's private context.
                    args = (_client_arguments(pid, str(shim))
                            if shim.is_file() else None)
                    if args is None:
                        args = _client_arguments(pid, client)
                    if args is not None:
                        if not shim.is_file():
                            raise OSError("container class mediation shim unavailable")
                        environment = [
                            value for value in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
                            if value and not value.startswith(context_key)
                        ]
                        environment.append(trusted_context)
                        # EXEC is inside the old execve syscall; reach its exit
                        # stop first or the kernel overwrites the injected return
                        # register while finishing that syscall (notably x86).
                        _ptrace(24, pid)  # SYSCALL
                        stopped, exit_status = os.waitpid(pid, 0x40000000)
                        if not os.WIFSTOPPED(exit_status):
                            raise OSError("container class exec exit stop unavailable")
                        _redirect(pid, [os.fsencode(shim), *args], environment)
                        redirecting.add(pid)
                sig = 0
            _ptrace(7, pid, 0, sig)  # CONT; preserve ordinary signal delivery
    finally:
        for pid in live:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        while live:
            try:
                pid, _ = os.waitpid(-1, 0x40000000)
                live.discard(pid)
            except ChildProcessError:
                break
    if result < 0:
        # Popen must observe the original signal, not an invented exit(128+n).
        if -result not in (signal.SIGKILL, signal.SIGSTOP):
            signal.signal(-result, signal.SIG_DFL)
        os.kill(os.getpid(), -result)
    return result


if __name__ == "__main__":
    try:
        raise SystemExit(_run_container_command(sys.argv[1], sys.argv[2:]))
    except OSError as error:
        print(f"container class exec mediation unavailable: {error}", file=sys.stderr)
        raise SystemExit(125)
