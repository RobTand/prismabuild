"""A forked role owns its log through multiprocessing's final stream flush."""
import faulthandler
import io
import os
import signal
import sys
import threading

from test_role_session_boundaries import CONTEXT, _finish, _run_role


def test_role_exit_keeps_its_own_log_after_fork_from_a_writer_thread(tmp_path, monkeypatch):
    entered = threading.Event()
    released = threading.Event()
    ready, stopped = CONTEXT.Event(), CONTEXT.Event()
    receiver, sender = CONTEXT.Pipe(duplex=False)
    log = tmp_path / "role.log"
    stack = tmp_path / "exit.stack"

    class BlockingRaw(io.RawIOBase):
        def writable(self):
            return True

        def write(self, data):
            entered.set()
            if not released.wait(10):
                raise TimeoutError("parent did not release its writer")
            return len(data)

    buffer = io.BufferedWriter(BlockingRaw(), buffer_size=1)
    stream = io.TextIOWrapper(buffer, encoding="utf-8", write_through=True)
    real_fork = os.fork
    writer = threading.Thread(target=lambda: buffer.write(b"writer-held"), daemon=True)

    def fork_after_parent_flush():
        writer.start()
        assert entered.wait(5), "writer did not hold the actual buffered stream"
        pid = real_fork()
        if pid:
            released.set()
            writer.join(5)
            assert not writer.is_alive()
        return pid

    def serve():
        descriptor = os.open(stack, os.O_WRONLY | os.O_CREAT, 0o600)
        faulthandler.register(signal.SIGUSR2, file=descriptor, all_threads=True)
        ready.set()
        assert receiver.recv_bytes() == b"finish"
        print("role-stop-complete", flush=True)
        stopped.set()
        return 0

    process = CONTEXT.Process(target=_run_role, args=(serve, log))
    try:
        with monkeypatch.context() as patch:
            patch.setattr(sys, "stdout", stream)
            patch.setattr(os, "fork", fork_after_parent_flush)
            process.start()
        receiver.close()
        assert ready.wait(5), "role did not reach its private pipe"
        sender.send_bytes(b"finish")
        process.join(3)
        assert stopped.is_set(), "role itself did not finish"
        assert log.read_text() == "role-stop-complete\n"
        assert process.exitcode == 0, "role returned but its process did not exit"
    finally:
        released.set()
        if process.pid is not None:
            if process.exitcode is None:
                os.kill(process.pid, signal.SIGUSR2)
            _finish(process)
            if stack.exists():
                print("role-exit-stack:", stack.read_text(), file=sys.stderr)
        sender.close()
        receiver.close()
        if writer.ident is not None:
            writer.join(5)
        stream.close()
