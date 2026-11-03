"""Complete private CLI records. Contract: docs/machine_record_channel.md."""
from __future__ import annotations

import fcntl
import json
import os
import stat
import subprocess
import sys
import tempfile

CAPABILITY = {"schema": "prismabuild.machine_record_capability.v1", "machine_record_fd": 1}
FLAG = '--machine-record-fd'


class MachineRecordError(ValueError):
    pass


class UnsupportedRuntime(MachineRecordError):
    pass


def _decode_machine_document(raw):
    def private_object(pairs):
        value = dict(pairs)
        if len(value) != len(pairs):
            raise MachineRecordError('duplicate private record key')
        return value

    def private_constant(_):
        raise MachineRecordError('non-finite private record number')

    try:
        decoder = json.JSONDecoder(object_pairs_hook=private_object, parse_constant=private_constant)
        return decoder.decode(raw.decode('utf-8').lstrip(' \t\r\n'))
    except (ValueError, UnicodeError, RecursionError):
        raise MachineRecordError('private machine record is malformed') from None


def capability(argv) -> bool:
    if list(sys.argv[1:] if argv is None else argv) != ['--machine-record-capability']:
        return False
    print(json.dumps(CAPABILITY, separators=(',', ':')))
    return True


def require_capability(command) -> None:
    try:
        result = subprocess.run([*command, '--machine-record-capability'],
            capture_output=True, text=True, check=False, timeout=30)
        value = _decode_machine_document(result.stdout.encode("utf-8"))
        supported = (result.returncode == 0 and isinstance(value, dict)
            and value.get('schema') == CAPABILITY['schema']
            and type(value.get('machine_record_fd')) is int and value['machine_record_fd'] >= 1)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        supported = False
    if not supported:
        raise UnsupportedRuntime('published runtime does not support private machine records')


class Writer:
    def __init__(self, fd: int):
        try:
            info = os.fstat(fd)
            if (type(fd) is not int or fd <= 2 or not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_size != 0
                    or fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY):
                raise MachineRecordError('machine record descriptor must be an empty private writable regular file')
            for standard in (1, 2):
                try:
                    target = os.fstat(standard)
                except OSError:
                    continue
                if (target.st_dev, target.st_ino) == (info.st_dev, info.st_ino):
                    raise MachineRecordError('machine record descriptor must not alias stdout or stderr')
            os.lseek(fd, 0, os.SEEK_SET)
            os.set_inheritable(fd, False)
        except OSError:
            raise MachineRecordError('cannot open machine record descriptor') from None
        self.fd = fd
        self.written = False

    def write(self, value) -> bool:
        if self.written:
            raise MachineRecordError('machine record already written')
        self.written = True
        try:
            body = json.dumps(value, ensure_ascii=True, allow_nan=False).encode('utf-8')
            view = memoryview(body)
            while view:
                size = os.write(self.fd, view)
                if size <= 0:
                    raise OSError('short machine record write')
                view = view[size:]
            if os.write(self.fd, b'\n') != 1:
                raise OSError('missing machine record commit')
            return True
        except (OSError, ValueError):
            try:
                os.ftruncate(self.fd, 0)
            except OSError:
                pass
            print('private machine record write failed; record unavailable', file=sys.stderr)
            return False


class Capture:
    """Own one unnamed 0600 record file across a CLI subprocess invocation."""
    def __init__(self):
        self.file = tempfile.TemporaryFile(mode='w+b')
        os.fchmod(self.file.fileno(), 0o600)

    @property
    def pass_fds(self):
        return (self.file.fileno(),)

    def command(self, command):
        position = command.index('--') if '--' in command else len(command)
        if any(word == FLAG or word.startswith(FLAG + '=') for word in command[:position]):
            raise MachineRecordError('caller already supplied a machine record descriptor')
        return [*command[:position], FLAG, str(self.file.fileno()), *command[position:]]

    def read(self):
        self.file.seek(0)
        raw = self.file.read()
        if not raw:
            return None
        if not raw.endswith(b'\n'):
            raise MachineRecordError('private machine record is incomplete')
        return _decode_machine_document(raw)

    def close(self):
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def retain(path, value):
    """Retain private evidence without creating a publicly readable file."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        if not Writer(stream.fileno()).write(value):
            raise MachineRecordError('cannot retain private machine record')
