"""The bounded private client protocol, separate from action output."""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
import fcntl
import math
import os
from pathlib import Path
import re
import select
import stat
import subprocess
import threading
from typing import TypeVar, assert_never

from . import action_client as client, action_wait, core as pb
from .digest_primitives import compact_ascii_json_bytes, decode_strict_json

CLIENT_EVENT_SCHEMA = 'prismabuild.client_event.v1'
MAX_EVENT_BYTES = 1024
MAX_CLIENT_EVENTS = 2
T = TypeVar('T')


class ClientProtocolError(ValueError):
    pass


def _event_optional(value: object, parse: Callable[[object], T]) -> T | None:
    return None if value is None else parse(value)


def _event_key(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch('[a-f0-9]{64}', value) is None:
        raise ClientProtocolError('client event requires an exact 64-hex identity')
    return value


def _event_generation(value: object) -> float:
    if type(value) not in (int, float):
        raise ClientProtocolError('client generation must be finite')
    try:
        result = float(value)
    except OverflowError:
        raise ClientProtocolError('client generation must be finite') from None
    if not math.isfinite(result):
        raise ClientProtocolError('client generation must be finite')
    return result


def _event_integer(value: object) -> int:
    if type(value) is not int:
        raise ClientProtocolError('client status must be an integer')
    return value


def _event_text(value: object) -> str:
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ClientProtocolError('client event requires nonempty text')
    return value


def _event_path(value: object) -> Path:
    result = Path(_event_text(value))
    if not result.is_absolute():
        raise ClientProtocolError('client event requires an absolute path')
    return result


def _event_fields(record: dict, expected: set[str]) -> None:
    if set(record) != expected:
        raise ClientProtocolError('client event has missing or unknown fields')


def _identity_event(identity: client.ClientIdentity) -> dict:
    record = dict(schema=CLIENT_EVENT_SCHEMA, event='identity',
                  action_key=identity.action_key)
    if isinstance(identity, client.PoolClientIdentity):
        return dict(record, kind=('queued_pool' if identity.arrival is client.ClientArrival.SUBMITTED
                                  else 'attached_pool'), published_unix=identity.published_unix,
                    submission=str(identity.submission) if identity.submission is not None else None)
    if isinstance(identity, client.SlurmClientIdentity):
        return dict(record, kind='slurm', published_unix=identity.published_unix,
                    submission=str(identity.submission) if identity.submission is not None else None,
                    job_id=identity.job_id, arrival=identity.arrival.value)
    if isinstance(identity, client.CachedClientIdentity):
        return dict(record, kind='cas_hit', transport=identity.transport.value,
                    result_sha256=identity.result_sha256)
    assert_never(identity)


def ending_identity(verdict: client.ClientVerdict) -> tuple[str | None, float | None]:
    if isinstance(verdict, client.ClientSubmitted):
        identity = verdict.identity
        return identity.action_key, (None if isinstance(identity, client.CachedClientIdentity)
                                     else identity.published_unix)
    if isinstance(verdict, (client.ClientOfferTimeout, client.ClientDeferredTimeout,
                            client.ClientDeferredSubmitted, client.ClientCommand)):
        return None, None
    return verdict.action_key, verdict.published_unix


def _ending_event(ending: client.ClientEnding, identity: client.ClientIdentity | None) -> dict:
    verdict = ending.verdict
    key, generation = ending_identity(verdict)
    record = dict(schema=CLIENT_EVENT_SCHEMA, event='ending', action_key=key,
                  published_unix=generation, client_returncode=ending.exit_code & 255,
                  reported_returncode=ending.exit_code,
                  abandoned_count=ending.readers.abandoned_count,
                  abandoned_readers=[reader.to_record() for reader in ending.readers.identities],
                  readers_complete=ending.readers.complete,
                  recovery_eligible=client.recovery_permitted(identity, ending))
    if isinstance(verdict, client.ClientExecution):
        return dict(record, kind='execution', status=verdict.status,
                    transport_returncode=verdict.transport_returncode,
                    action_returncode=verdict.action_returncode, action_signal=verdict.action_signal,
                    execution_timeout=verdict.execution_timeout,
                    receipt_published=verdict.receipt_published,
                    transport_state=verdict.transport_state,
                    transport_signal=verdict.transport_signal,
                    cached_result_sha256=verdict.cached_result_sha256,
                    cached_result_diagnostic=(verdict.cached_result_diagnostic.value
                                              if verdict.cached_result_diagnostic is not None else None))
    if isinstance(verdict, client.ClientUnreadable):
        return dict(record, kind='unreadable')
    if isinstance(verdict, client.ClientUnavailable):
        return dict(record, kind='unavailable', wait_end=verdict.wait_end.value)
    if isinstance(verdict, client.ClientPatienceExpired):
        return dict(record, kind='patience_expired')
    if isinstance(verdict, client.ClientSuperseded):
        return dict(record, kind='supersession', successor_published_unix=verdict.successor_published_unix)
    if isinstance(verdict, client.ClientOfferTimeout):
        return dict(record, kind='offer_timeout')
    if isinstance(verdict, client.ClientRefused):
        return dict(record, kind='refused')
    if isinstance(verdict, client.ClientDeferredTimeout):
        return dict(record, kind='deferred_timeout', pending_id=verdict.pending_id)
    if isinstance(verdict, client.ClientSubmitted):
        return dict(record, kind='submitted',
                    submission=str(verdict.submission) if verdict.submission is not None else None)
    if isinstance(verdict, client.ClientDeferredSubmitted):
        return dict(record, kind='deferred_submitted', pending_id=verdict.pending_id,
                    submission=str(verdict.submission), release=str(verdict.release))
    if isinstance(verdict, client.ClientUnsettled):
        return dict(record, kind='unsettled', reason=verdict.reason.value)
    if isinstance(verdict, client.ClientCommand):
        return dict(record, kind='command')
    assert_never(verdict)


def encode_client_event(event: client.ClientIdentity | client.ClientEnding, *,
                        identity: client.ClientIdentity | None = None) -> bytes:
    record = (_ending_event(event, identity) if isinstance(event, client.ClientEnding)
              else _identity_event(event))
    while True:
        raw = compact_ascii_json_bytes(record) + b'\n'
        if len(raw) <= MAX_EVENT_BYTES:
            return raw
        if record['event'] != 'ending' or not record['abandoned_readers']:
            raise ClientProtocolError('client event exceeds its byte limit')
        record['abandoned_readers'].pop()
        record['readers_complete'] = False


def _parse_identity(record: dict, *, queue_root: Path, cas_root: Path) -> client.ClientIdentity:
    base = {'schema', 'event', 'kind', 'action_key'}
    kind = _event_text(record.get('kind'))
    key = _event_key(record.get('action_key'))
    if kind in {'queued_pool', 'attached_pool'}:
        _event_fields(record, base | {'published_unix', 'submission'})
        return client.PoolClientIdentity(key, _event_optional(record['published_unix'], _event_generation), queue_root, cas_root,
            _event_optional(record['submission'], _event_path),
            client.ClientArrival.SUBMITTED if kind == 'queued_pool' else client.ClientArrival.ATTACHED)
    if kind == 'slurm':
        _event_fields(record, base | {'published_unix', 'submission', 'job_id', 'arrival'})
        return client.SlurmClientIdentity(key, _event_optional(record['published_unix'], _event_generation), queue_root, cas_root,
            _event_optional(record['submission'], _event_path), _event_text(record['job_id']),
            client.ClientArrival(_event_text(record['arrival'])))
    if kind == 'cas_hit':
        _event_fields(record, base | {'transport', 'result_sha256'})
        return client.CachedClientIdentity(key, queue_root, cas_root,
            client.ClientTransport(_event_text(record['transport'])), _event_key(record['result_sha256']))
    raise ClientProtocolError('unknown client identity kind')


def _parse_history(record: dict) -> action_wait.ReaderHistory:
    count = _event_integer(record['abandoned_count'])
    records, complete = record['abandoned_readers'], record['readers_complete']
    if (count < 0 or type(complete) is not bool or not isinstance(records, list)
            or len(records) > 8 or any(not isinstance(reader, dict) for reader in records)):
        raise ClientProtocolError('invalid client reader history')
    identities = tuple(action_wait.RetainedReader.from_record(reader) for reader in records)
    if count < len(identities) or (complete and count != len(identities)):
        raise ClientProtocolError('client reader count contradicts its identities')
    return action_wait.ReaderHistory(count, identities, complete)


def _parse_ending(record: dict, identity: client.ClientIdentity | None) -> client.ClientEnding:
    base = {'schema', 'event', 'kind', 'action_key', 'published_unix', 'client_returncode',
            'reported_returncode',
            'abandoned_count', 'abandoned_readers', 'readers_complete', 'recovery_eligible'}
    extras = {'execution': {'status', 'transport_returncode', 'action_returncode', 'action_signal',
                            'execution_timeout', 'cached_result_sha256', 'cached_result_diagnostic',
                            'receipt_published', 'transport_state', 'transport_signal'},
              'unavailable': {'wait_end'}, 'unreadable': set(), 'patience_expired': set(),
              'supersession': {'successor_published_unix'}, 'offer_timeout': set(), 'refused': set(),
              'deferred_timeout': {'pending_id'}, 'submitted': {'submission'}, 'command': set(),
              'deferred_submitted': {'pending_id', 'submission', 'release'}, 'unsettled': {'reason'}}
    kind = _event_text(record.get('kind'))
    if kind not in extras:
        raise ClientProtocolError('unknown client ending kind')
    _event_fields(record, base | extras[kind])
    key = _event_optional(record['action_key'], _event_key)
    generation = _event_optional(record['published_unix'], _event_generation)
    returncode = _event_integer(record['client_returncode'])
    reported_returncode = _event_integer(record['reported_returncode'])
    if not 0 <= returncode <= 255 or type(record['recovery_eligible']) is not bool:
        raise ClientProtocolError('invalid client exit status')
    if reported_returncode & 255 != returncode:
        raise ClientProtocolError('client exit bytes differ from the reported status')
    if kind == 'execution':
        if key is None or type(record['execution_timeout']) is not bool:
            raise ClientProtocolError('invalid client execution ending')
        digest = _event_optional(record['cached_result_sha256'], _event_key)
        diagnostic = _event_optional(record['cached_result_diagnostic'], _event_text)
        if digest is not None and diagnostic is not None:
            raise ClientProtocolError('cached replay cannot be present and refused')
        replay = (client.CachedResult(digest) if digest is not None
                  else client.ReplayUnavailable(client.ReplayDiagnostic(diagnostic))
                  if diagnostic is not None else None)
        status = _event_text(record['status'])
        if ((reported_returncode in client.RESERVED_EXITS
             and not (status == 'withdrawn' and reported_returncode == client.WITHDRAWN_EXIT))
                or (status == 'withdrawn' and reported_returncode != client.WITHDRAWN_EXIT)
                or (status == 'cache_hit' and reported_returncode != 0)
                or (record['receipt_published'] is not None
                    and type(record['receipt_published']) is not bool)):
            raise ClientProtocolError('invalid reported execution status')
        verdict = client.ClientExecution(key, generation, status,
            _event_optional(record['transport_returncode'], _event_integer),
            _event_optional(record['action_returncode'], _event_integer),
            _event_optional(record['action_signal'], _event_integer), record['execution_timeout'],
            reported_returncode, replay, record['receipt_published'],
            _event_optional(record['transport_state'], _event_text),
            _event_optional(record['transport_signal'], _event_integer))
    elif kind == 'unreadable':
        if key is None:
            raise ClientProtocolError('unreadable ending requires a watched key')
        verdict = client.ClientUnreadable(key, generation)
    elif kind == 'unavailable':
        verdict = client.ClientUnavailable(key, generation, action_wait.WaitEnd(record['wait_end']))
    elif kind == 'patience_expired':
        if key is None:
            raise ClientProtocolError('client patience requires a watched key')
        verdict = client.ClientPatienceExpired(key, generation)
    elif kind == 'supersession':
        successor = _event_generation(record['successor_published_unix'])
        if key is None or generation is None or successor <= generation:
            raise ClientProtocolError('invalid client publication supersession')
        verdict = client.ClientSuperseded(key, generation, successor)
    elif kind == 'offer_timeout':
        if key is not None or generation is not None:
            raise ClientProtocolError('offer timeout cannot name a runnable identity')
        verdict = client.ClientOfferTimeout()
    elif kind == 'refused':
        verdict = client.ClientRefused(reported_returncode, key, generation)
    elif kind == 'deferred_timeout':
        if key is not None or generation is not None:
            raise ClientProtocolError('deferred timeout cannot name a runnable identity')
        verdict = client.ClientDeferredTimeout(_event_key(record['pending_id']))
    elif kind == 'submitted':
        if identity is None or ending_identity(client.ClientSubmitted(identity)) != (key, generation):
            raise ClientProtocolError('submitted ending differs from its identity')
        verdict = client.ClientSubmitted(identity, _event_optional(record['submission'], _event_path))
    elif kind == 'deferred_submitted':
        if key is not None or generation is not None:
            raise ClientProtocolError('deferred submission cannot name a runnable identity')
        verdict = client.ClientDeferredSubmitted(_event_key(record['pending_id']),
            _event_path(record['submission']), _event_path(record['release']))
    elif kind == 'unsettled':
        if key is None:
            raise ClientProtocolError('unsettled submission requires its known key')
        verdict = client.ClientUnsettled(key, generation, client.UnsettledReason(record['reason']))
    else:
        if key is not None or generation is not None:
            raise ClientProtocolError('command ending cannot name an action')
        verdict = client.ClientCommand(reported_returncode)
    ending = client.ClientEnding(verdict, _parse_history(record))
    if ending.exit_code != reported_returncode:
        raise ClientProtocolError('client verdict contradicts its exit status')
    if client.recovery_permitted(identity, ending) != record['recovery_eligible']:
        raise ClientProtocolError('client recovery flag contradicts its ending')
    return ending


def parse_client_event(raw: bytes, *, queue_root: Path, cas_root: Path,
                       identity: client.ClientIdentity | None = None
                       ) -> client.ClientIdentity | client.ClientEnding:
    if len(raw) > MAX_EVENT_BYTES or not raw.endswith(b'\n') or raw.count(b'\n') != 1:
        raise ClientProtocolError('invalid client event framing')
    try:
        record = decode_strict_json(raw, where='client event')
        if not isinstance(record, dict) or record.get('schema') != CLIENT_EVENT_SCHEMA:
            raise ClientProtocolError('unknown client event schema')
        if record.get('event') == 'identity':
            return _parse_identity(record, queue_root=queue_root, cas_root=cas_root)
        if record.get('event') == 'ending':
            return _parse_ending(record, identity)
        raise ClientProtocolError('unknown client event type')
    except (ValueError, pb.PrismaBuildError, action_wait.OutcomeReadUnavailable) as exc:
        raise ClientProtocolError(str(exc)) from exc


@dataclass(frozen=True)
class ClientEventResult:
    identity: client.ClientIdentity | None
    ending: client.ClientEnding | None
    problem: str | None

    def may_recover(self, *, deadline: float, now: float) -> bool:
        return (self.problem is None and self.ending is not None and now < deadline
                and client.recovery_permitted(self.identity, self.ending))


class ClientEventDecoder:
    def __init__(self, *, queue_root: Path, cas_root: Path,
                 expected_identity: client.ClientIdentity | None = None,
                 on_identity=None):
        self.queue_root = queue_root
        self.cas_root = cas_root
        self.identity = expected_identity
        self.recovery = expected_identity is not None
        self.ending = None
        self.problem = None
        self.pending = b''
        self.bytes_seen = 0
        self.events = 0
        self.on_identity = on_identity

    def feed(self, raw: bytes) -> None:
        if self.problem is not None:
            return
        self.bytes_seen += len(raw)
        if self.bytes_seen > MAX_CLIENT_EVENTS * MAX_EVENT_BYTES:
            self.problem = 'client channel exceeds its total byte limit'
            return
        self.pending += raw
        try:
            while b'\n' in self.pending:
                record, self.pending = self.pending.split(b'\n', 1)
                self.events += 1
                if self.events > MAX_CLIENT_EVENTS:
                    raise ClientProtocolError('too many client events')
                event = parse_client_event(record + b'\n', identity=self.identity,
                                           queue_root=self.queue_root, cas_root=self.cas_root)
                if isinstance(event, client.ClientEnding):
                    if self.ending is not None:
                        raise ClientProtocolError('duplicate client ending')
                    if self.identity is not None and ending_identity(event.verdict)[0] != self.identity.action_key:
                        raise ClientProtocolError('client ending differs from its identity')
                    if isinstance(self.identity, (client.PoolClientIdentity, client.SlurmClientIdentity)):
                        watched = ending_identity(event.verdict)[1]
                        initial = self.identity.published_unix
                        if watched is not None and initial is not None and watched < initial:
                            raise ClientProtocolError('client ending predates its submission')
                    self.ending = event
                else:
                    if self.recovery or self.identity is not None or self.ending is not None:
                        raise ClientProtocolError('duplicate or out-of-order client identity')
                    self.identity = event
                    if self.on_identity is not None:
                        self.on_identity(event)
            if len(self.pending) >= MAX_EVENT_BYTES:
                raise ClientProtocolError('client event exceeds its byte limit')
        except ClientProtocolError as exc:
            self.problem = str(exc)

    def finish(self, returncode: int) -> ClientEventResult:
        if self.problem is None:
            if self.pending:
                self.problem = 'truncated client event'
            elif self.ending is None:
                self.problem = 'missing client ending'
            elif self.ending.exit_code & 255 != returncode:
                self.problem = 'client ending contradicts the process exit status'
            elif self.identity is None and isinstance(self.ending.verdict, (
                    client.ClientExecution, client.ClientUnreadable, client.ClientPatienceExpired, client.ClientSuperseded,
                    client.ClientSubmitted)):
                self.problem = 'client ending has no submission identity'
        return ClientEventResult(self.identity, self.ending, self.problem)


class ClientEventWriter:
    def __init__(self, fd: int, *, expected_identity: client.ClientIdentity | None = None):
        try:
            if (type(fd) is not int or fd < 0 or not stat.S_ISFIFO(os.fstat(fd).st_mode)
                    or fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_WRONLY
                    or os.fpathconf(fd, 'PC_PIPE_BUF') < MAX_EVENT_BYTES):
                raise ClientProtocolError('client event descriptor must be a writable pipe')
            fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.fcntl(fd, fcntl.F_GETFD) | fcntl.FD_CLOEXEC)
            os.set_blocking(fd, False)
        except OSError as exc:
            raise ClientProtocolError('cannot open the client event pipe') from exc
        self.fd = fd
        self.identity = expected_identity
        self.events = 0
        self.complete = True

    def send(self, event: client.ClientIdentity | client.ClientEnding) -> None:
        if self.fd is None:
            return
        try:
            if self.events >= MAX_CLIENT_EVENTS:
                raise ClientProtocolError('too many client events')
            raw = encode_client_event(event, identity=self.identity)
            if os.write(self.fd, raw) != len(raw):
                raise ClientProtocolError('incomplete client event write')
            self.events += 1
            if not isinstance(event, client.ClientEnding):
                self.identity = event
        except (OSError, ValueError, pb.PrismaBuildError):
            self.complete = False
            self.close()

    def close(self) -> None:
        if self.fd is not None:
            fd = self.fd
            self.fd = None
            try:
                os.close(fd)
            except OSError:
                self.complete = False


class ClientProcess:
    def __init__(self, process: subprocess.Popen, decoder: ClientEventDecoder, read_fd: int):
        self.process = process
        self.decoder = decoder
        self._read_fd = read_fd
        self._stop = threading.Event()
        self._reader = threading.Thread(target=self._drain_events,
            name='prismabuild-client-events', daemon=True)
        self._reader.start()

    @classmethod
    def launch(cls, command: list[str], *, queue_root: Path, cas_root: Path,
               env=None, expected_identity: client.ClientIdentity | None = None,
               on_identity=None) -> ClientProcess:
        position = command.index('--') if '--' in command else len(command)
        if any(arg == '--client-events-fd' or arg.startswith('--client-events-fd=')
               for arg in command[:position]):
            raise ValueError('the client launcher owns its event descriptor')
        decoder = ClientEventDecoder(queue_root=queue_root, cas_root=cas_root,
            expected_identity=expected_identity, on_identity=on_identity)
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
        try:
            argv = [*command[:position], '--client-events-fd', str(write_fd), *command[position:]]
            process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, errors='replace', env=env, pass_fds=(write_fd,))
        except BaseException:
            os.close(read_fd)
            raise
        finally:
            os.close(write_fd)
        return cls(process, decoder, read_fd)

    def _drain_events(self) -> None:
        try:
            while self.decoder.problem is None:
                finishing = self._stop.is_set()
                if not finishing and not select.select([self._read_fd], [], [], 0.05)[0]:
                    continue
                try:
                    allowance = MAX_CLIENT_EVENTS * MAX_EVENT_BYTES + 1 - self.decoder.bytes_seen
                    if allowance <= 0:
                        return
                    raw = os.read(self._read_fd, allowance)
                except BlockingIOError:
                    if finishing:
                        return
                    continue
                if not raw:
                    return
                self.decoder.feed(raw)
        except OSError as exc:
            self.decoder.problem = f'client event pipe could not be read: {exc}'
        finally:
            os.close(self._read_fd)

    def wait(self) -> ClientEventResult:
        returncode = self.process.wait()
        self._stop.set()
        self._reader.join()
        return self.decoder.finish(returncode)
