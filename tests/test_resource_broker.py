"""Privileged resource authority must isolate each exact caller-owned attempt."""
import importlib.util
import json
import os
from pathlib import Path
import socket
import threading
import pytest

PATH = Path(__file__).resolve().parents[1] / 'tools/fleet/resource_broker.py'

def module():
    spec=importlib.util.spec_from_file_location('resource_broker', PATH)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

class Backend:
    def __init__(self): self.groups={};self.attached=[];self.stopped=[]
    def create(self, scope, budget):
        self.groups[scope]={'budget':budget,'populated':False}
        return {'cgroup_path':'/fake/'+scope,'leaf_path':'/fake/'+scope+'/payload'}
    def run(self, scope, uid, command, stdio):
        self.groups[scope]['populated']=True;self.attached.append((scope,uid));return object()
    def stop(self, scope):self.groups[scope]['populated']=False;self.stopped.append(scope)
    def empty(self, scope):return not self.groups[scope]['populated']
    def release(self, scope):
        if self.groups[scope]['populated']:raise ValueError('scope still populated')
        self.groups.pop(scope)

@pytest.fixture
def authority(tmp_path):
    m=module();b=Backend();a=m.Authority(tmp_path/'state',os.getuid(),b,max_memory_bytes=1024**3)
    return a,b

def create(a, nonce='b'*32):
    req={'op':'create','action_key':'a'*64,'nonce':nonce,'memory_max_bytes':64*1024**2}
    return req,a.handle(os.getuid(),os.getpid(),req)

def auth(req,record,op):return {**req,'op':op,'token':record['token']}

def launch(a,req,record):
    return a.run(os.getuid(),os.getpid(),{'op':'run','action_key':req['action_key'],
        'nonce':req['nonce'],'token':record['token'],'argv':['/usr/bin/true'],
        'cwd':'/tmp','env':{}},[0,1,2])

def test_two_attempts_are_distinct_and_stopping_one_preserves_the_other(authority):
    a,b=authority;r,x=create(a);s,y=create(a,'c'*32)
    launch(a,r,x);launch(a,s,y)
    a.handle(os.getuid(),os.getpid(),auth(r,x,'stop'))
    assert b.groups[y['scope_id']]['populated']
    assert b.stopped==[x['scope_id']]
    assert x['scope_id']!=y['scope_id']
    assert b.attached==[(x['scope_id'],os.getuid()),(y['scope_id'],os.getuid())]

def test_numeric_process_migration_is_not_exposed(authority):
    a,b=authority;r,x=create(a);request=auth(r,x,'attach');request['pid']=1
    with pytest.raises(ValueError,match='field'):a.handle(os.getuid(),os.getpid(),request)
    assert b.attached==[]

@pytest.mark.parametrize('change',[{'action_key':'../evil'},{'nonce':'../bad'},{'memory_max_bytes':True},{'memory_max_bytes':2**40}])
def test_invalid_creation_never_reaches_privileged_backend(authority,change):
    a,b=authority
    with pytest.raises(ValueError):a.handle(os.getuid(),os.getpid(),{'op':'create','action_key':'a'*64,'nonce':'b'*32,'memory_max_bytes':1024,**change})
    assert not b.groups

def test_wrong_uid_or_token_cannot_stop_job(authority):
    a,b=authority;r,x=create(a)
    with pytest.raises(PermissionError):a.handle(os.getuid()+1,os.getpid(),auth(r,x,'stop'))
    with pytest.raises(PermissionError):a.handle(os.getuid(),os.getpid(),{**auth(r,x,'stop'),'token':'0'*64})
    assert b.stopped==[]

def test_live_scope_cannot_be_released_and_broker_restart_preserves_ownership(authority):
    a,b=authority;r,x=create(a);launch(a,r,x)
    with pytest.raises(ValueError,match='populated'):a.handle(os.getuid(),os.getpid(),auth(r,x,'release'))
    restored=type(a)(a.state_dir,os.getuid(),b,max_memory_bytes=1024**3)
    restored.handle(os.getuid(),os.getpid(),auth(r,x,'stop'));restored.handle(os.getuid(),os.getpid(),auth(r,x,'release'))
    assert not b.groups
    retained = json.loads((a.state_dir / (x['scope_id'] + '.json')).read_text())
    assert retained['released_unix'] > 0
    assert retained['token'] == x['token']

def test_duplicate_create_cannot_change_budget_or_return_new_token(authority):
    a,b=authority;r,x=create(a)
    assert a.handle(os.getuid(),os.getpid(),r)['token']==x['token']
    with pytest.raises(ValueError,match='budget'):a.handle(os.getuid(),os.getpid(),{**r,'memory_max_bytes':128*1024**2})
    assert b.groups[x['scope_id']]['budget']==64*1024**2


def test_stop_is_terminal_and_late_or_duplicate_launch_never_runs(authority):
    a,b=authority;r,x=create(a)
    a.handle(os.getuid(),os.getpid(),auth(r,x,'stop'))
    with pytest.raises(ValueError,match='stopped'):launch(a,r,x)
    with pytest.raises(ValueError,match='stopped'):a.handle(os.getuid(),os.getpid(),r)
    assert b.attached==[]

def test_duplicate_launch_is_not_replayed(authority):
    a,b=authority;r,x=create(a);launch(a,r,x)
    with pytest.raises(ValueError,match='already launched'):launch(a,r,x)
    assert len(b.attached)==1

@pytest.mark.parametrize('op,token',[([],None),('stop','é'*64),('stop',[])])
def test_malformed_types_have_bounded_protocol_errors(authority,op,token):
    a,b=authority;r,x=create(a)
    with pytest.raises((ValueError,PermissionError)):
        a.handle(os.getuid(),os.getpid(),{**auth(r,x,'stop'),'op':op,'token':token})
    assert b.stopped==[]

def test_stop_intent_survives_backend_failure(authority):
    a,b=authority;r,x=create(a)
    def unavailable(scope):raise OSError('interrupted kill')
    b.stop=unavailable
    with pytest.raises(OSError):a.handle(os.getuid(),os.getpid(),auth(r,x,'stop'))
    restored=type(a)(a.state_dir,os.getuid(),b,max_memory_bytes=1024**3)
    with pytest.raises(ValueError,match='stopped'):launch(restored,r,x)
    assert b.attached==[]


def container_ticket(a, record, monkeypatch):
    """Model only the peer's kernel cgroup identity; retain real state IO."""
    original = Path.read_text
    peer_path = Path(f'/proc/{os.getpid()}/cgroup')
    def read(path, *args, **kwargs):
        if path == peer_path:
            return f"0::/prismabuild.slice/{record['scope_id']}/payload\n"
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    return a.handle(os.getuid(), os.getpid(),
                    {'op': 'container_begin', 'scope_id': record['scope_id']})['ticket']


def test_pending_container_retains_stopped_parent_across_restart(authority, monkeypatch):
    a, b = authority; request, record = create(a)
    ticket = container_ticket(a, record, monkeypatch)
    launch(a, request, record)
    with pytest.raises(ValueError, match='not stopped'):
        a.handle(os.getuid(), os.getpid(), auth(request, record, 'release'))
    assert b.groups[record['scope_id']]['populated'] is True
    assert b.stopped == []
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    result = a.handle(os.getuid(), os.getpid(), auth(request, record, 'release'))
    assert result['retired'] is True
    assert record['scope_id'] in b.groups
    restored = type(a)(a.state_dir, os.getuid(), b, max_memory_bytes=1024**3)
    assert restored.records[record['scope_id']]['container_tickets'] == [ticket]
    with pytest.raises(ValueError, match='stopped'):
        launch(restored, request, record)
    with pytest.raises(ValueError, match='stopped'):
        restored.handle(os.getuid(), os.getpid(),
                        {'op': 'container_begin', 'scope_id': record['scope_id']})


def test_retirement_cannot_trust_stop_intent_after_freeze_failure(authority, monkeypatch):
    a, b = authority; request, record = create(a)
    container_ticket(a, record, monkeypatch)
    def cannot_freeze(scope):
        raise OSError('kernel freeze unavailable')
    monkeypatch.setattr(b, 'stop', cannot_freeze)
    with pytest.raises(OSError, match='freeze unavailable'):
        a.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    # Empty does not mean frozen: an in-flight Docker RPC may still arrive.
    with pytest.raises(OSError, match='freeze unavailable'):
        a.handle(os.getuid(), os.getpid(), auth(request, record, 'release'))
    assert not a.records[record['scope_id']].get('retired_unix')


@pytest.mark.parametrize('change', [
    {'scope_id': '../system.slice'}, {'scope_id': []}, {'pid': 1},
    {'ticket': '0'*64},
])
def test_invalid_container_begin_fields_never_register_intent(authority, change):
    a, b = authority; _, record = create(a)
    with pytest.raises(ValueError):
        a.handle(os.getuid(), os.getpid(),
                 {'op': 'container_begin', 'scope_id': record['scope_id'], **change})
    assert not a.records[record['scope_id']].get('container_tickets')


def test_container_end_requires_exact_ticket_and_preserves_other_intents(authority, monkeypatch):
    a, b = authority; _, record = create(a)
    first = container_ticket(a, record, monkeypatch)
    second = container_ticket(a, record, monkeypatch)
    request = {'op': 'container_end', 'scope_id': record['scope_id'], 'ticket': first}
    with pytest.raises(PermissionError):
        a.handle(os.getuid()+1, os.getpid(), request)
    with pytest.raises(PermissionError):
        a.handle(os.getuid(), os.getpid(), {**request, 'ticket': '0'*64})
    a.handle(os.getuid(), os.getpid(), request)
    assert a.records[record['scope_id']]['container_tickets'] == [second]
    with pytest.raises(PermissionError):
        a.handle(os.getuid(), os.getpid(), request)


def test_released_authority_survives_retry_without_touching_another_group(authority):
    a, b = authority; request, record = create(a)
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'release'))
    assert record['scope_id'] not in b.groups
    restored = type(a)(a.state_dir, os.getuid(), b, max_memory_bytes=1024**3)
    # A same-name group appearing later is not covered by completed authority.
    b.groups[record['scope_id']] = {'budget': 1, 'populated': True}
    for operation in ('release', 'stop', 'status'):
        answer = restored.handle(os.getuid(), os.getpid(), auth(request, record, operation))
        assert answer.get('released') is True
    assert b.groups[record['scope_id']]['populated'] is True
    assert b.stopped == []
    with pytest.raises(ValueError, match='released'):
        restored.handle(os.getuid(), os.getpid(), request)
    with pytest.raises(ValueError, match='released'):
        launch(restored, request, record)


@pytest.mark.parametrize('message', [
    b'[]\n', b'{"op": []}\n', b'{not-json}\n',
    b' ' * 65536 + b'\n',
    json.dumps({'op': 'run', 'argv': ['/usr/bin/true'], 'cwd': '/',
                'env': {}, 'affinity': [{}]}).encode() + b'\n',
])
def test_socket_handler_rejects_invalid_protocol_without_backend_work(authority, tmp_path, message):
    a, b = authority
    m = module()
    endpoint = tmp_path / 'broker.sock'
    with m.Server(str(endpoint), m.Handler) as server:
        server.authority = a
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={'poll_interval': 0.01}, daemon=True)
        thread.start()
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(5)
                client.connect(str(endpoint))
                client.sendall(message)
                answer = bytearray()
                while b'\n' not in answer:
                    block = client.recv(4096)
                    assert block, 'handler must return a structured refusal'
                    answer.extend(block)
                result = json.loads(answer)
                assert result['ok'] is False
                assert isinstance(result['error'], str)
                assert len(result['error']) <= 1500
        finally:
            server.shutdown()
            thread.join(timeout=5)
    assert not b.groups
    assert not a.records
