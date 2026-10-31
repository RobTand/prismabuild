"""Privileged resource authority must isolate each exact caller-owned attempt."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import socket
import threading
from types import SimpleNamespace
import pytest

PATH = Path(__file__).resolve().parents[1] / 'tools/fleet/resource_broker.py'

def module():
    spec=importlib.util.spec_from_file_location('resource_broker', PATH)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

class Backend:
    #: ``identity`` and ``charge`` are opt-in per scope: a test that says
    #: nothing about them gets exactly the inventory rows this fake always
    #: produced, and records created through it carry no ``cgroup_identity``.
    #: ``ops`` is the order log, because "reclaim before removal" is a claim
    #: about sequence that a pair of end-state assertions cannot make.
    def __init__(self):
        self.groups={};self.attached=[];self.stopped=[]
        self.identity={};self.charge={};self.ops=[]
    def create(self, scope, budget):
        self.groups[scope]={'budget':budget,'populated':False}
        return {'cgroup_path':'/fake/'+scope,'leaf_path':'/fake/'+scope+'/payload'}
    def run(self, scope, uid, command, stdio):
        self.groups[scope]['populated']=True;self.attached.append((scope,uid));return object()
    def stop(self, scope):
        self.ops.append(('stop',scope))
        self.groups[scope]['populated']=False;self.stopped.append(scope)
    def empty(self, scope):return scope not in self.groups or not self.groups[scope]['populated']
    def exists(self, scope):return scope in self.groups
    def path(self, scope):return Path('/sys/fs/cgroup/prismabuild.slice')/scope
    def healthy(self):return True
    def _row(self, scope):
        return {'populated':self.groups[scope]['populated'],'frozen':scope in self.stopped,
                **({'identity':self.identity[scope]} if scope in self.identity else {})}
    def inventory(self):
        return {scope:self._row(scope) for scope in self.groups}
    def observe(self, scope):
        self.ops.append(('observe',scope))
        return self._row(scope) if scope in self.groups else None
    def reclaim(self, scope):
        self.ops.append(('reclaim',scope))
        before=self.charge.get(scope,0);self.charge[scope]=0
        return {'before':before,'after':0,'complete':True,'page_bytes_after':0}
    def release(self, scope):
        self.ops.append(('release',scope))
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


def test_separate_gpu_budget_can_exceed_host_ram_and_survives_recovery(authority):
    a,b=authority
    request={'op':'create','action_key':'a'*64,'nonce':'b'*32,
             'memory_max_bytes':64*1024**2,'gpu_memory_max_bytes':4*1024**3}
    record=a.handle(os.getuid(),os.getpid(),request)
    assert record['gpu_memory_max_bytes']==4*1024**3
    assert b.groups[record['scope_id']]['budget']==64*1024**2
    restored=module().Authority(a.state_dir,os.getuid(),b,max_memory_bytes=1024**3)
    recovery=restored.handle(os.getuid(),os.getpid(),{**request,'op':'recover_create'})
    assert recovery['token']==record['token']
    assert recovery['gpu_memory_max_bytes']==4*1024**3
    for operation in ['create','recover_create']:
        with pytest.raises(ValueError,match='budget cannot change'):
            restored.handle(os.getuid(),os.getpid(),
                            {**request,'op':operation,'gpu_memory_max_bytes':8*1024**3})


def test_legacy_gpu_budget_defaults_to_system_budget(authority):
    a,b=authority
    request,record=create(a)
    assert record['gpu_memory_max_bytes']==record['memory_max_bytes']
    assert a.handle(os.getuid(),os.getpid(),
                    {**request,'gpu_memory_max_bytes':request['memory_max_bytes']})['token']==record['token']


def test_recovery_of_pre_gpu_budget_record_restores_legacy_default(authority):
    a,b=authority
    request,record=create(a)
    del a.records[record['scope_id']]['gpu_memory_max_bytes']
    recovered=a.handle(os.getuid(),os.getpid(),{**request,'op':'recover_create',
                                              'gpu_memory_max_bytes':request['memory_max_bytes']})
    assert recovered['gpu_memory_max_bytes']==record['memory_max_bytes']
    stored=json.loads((a.state_dir/(record['scope_id']+'.json')).read_text())
    assert stored['gpu_memory_max_bytes']==record['memory_max_bytes']


@pytest.mark.parametrize('budget',[True,0,-1,1.5,2**63])
def test_invalid_gpu_budget_never_creates_kernel_scope(authority,budget):
    a,b=authority
    with pytest.raises(ValueError,match='GPU memory budget'):
        a.handle(os.getuid(),os.getpid(),{'op':'create','action_key':'a'*64,'nonce':'b'*32,
                                       'memory_max_bytes':1024,'gpu_memory_max_bytes':budget})
    assert not b.groups

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


@pytest.mark.parametrize('mask', [True, -1, 0o1000, '022', [], None])
def test_invalid_umask_never_launches_payload(authority, mask):
    a, b = authority; request, record = create(a)
    run = {'op': 'run', 'action_key': request['action_key'], 'nonce': request['nonce'],
           'token': record['token'], 'argv': ['/usr/bin/true'], 'cwd': '/',
           'env': {}, 'umask': mask}
    with pytest.raises(ValueError, match='umask'):
        a.run(os.getuid(), os.getpid(), run, [0, 1, 2])
    assert not b.attached
    assert 'launched_unix' not in a.records[record['scope_id']]


@pytest.mark.parametrize('mask', [0, 0o022, 0o777])
def test_valid_umask_is_forwarded_to_privilege_dropping_helper(authority, monkeypatch, mask):
    a, b = authority; request, record = create(a)
    commands = []
    monkeypatch.setattr(b, 'run', lambda scope, uid, command, stdio: commands.append(command))
    a.run(os.getuid(), os.getpid(),
          {'op': 'run', 'action_key': request['action_key'], 'nonce': request['nonce'],
           'token': record['token'], 'argv': ['/usr/bin/true'], 'cwd': '/',
           'env': {}, 'umask': mask}, [0, 1, 2])
    assert commands[0]['umask'] == mask


@pytest.mark.parametrize('operation', ['stop', 'release', 'status'])
def test_unknown_attempt_after_reboot_only_accepts_proven_absence(authority, operation):
    a, b = authority; request, record = create(a)
    a.records.clear()
    b.groups.clear()
    assert a.handle(os.getuid(), os.getpid(), auth(request, record, operation))['released'] is True
    # An unknown existing group must not be adopted or stopped, even if empty.
    b.groups[record['scope_id']] = {'budget': 1, 'populated': False}
    with pytest.raises(PermissionError):
        a.handle(os.getuid(), os.getpid(), auth(request, record, operation))
    assert b.groups[record['scope_id']]['populated'] is False
    assert not b.stopped
    assert not a.records


class GpuSamples:
    def __init__(self):self.decisions=[];self.calls=[];self.during_collect=None
    def Scope(self, scope_id, cgroup_path, budget_bytes, *, gpu_budget_bytes=None):
        return SimpleNamespace(scope_id=scope_id,cgroup_path=cgroup_path,budget_bytes=budget_bytes,
                               gpu_budget_bytes=gpu_budget_bytes)
    def Guard(self):return self
    def collect(self, scopes, *, timeout_s):
        self.calls.append((scopes,timeout_s))
        if self.during_collect:self.during_collect()
        return self
    def observe(self, snapshot):return self.decisions
    def as_dict(self):return {'source':'fake-gpu-counter'}


@pytest.fixture
def monitored(authority, tmp_path, monkeypatch):
    a,b=authority;rows=[]
    monkeypatch.setattr(b,'path',lambda scope:tmp_path/'groups'/scope,raising=False)
    for nonce in ('b'*32,'c'*32):
        request,record=create(a,nonce)
        path=b.path(record['scope_id']);path.mkdir(parents=True)
        (path/'memory.events').write_text('oom_kill 0\n')
        (path/'memory.events.local').write_text('oom 0\n')
        (path/'cgroup.events').write_text('populated 0\nfrozen 0\n')
        info=path.stat()
        a.records[record['scope_id']]['cgroup_identity']=[info.st_dev,info.st_ino]
        a.records[record['scope_id']]['memory_oom_kill_baseline']=0
        rows.append((request,record,path))
    gpu=GpuSamples();monitor=module().ResourceMonitor(a,gpu)
    try:
        yield a,b,rows,gpu,monitor
    finally:
        monitor.close()


def decision(record, path, *, scope=None, identity=None):
    info=path.stat()
    data={'scope_id':scope or record['scope_id'],'reason':'gpu_budget_exceeded',
          'cgroup_identity':identity or (info.st_dev,info.st_ino),
          'evidence':{'lower_bound_bytes':128*1024**2}}
    return SimpleNamespace(**data,as_dict=lambda:data)


class ControlledCgroupEvents:
    def __init__(self):
        self.watched={};self.enrollments=[];self.removed=[]
        self.dirty=set();self.lost=set();self.fail=set()
        self.on_watch=None;self.on_wait=None;self.closed=False
        self.read_error=None
        self.waiting=threading.Event();self.drain_limits=[]

    def watch(self,scope,path,identity):
        self.enrollments.append((scope,tuple(identity)))
        if scope in self.fail:raise OSError('injected cgroup watch loss')
        self.watched[scope]=(path,tuple(identity))
        if self.on_watch:self.on_watch(scope,path)

    def unwatch(self,scope):
        self.removed.append(scope);self.watched.pop(scope,None)

    def drain(self,max_events=256):
        self.drain_limits.append(max_events)
        if self.read_error is not None:raise self.read_error
        dirty,lost=self.dirty,self.lost
        self.dirty=set();self.lost=set()
        return dirty,lost

    def wait(self,wake_fd,timeout_s):
        self.waiting.set()
        if self.on_wait:self.on_wait(timeout_s)
        else:
            import select
            select.select([wake_fd],[],[],timeout_s)

    def close(self):self.closed=True


class MonitorClock:
    def __init__(self):self.now=100.0
    def __call__(self):return self.now
    def advance(self,seconds):self.now+=seconds


@pytest.fixture
def event_monitored(monitored):
    a,b,rows,gpu,old=monitored
    old.close()
    events=ControlledCgroupEvents();clock=MonitorClock()
    monitor=type(old)(a,gpu,event_source_factory=lambda:events,clock=clock)
    try:
        yield a,b,rows,gpu,monitor,events,clock
    finally:
        monitor.close()


def test_cgroup_event_invalidations_read_only_dirty_scope_and_do_not_sample_gpu(event_monitored):
    a,b,rows,gpu,monitor,events,clock=event_monitored
    monitor.poll_once();sampled=monitor.last_success_monotonic
    for _,_,path in rows:(path/'memory.events.local').write_text('oom 1\n')
    scope=rows[0][1]['scope_id'];events.dirty.add(scope);clock.advance(2)
    status=monitor.poll_events_once()
    assert status['stopped']==[scope]
    assert b.stopped==[scope]
    assert len(gpu.calls)==1
    assert monitor.last_success_monotonic==sampled
    assert 'stopped_unix' not in a.records[rows[1][1]['scope_id']]


def test_cgroup_watch_is_enrolled_before_initial_oom_snapshot(event_monitored):
    a,b,rows,gpu,monitor,events,_=event_monitored
    scope=rows[0][1]['scope_id']
    def during_watch(name,path):
        if name==scope:(path/'memory.events.local').write_text('oom 1\n')
    events.on_watch=during_watch
    assert monitor.poll_once()['stopped']==[scope]
    assert b.stopped==[scope]
    assert a.records[scope]['termination_evidence']['oom_local']==1


def test_new_scope_create_wakes_watch_reconciliation(event_monitored,monkeypatch):
    a,b,rows,gpu,monitor,events,_=event_monitored
    monitor.poll_once();original=b.create
    def create_group(scope,budget):
        result=original(scope,budget);path=b.path(scope);path.mkdir()
        (path/'memory.events').write_text('oom_kill 0\n')
        (path/'memory.events.local').write_text('oom 1\n')
        (path/'cgroup.events').write_text('populated 1\nfrozen 0\n')
        info=path.stat()
        return {**result,'cgroup_identity':[info.st_dev,info.st_ino],
                'memory_oom_local_baseline':0,'memory_oom_kill_baseline':0}
    monkeypatch.setattr(b,'create',create_group)
    _,record=create(a,'d'*32);scope=record['scope_id']
    assert monitor.poll_events_once()['stopped']==[scope]
    assert scope in events.watched
    assert b.stopped==[scope]
    assert len(gpu.calls)==1


def test_recovered_scopes_are_watched_before_first_numeric_sample(event_monitored):
    a,b,rows,gpu,old,_,clock=event_monitored
    old.close();m=module()
    for _,record,path in rows:
        m._atomic(a.state_dir/(record['scope_id']+'.json'),a.records[record['scope_id']])
    restored=m.Authority(a.state_dir,os.getuid(),b,max_memory_bytes=1024**3)
    scope=rows[0][1]['scope_id'];(rows[0][2]/'memory.events.local').write_text('oom 1\n')
    events=ControlledCgroupEvents()
    monitor=m.ResourceMonitor(restored,gpu,event_source_factory=lambda:events,clock=clock)
    try:
        assert monitor.poll_events_once()['stopped']==[scope]
        assert set(events.watched)=={row[1]['scope_id'] for row in rows}
        assert b.stopped==[scope]
        assert not gpu.calls
        assert monitor.last_success_monotonic is None
    finally:
        monitor.close()


def test_replaced_cgroup_cannot_reuse_old_watch_identity(event_monitored):
    a,b,rows,gpu,monitor,events,_=event_monitored
    monitor.poll_once();_,record,path=rows[0];scope=record['scope_id']
    path.rename(path.with_name(path.name+'.old'));path.mkdir()
    (path/'memory.events').write_text('oom_kill 1\n')
    (path/'memory.events.local').write_text('oom 1\n')
    (path/'cgroup.events').write_text('populated 1\nfrozen 0\n')
    events.dirty.add(scope)
    status=monitor.poll_events_once()
    assert status['errors']
    assert not b.stopped
    assert 'stopped_unix' not in a.records[scope]


def test_cgroup_event_stop_retry_does_not_require_another_notification(event_monitored,monkeypatch):
    a,b,rows,gpu,monitor,events,clock=event_monitored
    monitor.poll_once();scope=rows[0][1]['scope_id']
    (rows[0][2]/'memory.events.local').write_text('oom 1\n')
    original=b.stop
    def fail_once(name):raise OSError('injected stop failure')
    monkeypatch.setattr(b,'stop',fail_once);events.dirty.add(scope)
    assert monitor.poll_events_once()['errors']
    assert a.records[scope]['monitor_stop_pending'] is True
    monkeypatch.setattr(b,'stop',original)
    clock.advance(10)
    assert monitor.poll_events_once()['stopped']==[scope]
    assert a.records[scope]['monitor_stop_pending'] is False
    assert b.stopped==[scope]
    assert len(gpu.calls)==1


@pytest.mark.parametrize('field',['token','memory_max_bytes','gpu_memory_max_bytes'])
def test_cgroup_watch_identity_includes_attempt_and_budgets(event_monitored,field):
    a,b,rows,gpu,monitor,events,_=event_monitored
    monitor.poll_once();scope=rows[0][1]['scope_id']
    if field=='token':a.records[scope][field]='f'*64
    else:a.records[scope][field]*=2
    monitor.poll_events_once()
    assert [name for name,_ in events.enrollments].count(scope)==2
    assert scope in events.removed
    assert not b.stopped


def test_retired_unreleased_scope_keeps_population_interest(event_monitored):
    a,b,rows,gpu,monitor,events,_=event_monitored
    request,record,path=rows[0];scope=record['scope_id']
    monitor.poll_once()
    a.handle(os.getuid(),os.getpid(),auth(request,record,'stop'))
    monitor.poll_events_once()
    assert scope in events.watched
    (path/'memory.events.local').write_text('oom 1\n')
    events.dirty.add(scope)
    monitor.poll_events_once()
    assert b.stopped==[scope]
    a.handle(os.getuid(),os.getpid(),auth(request,record,'release'))
    monitor.poll_events_once()
    assert scope not in events.watched


def test_cgroup_watch_loss_is_visible_and_does_not_spin_on_numeric_ticks(event_monitored):
    _,_,rows,gpu,monitor,events,clock=event_monitored
    monitor.poll_once();scope=rows[0][1]['scope_id']
    events.lost.add(scope);events.fail.add(scope)
    lost=monitor.poll_events_once()
    assert lost['errors']
    assert not lost['event_coverage']['complete']
    assert lost['event_coverage']['scopes'][scope].get('populated') is None
    attempts=len(events.enrollments)
    for _ in range(3):assert monitor.poll_once()['errors']
    assert len(events.enrollments)==attempts
    assert len(gpu.calls)==4
    events.fail.clear();clock.advance(10)
    recovered=monitor.poll_once()
    assert not recovered['errors']
    assert recovered['event_coverage']['complete']
    assert len(events.enrollments)>attempts


def test_monitor_close_wakes_blocked_event_wait(event_monitored):
    _,_,_,_,monitor,events,_=event_monitored
    import select
    read,write=os.pipe();finished=threading.Event()
    events.on_wait=lambda timeout:select.select([monitor.wake_read,read],[],[])
    def close():
        monitor.close();finished.set()
    closer=threading.Thread(target=close,daemon=True)
    monitor.start()
    try:
        assert events.waiting.wait(60), 'monitor never reached its event wait'
        closer.start()
        closed=finished.wait(60)
    finally:
        os.write(write,b'x')
        if closer.ident is not None:closer.join(timeout=60)
        monitor.close();os.close(read);os.close(write)
    assert closed, 'close did not wake the event wait'
    assert not monitor.thread.is_alive()
    assert events.closed


def test_continuous_events_do_not_postpone_numeric_deadline(event_monitored):
    _,_,rows,gpu,monitor,events,clock=event_monitored
    scope=rows[0][1]['scope_id'];timeouts=[]
    def ready(timeout):
        timeouts.append(timeout);clock.advance(0.25);events.dirty.add(scope)
        if len(timeouts)>32:pytest.fail('continuous events starved numeric sampling')
    def sampled():
        if len(gpu.calls)==3:monitor.stopping.set()
    events.on_wait=ready;gpu.during_collect=sampled
    monitor._loop()
    assert len(gpu.calls)==3
    assert timeouts and all(0<=value<=monitor.interval_s for value in timeouts)
    assert all(0<limit<=256 for limit in events.drain_limits)


@pytest.mark.parametrize('filename',['memory.events','memory.events.local','cgroup.events'])
def test_linux_cgroup_event_source_invalidates_on_each_watched_file(monitored,filename):
    _,_,rows,_,_=monitored
    _,record,path=rows[0];source=module().CgroupEventSource();scope=record['scope_id']
    info=path.stat()
    try:
        source.watch(scope,path,(info.st_dev,info.st_ino))
        (path/filename).write_text('oom 1\noom_kill 1\npopulated 0\nfrozen 0\n')
        dirty,lost=source.drain()
        assert scope in dirty
        assert not lost
    finally:
        source.close()


def test_linux_cgroup_event_source_reports_deleted_input_as_lost_coverage(monitored):
    _,_,rows,_,_=monitored
    _,record,path=rows[0];source=module().CgroupEventSource();scope=record['scope_id']
    info=path.stat()
    try:
        source.watch(scope,path,(info.st_dev,info.st_ino))
        (path/'memory.events.local').unlink()
        _,lost=source.drain()
        assert scope in lost
    finally:
        source.close()


def test_linux_cgroup_event_source_overflow_invalidates_all_retained_scopes(monitored,monkeypatch):
    _,_,rows,_,_=monitored
    import struct
    m=module();source=m.CgroupEventSource()
    packets=iter([struct.pack('iIII',-1,0x4000,0,0)])
    def read_overflow(fd,size):
        try:return next(packets)
        except StopIteration:raise BlockingIOError()
    try:
        for _,record,path in rows:
            info=path.stat();source.watch(record['scope_id'],path,(info.st_dev,info.st_ino))
        with monkeypatch.context() as patch:
            patch.setattr(m.os,'read',read_overflow)
            with pytest.raises(OSError,match='overflow'):
                source.drain()
    finally:
        source.close()


def test_linux_cgroup_event_source_caps_work_before_discarding_uncertain_queue(monitored,monkeypatch):
    import struct
    m=module();source=m.CgroupEventSource()
    packet=struct.pack('iIII',-2,0x2,0,0)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(m.os,'read',lambda fd,size:packet*2)
            with pytest.raises(OSError,match='budget'):
                source.drain(max_events=1)
    finally:
        source.close()


def test_event_only_capacity_invalidation_keeps_numeric_timestamp(event_monitored,monkeypatch):
    a,_,rows,gpu,monitor,events,clock=event_monitored
    original=gpu.collect
    monkeypatch.setattr(gpu,'collect',lambda scopes,**options:original(scopes,timeout_s=options['timeout_s']))
    class Capacity:
        def devices(self,**options):return [],[]
        def collect(self,*args,**options):
            return {'complete':True,'errors':[],'sampled_unix':clock()}
    monitor.capacity=Capacity();monitor.poll_once()
    path=a.state_dir.parent/'gpu-capacity.json';before=json.loads(path.read_text())
    assert before['complete']
    clock.advance(2);scope=rows[0][1]['scope_id']
    (rows[0][2]/'cgroup.events').write_text('populated 1\nfrozen 0\n')
    events.dirty.add(scope);monitor.poll_events_once()
    changed=json.loads(path.read_text())
    assert changed['complete'] is False
    assert changed['sampled_unix']==before['sampled_unix']
    assert len(gpu.calls)==1
    monitor.poll_once();fresh=json.loads(path.read_text())
    assert fresh['complete']
    assert fresh['sampled_unix']==clock()


def test_cgroup_source_failure_recreates_descriptor_after_recovery_deadline(event_monitored):
    import errno
    a,b,rows,gpu,old,_,clock=event_monitored
    old.close();first=ControlledCgroupEvents();second=ControlledCgroupEvents()
    sources=iter([first,second]);monitor=type(old)(a,gpu,event_source_factory=lambda:next(sources),clock=clock)
    try:
        monitor.poll_once();first.read_error=OSError(errno.ENOBUFS,'injected overflow')
        status=monitor.poll_events_once()
        assert first.closed
        assert not status['event_coverage']['complete']
        assert all('populated' not in row for row in status['event_coverage']['scopes'].values())
        monitor.poll_events_once()
        assert not second.watched
        clock.advance(monitor.recovery_interval_s)
        recovered=monitor.poll_events_once()
        assert recovered['event_coverage']['complete']
        assert set(second.watched)=={row[1]['scope_id'] for row in rows}
        assert len(gpu.calls)==1
    finally:
        monitor.close()


def test_scope_change_wake_after_inventory_snapshot_is_not_consumed(event_monitored,monkeypatch):
    import select
    a,_,rows,_,monitor,_,_=event_monitored
    monitor.poll_once();original=monitor._inventory
    request,record,_=rows[0]
    def snapshot_then_retire():
        result=original()
        monkeypatch.setattr(monitor,'_inventory',original)
        a.handle(os.getuid(),os.getpid(),auth(request,record,'stop'))
        return result
    monkeypatch.setattr(monitor,'_inventory',snapshot_then_retire)
    monitor.poll_events_once()
    assert select.select([monitor.wake_read],[],[],0)[0]==[monitor.wake_read], (
        'scope-change wake arriving after inventory must remain ready for reconciliation')
    assert monitor.poll_events_once()['active_scopes']==1


def test_failed_enrollment_prefix_cannot_starve_a_healthy_scope(event_monitored):
    _,_,rows,_,monitor,events,clock=event_monitored
    monitor.event_work_limit=1
    events.fail.add(rows[0][1]['scope_id'])
    healthy=rows[1][1]['scope_id']
    for _ in range(3):
        monitor.poll_events_once()
        clock.advance(monitor.recovery_interval_s)
    assert healthy in events.watched, 'failed enrollment prefix starved a healthy later scope'


def test_gpu_monitor_stops_only_attributed_attempt_and_preserves_first_cause(monitored):
    a,b,rows,gpu,monitor=monitored
    request,record,path=rows[0];gpu.decisions=[decision(record,path)]
    status=monitor.poll_once()
    assert status['stopped']==[record['scope_id']]
    assert b.stopped==[record['scope_id']]
    first=a.records[record['scope_id']]['stopped_unix']
    a.handle(os.getuid(),os.getpid(),{**auth(request,record,'stop'),'reason':'completion cleanup'})
    status=a.handle(os.getuid(),os.getpid(),auth(request,record,'status'))
    assert status['stop_reason']=='gpu_budget_exceeded'
    assert status['stopped_unix']==first
    assert status['last_cleanup_reason']=='completion cleanup'
    assert status['termination_evidence']['evidence']['lower_bound_bytes']==128*1024**2
    assert 'stopped_unix' not in a.records[rows[1][1]['scope_id']]


@pytest.mark.parametrize('change', ['token','gpu_budget','inode','released','foreign_decision'])
def test_gpu_monitor_revalidates_authority_and_kernel_identity_after_sampling(monitored, change):
    a,b,rows,gpu,monitor=monitored
    request,record,path=rows[0];gpu.decisions=[decision(record,path)]
    def mutate():
        if change=='token':a.records[record['scope_id']]['token']='f'*64
        elif change=='gpu_budget':a.records[record['scope_id']]['gpu_memory_max_bytes']*=2
        elif change=='released':a.records[record['scope_id']]['released_unix']=1
        elif change=='inode':
            path.rename(path.with_name(path.name+'.old'))
            path.mkdir();(path/'memory.events').write_text('oom_kill 0\n')
        else:gpu.decisions=[decision(record,path,scope='system.slice')]
    gpu.during_collect=mutate
    assert monitor.poll_once()['stopped']==[]
    assert b.stopped==[]


def test_gpu_census_does_not_hold_authority_lock(monitored):
    a,b,rows,gpu,monitor=monitored
    acquired=threading.Event()
    def census():
        def other_request():
            with a.lock:acquired.set()
        thread=threading.Thread(target=other_request,daemon=True);thread.start()
        assert acquired.wait(1), 'GPU census blocked ordinary broker requests'
        thread.join(timeout=1)
    gpu.during_collect=census
    monitor.poll_once()
    assert gpu.calls[0][1]==1.0


def test_memory_oom_finishes_entire_attempt_before_gpu_sampling(monitored):
    a,b,rows,gpu,monitor=monitored
    request,record,path=rows[0]
    (path/'memory.events').write_text('oom_kill 1\n')
    (path/'memory.events.local').write_text('oom 1\noom_kill 0\n')
    assert monitor.poll_once()['stopped']==[record['scope_id']]
    assert b.stopped==[record['scope_id']]
    assert [scope.scope_id for scope in gpu.calls[0][0]]==[rows[1][1]['scope_id']]
    evidence=a.records[record['scope_id']]['termination_evidence']
    assert evidence['source']=='cgroup.memory.events.local'
    assert evidence['oom_local']==1
    assert evidence['oom_kill']==1


def test_child_local_oom_does_not_kill_the_parent_attempt(monitored):
    a,b,rows,gpu,monitor=monitored
    _,record,path=rows[0]
    # A child capped at 1GiB failed inside a PB attempt capped at 8GiB.
    # Victim counts propagate; the ancestor's own limit did not trigger OOM.
    (path/'memory.events').write_text('max 23\noom 1\noom_kill 1\n')
    (path/'memory.events.local').write_text('max 0\noom 0\noom_kill 0\n')
    assert monitor.poll_once()['stopped']==[]
    assert not b.stopped
    assert 'stopped_unix' not in a.records[record['scope_id']]


def test_parent_limit_exhaustion_stops_before_a_victim_is_counted(monitored):
    a,b,rows,gpu,monitor=monitored
    _,record,path=rows[0]
    (path/'memory.events.local').write_text('oom 1\noom_kill 0\n')
    assert monitor.poll_once()['stopped']==[record['scope_id']]
    assert b.stopped==[record['scope_id']]
    assert a.records[record['scope_id']]['termination_evidence']['oom_kill']==0


def test_old_parent_oom_plus_new_child_victim_is_not_new_exhaustion(monitored):
    a,b,rows,gpu,monitor=monitored
    _,record,path=rows[0]
    a.records[record['scope_id']]['memory_oom_local_baseline']=3
    (path/'memory.events.local').write_text('oom 3\noom_kill 0\n')
    (path/'memory.events').write_text('oom 4\noom_kill 1\n')
    assert monitor.poll_once()['stopped']==[]
    assert not b.stopped


def test_preexisting_oom_counter_is_not_a_new_failure(monitored):
    a,b,rows,gpu,monitor=monitored
    _,record,path=rows[0]
    a.records[record['scope_id']]['memory_oom_kill_baseline']=7
    (path/'memory.events').write_text('oom_kill 7\n')
    assert monitor.poll_once()['stopped']==[]
    assert not b.stopped


def test_failed_monitor_stop_is_retried_with_persisted_evidence(monitored, monkeypatch):
    a,b,rows,gpu,monitor=monitored
    _,record,path=rows[0];(path/'memory.events').write_text('oom_kill 1\n')
    (path/'memory.events.local').write_text('oom 1\n')
    original=b.stop
    def fail(scope):raise OSError('temporary cgroup failure')
    monkeypatch.setattr(b,'stop',fail)
    assert monitor.poll_once()['errors']
    stored=json.loads((a.state_dir/(record['scope_id']+'.json')).read_text())
    assert stored['monitor_stop_pending'] is True
    assert stored['stop_reason']=='memory_limit_oom'
    monkeypatch.setattr(b,'stop',original)
    assert monitor.poll_once()['stopped']==[record['scope_id']]
    assert a.records[record['scope_id']]['monitor_stop_pending'] is False


@pytest.mark.parametrize('operation',['maintenance_begin','maintenance_status','maintenance_end'])
def test_maintenance_requires_root_without_changing_admission(authority,operation):
    a,b=authority
    with pytest.raises(PermissionError,match='root'):
        a.handle(1000,os.getpid(),{'op':operation})
    assert not a.maintenance['draining']
    assert not a.maintenance_path.exists()
    assert not b.groups


def test_drain_survives_restart_and_existing_scope_can_finish(authority):
    a,b=authority;request,record=create(a)
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['draining'] and status['active_scopes']==1 and status['health']
    assert a.maintenance_path.stat().st_mode&0o777==0o644
    restored=type(a)(a.state_dir,os.getuid(),b,max_memory_bytes=1024**3)
    blocked=restored.handle(os.getuid(),os.getpid(),{**request,'nonce':'c'*32})
    assert blocked['ok'] is False and blocked['maintenance'] is True and blocked['retryable'] is True
    assert restored.handle(os.getuid(),os.getpid(),request)['token']==record['token']
    with pytest.raises(ValueError,match='budget'):
        restored.handle(os.getuid(),os.getpid(),{**request,'memory_max_bytes':128*1024**2})
    launch(restored,request,record)
    assert b.stopped==[]
    restored.handle(os.getuid(),os.getpid(),auth(request,record,'stop'))
    restored.handle(os.getuid(),os.getpid(),auth(request,record,'release'))
    assert restored.handle(0,os.getpid(),{'op':'maintenance_status'})['active_scopes']==0
    assert restored.handle(0,os.getpid(),{'op':'maintenance_end'})['draining'] is False
    assert json.loads(a.maintenance_path.read_text())['draining'] is False
    assert restored.handle(os.getuid(),os.getpid(),{**request,'nonce':'c'*32})['ok'] is True


def test_failed_gate_persistence_does_not_acknowledge_drain(authority,monkeypatch):
    a,b=authority
    globals_=type(a).admin.__globals__;original=globals_['_atomic']
    def fail_gate(path,value,**kwargs):
        if path==a.maintenance_path:raise OSError('gate write failed')
        return original(path,value,**kwargs)
    monkeypatch.setitem(globals_,'_atomic',fail_gate)
    with pytest.raises(OSError,match='gate write failed'):
        a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert not a.maintenance['draining']
    assert create(a)[1]['ok'] is True


def test_drain_safely_retires_empty_failed_creation_without_restart_or_kill(authority,monkeypatch):
    a,b=authority;original=b.create
    def partial(scope,budget):
        original(scope,budget)
        raise OSError('controller setup failed')
    monkeypatch.setattr(b,'create',partial)
    with pytest.raises(OSError):create(a)
    scope=next(iter(a.records))
    assert a.records[scope]['pending'] is True
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['active_scopes']==0 and status['health'] is True
    assert a.records[scope]['released_unix']>0
    assert not b.groups and not b.stopped


@pytest.mark.parametrize('ambiguity',['populated','launched','docker'])
def test_drain_preserves_ambiguous_pending_scopes(authority,monkeypatch,ambiguity):
    a,b=authority;original=b.create
    def partial(scope,budget):
        original(scope,budget)
        raise OSError('controller setup failed')
    monkeypatch.setattr(b,'create',partial)
    with pytest.raises(OSError):create(a)
    scope=next(iter(a.records))
    if ambiguity=='populated':b.groups[scope]['populated']=True
    elif ambiguity=='launched':a.records[scope]['launched_unix']=1
    else:a.records[scope]['container_tickets']=['d'*64]
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['active_scopes']==1
    assert scope in b.groups and not b.stopped
    assert not a.records[scope].get('released_unix')


def test_retired_scope_only_allows_upgrade_while_empty_and_frozen(authority,monkeypatch):
    a,b=authority;request,record=create(a)
    container_ticket(a,record,monkeypatch)
    a.handle(os.getuid(),os.getpid(),auth(request,record,'stop'))
    a.handle(os.getuid(),os.getpid(),auth(request,record,'release'))
    assert a.handle(0,os.getpid(),{'op':'maintenance_begin'})['active_scopes']==0
    b.groups[record['scope_id']]['populated']=True
    assert a.handle(0,os.getpid(),{'op':'maintenance_status'})['active_scopes']==1
    b.groups[record['scope_id']]['populated']=False;b.stopped.clear()
    assert a.handle(0,os.getpid(),{'op':'maintenance_status'})['active_scopes']==1


@pytest.mark.parametrize('population',[False,True])
def test_unknown_kernel_group_blocks_upgrade_without_adoption(authority,population):
    a,b=authority;b.groups['foreign.scope']={'populated':population,'budget':1}
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['active_scopes']==1 and status['health'] is False
    assert status['active_scope_ids']==['foreign.scope']
    assert not a.records and not b.stopped
    with pytest.raises(ValueError,match='not healthy'):
        a.handle(0,os.getpid(),{'op':'maintenance_end'})
    assert a.maintenance['draining'] is True


def test_monitor_health_failure_keeps_maintenance_gate_closed(authority):
    a,b=authority
    a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    a.health_check=lambda:False
    assert a.handle(0,os.getpid(),{'op':'maintenance_status'})['health'] is False
    with pytest.raises(ValueError,match='not healthy'):
        a.handle(0,os.getpid(),{'op':'maintenance_end'})
    assert json.loads(a.maintenance_path.read_text())['draining'] is True


def test_changed_installed_bytes_fail_health_and_do_not_reopen_gate(authority,tmp_path,monkeypatch):
    a,b=authority;path=tmp_path/'installed.py';path.write_bytes(b'original bytes')
    expected=hashlib.sha256(path.read_bytes()).hexdigest()
    a.installation_paths={'resource_broker.py':path}
    a.installed_sha256={'resource_broker.py':expected}
    monkeypatch.setitem(type(a)._maintenance_status.__globals__,'_trusted_file',lambda p:p)
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['health'] and status['installed_sha256']=={'resource_broker.py':expected}
    path.write_bytes(b'changed after startup')
    status=a.handle(0,os.getpid(),{'op':'maintenance_status'})
    assert status['health'] is False
    assert status['installed_sha256']['resource_broker.py']==expected
    with pytest.raises(ValueError,match='not healthy'):
        a.handle(0,os.getpid(),{'op':'maintenance_end'})
    assert a.maintenance['draining'] is True


def test_unreadable_kernel_inventory_never_reports_safe_upgrade(authority,monkeypatch):
    a,b=authority
    def unreadable():raise OSError('kernel inventory unavailable')
    monkeypatch.setattr(b,'inventory',unreadable)
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['active_scopes']>0 and status['health'] is False
    assert not b.stopped


def test_pending_scope_identity_change_blocks_admin_cleanup(authority):
    a,b=authority;_,record=create(a)
    stored=a.records[record['scope_id']]
    stored.update(pending=True,cgroup_identity=[1,2])
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['active_scopes']==1 and status['health'] is False
    assert record['scope_id'] in b.groups
    assert not stored.get('released_unix') and not b.stopped


def test_maintenance_status_is_bounded_without_undercounting_unknown_groups(authority):
    a,b=authority
    for index in range(1000):
        b.groups[f'unknown-{index:04d}.scope']={'budget':1,'populated':False}
    status=a.handle(0,os.getpid(),{'op':'maintenance_begin'})
    assert status['active_scopes']==1000 and status['health'] is False
    assert status['active_scopes_truncated'] and status['errors_truncated']
    assert len(json.dumps(status).encode())<65536


def test_creation_recovery_returns_same_authority_without_recreating(authority):
    a,b=authority;request,record=create(a)
    recovered=a.handle(os.getuid(),os.getpid(),{**request,'op':'recover_create'})
    assert recovered['token']==record['token']
    assert list(b.groups)==[record['scope_id']]
    assert not b.stopped
    with pytest.raises(ValueError,match='budget'):
        a.handle(os.getuid(),os.getpid(),{**request,'op':'recover_create','memory_max_bytes':1})


def test_absent_creation_recovery_fences_a_delayed_create_across_restart(authority):
    a,b=authority
    request={'op':'create','action_key':'a'*64,'nonce':'b'*32,'memory_max_bytes':64*1024**2}
    recovered=a.handle(os.getuid(),os.getpid(),{**request,'op':'recover_create'})
    assert recovered['missing'] is True and not b.groups
    restored=type(a)(a.state_dir,os.getuid(),b,max_memory_bytes=1024**3)
    with pytest.raises(ValueError,match='released'):
        restored.handle(os.getuid(),os.getpid(),request)
    assert not b.groups and not b.stopped


def test_creation_recovery_refuses_unknown_existing_group(authority):
    a,b=authority;request,record=create(a);a.records.clear()
    with pytest.raises(PermissionError,match='unknown kernel'):
        a.handle(os.getuid(),os.getpid(),{**request,'op':'recover_create'})
    assert not a.records and record['scope_id'] in b.groups and not b.stopped


def test_partial_creation_can_be_recovered_and_empty_setup_released(authority,monkeypatch):
    a,b=authority;original=b.create
    def partial(scope,budget):
        original(scope,budget)
        raise OSError('controller creation failed')
    monkeypatch.setattr(b,'create',partial)
    with pytest.raises(OSError):create(a)
    request={'op':'recover_create','action_key':'a'*64,'nonce':'b'*32,'memory_max_bytes':64*1024**2}
    record=a.handle(os.getuid(),os.getpid(),request)
    assert record['pending'] is True
    a.handle(os.getuid(),os.getpid(),{**request,'op':'stop','token':record['token']})
    assert a.handle(os.getuid(),os.getpid(),{**request,'op':'release','token':record['token']})['released']
    assert not b.groups and not b.stopped


@pytest.mark.parametrize('version',[0,2,True,'1'])
def test_unsupported_creation_protocol_never_creates_authority(authority,version):
    a,b=authority
    with pytest.raises(ValueError,match='protocol'):
        a.handle(os.getuid(),os.getpid(),{'op':'create','action_key':'a'*64,'nonce':'b'*32,
                 'memory_max_bytes':64*1024**2,'recovery_protocol':version})
    assert not a.records and not b.groups


def test_local_oom_retries_transient_fresh_identity_check_failure(event_monitored, monkeypatch):
    import errno
    a, b, rows, gpu, monitor, events, clock = event_monitored
    monitor.poll_once()
    _, record, path = rows[0]
    scope = record['scope_id']
    (path / 'memory.events.local').write_text('oom 1\n')
    events.dirty.add(scope)
    original_read = monitor._read_scope_events
    path_type = type(path)
    original_stat = path_type.stat
    armed = False
    failures = 0

    def observe_then_arm(name, observed_record):
        nonlocal armed
        snapshot = original_read(name, observed_record)
        if name == scope:
            armed = True
        return snapshot

    def fail_fresh_identity_stat(candidate, *args, **kwargs):
        nonlocal armed, failures
        if candidate == path and armed:
            armed = False
            failures += 1
            raise OSError(errno.EIO, 'injected fresh stop identity failure')
        return original_stat(candidate, *args, **kwargs)

    # The event snapshot and its before/after inode checks succeed first.
    # Only the later fresh check in _stop fails, before pending intent is set.
    with monkeypatch.context() as patch:
        patch.setattr(monitor, '_read_scope_events', observe_then_arm)
        patch.setattr(path_type, 'stat', fail_fresh_identity_stat)
        failed = monitor.poll_events_once()
    assert failures == 1
    assert failed['errors']
    assert b.stopped == []
    assert not a.records[scope].get('monitor_stop_pending')

    # No further file modification or event. The transient failure is gone.
    clock.advance(max(monitor.recovery_interval_s, monitor.stop_retry_interval_s))
    retried = monitor.poll_once()
    assert retried['stopped'] == [scope], (
        'observed local OOM must retry after a transient fresh identity check failure',
        retried,
    )
    assert b.stopped == [scope]
