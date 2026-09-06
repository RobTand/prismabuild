#!/usr/bin/python3
"""Local resource authority: contain PB payloads and Docker children together.

Runs as root behind a local Unix socket. Client commands never execute as root.
Only the configured local UID may create bounded scopes, launch user processes through a fixed privilege-dropping helper, or stop/release an exact token-authenticated attempt.
"""
from __future__ import annotations
import argparse
import array
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import pwd
import re
import secrets
import select
import socket
import socketserver
import stat
import struct
import subprocess
import sys
import threading
import time

HEX64=re.compile(r'[0-9a-f]{64}\Z');HEX32=re.compile(r'[0-9a-f]{32}\Z')

def scope_id(key, nonce):
    return 'prismabuild-job'+hashlib.sha256((key+nonce).encode()).hexdigest()[:32]+'.slice'

def _atomic(path, value, *, mode=0o600):
    temp=path.with_name('.'+path.name+'.'+secrets.token_hex(8))
    try:
        fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        os.fchmod(fd,mode)
        with os.fdopen(fd,'w') as out:
            json.dump(value,out,sort_keys=True);out.write('\n');out.flush();os.fsync(out.fileno())
        os.replace(temp,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:temp.unlink(missing_ok=True)


def _trusted_file(path):
    path=Path(path).resolve(strict=True)
    info=path.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022
            or any(q.stat().st_uid!=0 or q.stat().st_mode&0o022 for q in path.parents)):
        raise ValueError('privileged module must be root-owned and immutable to callers')
    return path


def _load_gpu_module():
    path=_trusted_file(Path(__file__).resolve().with_name('gpu_memory.py'))
    name='_prismabuild_privileged_gpu_memory'
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    sys.modules[name]=module
    spec.loader.exec_module(module)
    return module

class SystemdBackend:
    def __init__(self, root=Path('/sys/fs/cgroup')):self.root=Path(root)
    def path(self, scope):
        if not re.fullmatch(r'prismabuild-job[0-9a-f]{32}\.slice',scope):raise ValueError('invalid scope')
        return self.root/'prismabuild.slice'/scope
    def command(self,*argv):
        r=subprocess.run(['/usr/bin/systemctl',*argv],capture_output=True,text=True,timeout=15)
        if r.returncode:raise ValueError('systemd resource operation failed: '+r.stderr.strip()[:500])
    def create(self, scope, budget):
        # A generated name already containing processes is not ours to adopt.
        existing=self.path(scope)
        if existing.exists() and 'populated 1' in (existing/'cgroup.events').read_text():
            raise ValueError('refusing to adopt a populated pre-existing scope')
        # No process enters until the complete aggregate envelope is installed.
        self.command('start',scope)
        self.command('set-property','--runtime',scope,
                     f'MemoryMax={budget}',f'MemoryHigh={max(1,budget*9//10)}',
                     'MemorySwapMax=0','CPUAccounting=yes','CPUWeight=100','MemoryAccounting=yes')
        group=self.path(scope)
        # Older supported systemd releases lack a MemoryOOMGroup property.
        # Install and verify the kernel contract directly before any launch.
        (group/'memory.oom.group').write_text('1')
        if int((group/'memory.max').read_text())!=budget or (group/'memory.oom.group').read_text().strip()!='1':
            raise ValueError('kernel memory envelope differs from requested scope')
        available=set((group/'cgroup.controllers').read_text().split())
        if not {'cpu','memory'}<=available:raise ValueError('CPU/memory controllers unavailable')
        (group/'cgroup.subtree_control').write_text('+cpu +memory')
        leaf=group/'payload';leaf.mkdir(exist_ok=True)
        info=group.stat()
        events=dict(line.split() for line in (group/'memory.events').read_text().splitlines())
        return {'cgroup_path':str(group),'leaf_path':str(leaf),
                'cgroup_identity':[info.st_dev,info.st_ino],
                'memory_oom_kill_baseline':int(events.get('oom_kill',0))}
    def run(self, scope, uid, command, stdio):
        helper=_trusted_file(Path(__file__).resolve().with_name('resource_payload.py'))
        read_fd=os.memfd_create('pb-user-command',os.MFD_CLOEXEC)
        write_fd=-1;ready_read,ready_write=os.pipe()
        process=None
        try:
            # A bounded seekable command avoids pipe-capacity deadlock while
            # privileged startup or NSS is stalled. Consumed only after UID drop.
            data=json.dumps(command).encode()
            os.write(read_fd,data);os.lseek(read_fd,0,os.SEEK_SET)
            process=subprocess.Popen(['/usr/bin/python3','-I',str(helper),
                '--leaf',str(self.path(scope)/'payload'),'--uid',str(uid),
                '--command-fd',str(read_fd),'--ready-fd',str(ready_write)],
                stdin=stdio[0],stdout=stdio[1],stderr=stdio[2],cwd='/',
                env={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8'},
                pass_fds=(read_fd,ready_write),start_new_session=True)
            os.close(read_fd);read_fd=-1
            os.close(ready_write);ready_write=-1
            if not select.select([ready_read],[],[],10)[0] or os.read(ready_read,1)!=b'1':
                raise ValueError('payload did not confirm containment and identity drop')
            return process
        except BaseException:
            if process is not None:
                self.stop(scope)
                process.kill();process.wait(timeout=10)
            raise
        finally:
            for fd in (read_fd,write_fd,ready_read,ready_write):
                if fd>=0:os.close(fd)
    def stop(self, scope):
        group=self.path(scope)
        if group.exists():
            (group/'cgroup.freeze').write_text('1')
            (group/'cgroup.kill').write_text('1')
    def empty(self, scope):
        group=self.path(scope)
        return not group.exists() or 'populated 1' not in (group/'cgroup.events').read_text()
    def exists(self, scope):
        try:self.path(scope).stat()
        except FileNotFoundError:return False
        return True
    def healthy(self):
        return {'cpu','memory'}<=set((self.root/'cgroup.controllers').read_text().split())
    def inventory(self):
        parent=self.root/'prismabuild.slice'
        try:entries=list(parent.iterdir())
        except FileNotFoundError:return {}
        result={}
        for path in entries:
            info=path.lstat()
            if not stat.S_ISDIR(info.st_mode):continue
            events=dict(line.split() for line in (path/'cgroup.events').read_text().splitlines())
            populated=events.get('populated')
            if populated not in {'0','1'}:raise ValueError('invalid cgroup population evidence')
            result[path.name]={'populated':populated=='1',
                'frozen':(path/'cgroup.freeze').read_text().strip()=='1',
                'identity':[info.st_dev,info.st_ino]}
        return result
    def release(self, scope):
        group=self.path(scope)
        if group.exists():
            if 'populated 1' in (group/'cgroup.events').read_text():raise ValueError('scope still populated')
            leaf=group/'payload'
            if leaf.exists():leaf.rmdir()
        self.command('stop',scope)

class Authority:
    def __init__(self,state_dir,uid,backend,*,max_memory_bytes):
        self.state_dir=Path(state_dir);self.uid=int(uid);self.backend=backend
        self.max_memory_bytes=int(max_memory_bytes);self.lock=threading.RLock();self.records={}
        self.maintenance_path=self.state_dir.parent/'maintenance.json'
        self.maintenance={'schema':'prismabuild.resource-maintenance.v1','draining':False}
        self.installed_sha256={};self.installation_paths={};self.health_check=lambda:True
        self.state_dir.mkdir(parents=True,exist_ok=True,mode=0o700)
        info=self.state_dir.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077:
            raise ValueError('state directory must be private and owned by broker')
        for path in self.state_dir.glob('*.json'):
            st=path.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.geteuid() or st.st_mode&0o077:
                raise ValueError('unsafe stored resource authority')
            record=json.loads(path.read_text())
            key,nonce=self.identity(record)
            if path.stem!=scope_id(key,nonce) or record.get('scope_id')!=path.stem:
                raise ValueError('stored resource scope identity mismatch')
            if record.get('uid')!=self.uid or not HEX64.fullmatch(str(record.get('token',''))):
                raise ValueError('invalid stored resource owner')
            self.records[path.stem]=record
        try:info=self.maintenance_path.lstat()
        except FileNotFoundError:pass
        else:
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022:
                raise ValueError('unsafe maintenance gate')
            value=json.loads(self.maintenance_path.read_text())
            if (not isinstance(value,dict) or value.get('schema')!='prismabuild.resource-maintenance.v1'
                    or type(value.get('draining')) is not bool):raise ValueError('invalid maintenance gate')
            self.maintenance=value
    @staticmethod
    def identity(request):
        key=request.get('action_key');nonce=request.get('nonce')
        if not isinstance(key,str) or not HEX64.fullmatch(key):raise ValueError('invalid action key')
        if not isinstance(nonce,str) or not HEX32.fullmatch(nonce):raise ValueError('invalid attempt nonce')
        return key,nonce
    def handle(self,uid,pid,request):
        if not isinstance(request,dict):raise ValueError('request must be an object')
        op=request.get('op')
        if not isinstance(op,str):raise ValueError('operation must be a string')
        if op in {'maintenance_begin','maintenance_status','maintenance_end'}:
            return self.admin(uid,request)
        if uid!=self.uid:raise PermissionError('caller UID is not authorized')
        if op in {'container_begin','container_end'}:return self.container(uid,pid,request)
        key,nonce=self.identity(request);scope=scope_id(key,nonce)
        allowed={'op','action_key','nonce','memory_max_bytes'} if op=='create' else {'op','action_key','nonce','token','reason','memory_max_bytes'}
        if set(request)-allowed:raise ValueError('unknown request field')
        with self.lock:
            if op=='create':
                budget=request.get('memory_max_bytes')
                if type(budget) is not int or not 0<budget<=self.max_memory_bytes:raise ValueError('memory budget outside host bounds')
                existing=self.records.get(scope)
                if self.maintenance['draining'] and (existing is None or existing.get('pending')):
                    return {'ok':False,'maintenance':True,'retryable':True,
                            'error':'resource broker is draining for maintenance'}
                if existing:
                    if existing['memory_max_bytes']!=budget:raise ValueError('attempt budget cannot change')
                    if existing.get('released_unix'):raise ValueError('attempt is released')
                    if existing.get('stopped_unix'):raise ValueError('attempt is stopped')
                    if existing.get('pending'):
                        existing.update(self.backend.create(scope,budget));existing.pop('pending',None)
                        _atomic(self.state_dir/(scope+'.json'),existing)
                    return {'ok':True,**existing}
                record={'uid':uid,'action_key':key,'nonce':nonce,'scope_id':scope,
                        'memory_max_bytes':budget,'token':secrets.token_hex(32),'created_unix':time.time()}
                # Persist authority before OS creation so a broker restart can
                # retain exact recovery ownership even after partial setup.
                path=self.state_dir/(scope+'.json');record['pending']=True
                _atomic(path,record);self.records[scope]=record
                try:record.update(self.backend.create(scope,budget))
                except Exception:
                    # No payload was attached. Retain pending evidence; do not
                    # silently treat a partially configured scope as runnable.
                    raise
                record.pop('pending',None);_atomic(path,record)
                return {'ok':True,**record}
            if op not in {'stop','release','status'}:raise ValueError('unknown operation')
            record=self.records.get(scope)
            token=request.get('token')
            if not isinstance(token,str) or HEX64.fullmatch(token) is None:raise PermissionError('invalid attempt token')
            if record is None and not self.backend.exists(scope):
                # Reboot loses /run authority and kernel groups together. Only
                # absence is sufficient; never operate on an unknown group.
                return {'ok':True,'scope_id':scope,'released':True}
            if record is None or not hmac.compare_digest(token,record['token']):
                raise PermissionError('attempt authority does not match')
            if record.get('released_unix'):
                # Recovery after a worker crash may repeat cleanup. Retain
                # authority, but never touch a subsequently recreated group.
                return {'ok':True,'scope_id':scope,'released':True,**self._stop_details(record)}
            if record.get('pending'):raise ValueError('scope setup incomplete')
            if op=='stop':
                reason=str(request.get('reason','requested'))[:1000]
                if record.get('stopped_unix'):record['last_cleanup_reason']=reason
                else:record['stopped_unix']=time.time();record['stop_reason']=reason
                _atomic(self.state_dir/(scope+'.json'),record)
                self.backend.stop(scope)
            elif op=='release':
                if record.get('container_tickets'):
                    if not record.get('stopped_unix'):
                        raise ValueError('scope not stopped')
                    # Persisted stop intent survives a failed freeze. Reassert
                    # the kernel operation before treating emptiness as safe.
                    self.backend.stop(scope)
                    if not self.backend.empty(scope):
                        raise ValueError('scope still populated or not stopped')
                    # A killed client cannot prove a daemon RPC completed.
                    # Keep the empty frozen parent: a late container cannot
                    # reactivate an unfrozen replacement after admission ends.
                    record['retired_unix']=time.time()
                    _atomic(self.state_dir/(scope+'.json'),record)
                    return {'ok':True,'scope_id':scope,'retired':True}
                self.backend.release(scope)
                record['released_unix']=time.time()
                _atomic(self.state_dir/(scope+'.json'),record)
            return {'ok':True,'scope_id':scope,**self._stop_details(record)}

    @staticmethod
    def _stop_details(record):
        return {key:record[key] for key in ('stop_reason','stopped_unix',
                'termination_evidence','last_cleanup_reason') if key in record}

    def admin(self,uid,request):
        if uid!=0:raise PermissionError('maintenance requires root')
        op=request['op']
        allowed={'op','reason'} if op=='maintenance_begin' else {'op'}
        if set(request)-allowed or ('reason' in request and not isinstance(request['reason'],str)):
            raise ValueError('invalid maintenance fields')
        with self.lock:
            if op=='maintenance_begin':
                value={'schema':'prismabuild.resource-maintenance.v1','draining':True,
                       'changed_unix':time.time(),'reason':request.get('reason','automatic upgrade')[:1000]}
                _atomic(self.maintenance_path,value,mode=0o644);self.maintenance=value
            status=self._maintenance_status()
            if op=='maintenance_end':
                if not status['health']:raise ValueError('resource broker is not healthy; maintenance remains active')
                value={'schema':'prismabuild.resource-maintenance.v1','draining':False,'changed_unix':time.time()}
                _atomic(self.maintenance_path,value,mode=0o644);self.maintenance=value
                status['draining']=False
            return status

    def _maintenance_status(self):
        errors=[];active=set();inventory={}
        try:
            if not self.backend.healthy():errors.append('kernel resource controllers unavailable')
            inventory=self.backend.inventory()
        except (OSError,ValueError) as exc:errors.append(str(exc)[:1500]);active.add('unreadable kernel inventory')
        for scope,record in self.records.items():
            kernel=inventory.get(scope)
            if (kernel is not None and record.get('cgroup_identity')
                    and record['cgroup_identity']!=kernel.get('identity')):
                active.add(scope);errors.append('kernel scope identity changed: '+scope)
                continue
            if record.get('released_unix'):
                if kernel is not None:
                    active.add(scope);errors.append('released scope has an unknown kernel group: '+scope)
                continue
            # A failed create has no launcher or Docker RPC. Retire only exact
            # owned empty setup remnants, without signalling any processes.
            if (self.maintenance['draining'] and record.get('pending')
                    and not record.get('launched_unix') and not record.get('container_tickets')
                    and not errors and (kernel is None or not kernel['populated'])):
                try:
                    if kernel is None:
                        if self.backend.exists(scope):raise ValueError('pending scope appeared during inventory')
                    else:
                        if not self.backend.empty(scope):raise ValueError('pending scope became populated')
                        self.backend.release(scope)
                    record['released_unix']=time.time();record['maintenance_cleanup']='unlaunched empty setup'
                    _atomic(self.state_dir/(scope+'.json'),record)
                    inventory.pop(scope,None)
                    continue
                except (OSError,ValueError) as exc:errors.append(str(exc)[:1500])
            if record.get('retired_unix') and kernel is not None and not kernel['populated'] and kernel['frozen']:
                if not record.get('cgroup_identity') or record['cgroup_identity']==kernel.get('identity'):
                    continue
            active.add(scope)
        for scope in inventory:
            if scope not in self.records:
                active.add(scope);errors.append('unknown broker namespace group: '+scope)
        try:
            for name,path in self.installation_paths.items():
                if hashlib.sha256(_trusted_file(path).read_bytes()).hexdigest()!=self.installed_sha256[name]:
                    errors.append('installed privileged file changed: '+name)
            if not self.health_check():errors.append('resource monitor is not healthy')
        except (OSError,ValueError,KeyError) as exc:errors.append(str(exc)[:1500])
        return {'ok':True,'draining':self.maintenance['draining'],'active_scopes':len(active),
                'active_scope_ids':sorted(active)[:128],'active_scopes_truncated':len(active)>128,
                'health':not errors,'errors':[error[:500] for error in errors[:32]],
                'errors_truncated':len(errors)>32,
                'installed_sha256':dict(self.installed_sha256)}

    def run(self,uid,pid,request,stdio):
        allowed={'op','action_key','nonce','token','argv','cwd','env','affinity','umask'}
        if not isinstance(request,dict) or set(request)-allowed:raise ValueError('unknown run field')
        command={k:request.get(k) for k in ('argv','cwd','env')}
        if 'umask' in request:
            mask=request['umask']
            if type(mask) is not int or not 0<=mask<=0o777:
                raise ValueError('invalid inherited umask')
            command['umask']=mask
        if 'affinity' in request:
            mask=request['affinity']
            if (not isinstance(mask,list) or not mask or len(mask)!=len(set(mask))
                    or any(type(x) is not int or x<0 for x in mask)
                    or not set(mask)<=os.sched_getaffinity(0)):
                raise ValueError('invalid inherited CPU affinity')
            command['affinity']=mask
        argv,cwd,env=command['argv'],command['cwd'],command['env']
        if (not isinstance(argv,list) or not argv or len(argv)>4096
                or any(not isinstance(x,str) or '\x00' in x for x in argv)
                or not isinstance(cwd,str) or not cwd.startswith('/') or '\x00' in cwd
                or not isinstance(env,dict) or any(not isinstance(k,str) or not k or '=' in k
                    or '\x00' in k or not isinstance(v,str) or '\x00' in v for k,v in env.items())
                or len(json.dumps(command).encode())>49152 or len(stdio)!=3):
            raise ValueError('invalid bounded command or stdio descriptors')
        with self.lock:
            identity={k:request[k] for k in ('action_key','nonce','token')}
            if self.handle(uid,pid,{'op':'status',**identity}).get('released'):
                raise ValueError('attempt is released')
            record=self.records[scope_id(identity['action_key'],identity['nonce'])]
            if record.get('released_unix'):raise ValueError('attempt is released')
            if record.get('stopped_unix'):raise ValueError('attempt is stopped')
            if record.get('launched_unix'):raise ValueError('attempt already launched')
            # Persist launch intent before spawning; ambiguous restart refuses
            # a duplicate, retaining the exact scope for stop/recovery.
            record['launched_unix']=time.time()
            _atomic(self.state_dir/(record['scope_id']+'.json'),record)
            return self.backend.run(record['scope_id'],uid,command,stdio)

    def container(self,uid,pid,request):
        op=request['op'];scope=request.get('scope_id')
        fields={'op','scope_id'} if op=='container_begin' else {'op','scope_id','ticket'}
        if set(request)-fields or not isinstance(scope,str) or not re.fullmatch(r'prismabuild-job[0-9a-f]{32}\.slice',scope):
            raise ValueError('invalid container intent fields')
        with self.lock:
            record=self.records.get(scope)
            if record is None or record.get('uid')!=uid:raise PermissionError('unknown scope owner')
            if record.get('released_unix'):raise ValueError('attempt is released')
            if op=='container_begin':
                if record.get('stopped_unix'):raise ValueError('attempt is stopped')
                # This check grants only conservative intent registration, not
                # process migration or a kill capability. A vanished peer
                # cannot make another scope runnable or clear an intent.
                group=Path(f'/proc/{pid}/cgroup').read_text()
                if not any(scope in line.split('/') for line in group.splitlines()):
                    raise PermissionError('container creator is not in its scope')
                ticket=secrets.token_hex(32)
                record.setdefault('container_tickets',[]).append(ticket)
                _atomic(self.state_dir/(scope+'.json'),record)
                return {'ok':True,'ticket':ticket}
            ticket=request.get('ticket')
            if not isinstance(ticket,str) or HEX64.fullmatch(ticket) is None or ticket not in record.get('container_tickets',[]):
                raise PermissionError('container intent authority does not match')
            record['container_tickets'].remove(ticket)
            _atomic(self.state_dir/(scope+'.json'),record)
            return {'ok':True}

class ResourceMonitor:
    """Sample outside admission locks; stop only the same recorded kernel group."""
    identity_fields=('action_key','nonce','token','memory_max_bytes','cgroup_identity')

    def __init__(self,authority,gpu_module,*,interval_s=1.0,timeout_s=1.0):
        self.authority=authority;self.gpu=gpu_module;self.guard=gpu_module.Guard()
        self.interval_s=interval_s;self.timeout_s=timeout_s
        self.stopping=threading.Event();self.thread=None;self.last_success_monotonic=None

    def _records(self):
        with self.authority.lock:
            return {scope:{key:value for key,value in record.items() if key in {
                    *self.identity_fields,'memory_oom_kill_baseline','monitor_stop_pending',
                    'termination_evidence','stop_reason'}}
                for scope,record in self.authority.records.items()
                if not record.get('pending') and not record.get('released_unix')
                and (not record.get('stopped_unix') or record.get('monitor_stop_pending'))
                and isinstance(record.get('cgroup_identity'),list)
                and len(record['cgroup_identity'])==2}

    def _stop(self,scope,expected,identity,reason,evidence):
        with self.authority.lock:
            current=self.authority.records.get(scope)
            if (current is None or current.get('pending') or current.get('released_unix')
                    or (current.get('stopped_unix') and not current.get('monitor_stop_pending'))
                    or any(current.get(key)!=expected.get(key) for key in self.identity_fields)
                    or tuple(current['cgroup_identity'])!=tuple(identity)):
                return False
            info=self.authority.backend.path(scope).stat()
            if (info.st_dev,info.st_ino)!=tuple(identity):return False
            current.setdefault('stopped_unix',time.time())
            current.setdefault('stop_reason',reason)
            current.setdefault('termination_evidence',evidence)
            current['monitor_stop_pending']=True
            path=self.authority.state_dir/(scope+'.json')
            _atomic(path,current)
            self.authority.backend.stop(scope)
            current['monitor_stop_pending']=False
            _atomic(path,current)
            return True

    def poll_once(self):
        records=self._records();stopped=[];errors=[]
        # memory.events covers direct allocations and every Docker descendant.
        # Systemd can reset oom.group while creating Docker units, so finish an
        # OOM-affected attempt even when the kernel selected just one child.
        for scope,record in records.items():
            try:
                path=self.authority.backend.path(scope);info=path.stat()
                identity=(info.st_dev,info.st_ino)
                if identity!=tuple(record['cgroup_identity']):continue
                if record.get('monitor_stop_pending'):
                    if self._stop(scope,record,identity,record['stop_reason'],record['termination_evidence']):
                        stopped.append(scope)
                    continue
                events=dict(line.split() for line in (path/'memory.events').read_text().splitlines())
                after=path.stat()
                if (after.st_dev,after.st_ino)!=identity:continue
                count=int(events.get('oom_kill',0))
                baseline=int(record.get('memory_oom_kill_baseline',0))
                if count>baseline:
                    evidence={'source':'cgroup.memory.events','scope_id':scope,
                              'cgroup_identity':list(identity),'oom_kill':count,
                              'oom_kill_baseline':baseline,'sampled_unix':time.time()}
                    if self._stop(scope,record,identity,'memory_limit_oom',evidence):stopped.append(scope)
            except (OSError,ValueError,KeyError) as exc:errors.append({'scope_id':scope,'error':str(exc)[:1500]})
        scopes=[self.gpu.Scope(scope,self.authority.backend.path(scope),record['memory_max_bytes'])
                for scope,record in records.items() if scope not in stopped and not record.get('monitor_stop_pending')]
        snapshot=None
        if scopes:
            snapshot=self.gpu.collect(scopes,timeout_s=self.timeout_s)
            for decision in self.guard.observe(snapshot):
                expected=records.get(decision.scope_id)
                if expected is None:continue
                try:
                    if self._stop(decision.scope_id,expected,decision.cgroup_identity,
                                  decision.reason,decision.as_dict()):stopped.append(decision.scope_id)
                except (OSError,ValueError,KeyError) as exc:
                    errors.append({'scope_id':decision.scope_id,'error':str(exc)[:1500]})
        status={'sampled_unix':time.time(),'active_scopes':len(records),'stopped':stopped,
                'errors':errors,'gpu':snapshot.as_dict() if snapshot is not None else None}
        _atomic(self.authority.state_dir/'monitor.status',status)
        self.last_success_monotonic=time.monotonic()
        return status

    def healthy(self):
        return (self.thread is not None and self.thread.is_alive()
                and self.last_success_monotonic is not None
                and time.monotonic()-self.last_success_monotonic<15)

    def _loop(self):
        while not self.stopping.is_set():
            try:self.poll_once()
            except Exception as exc:
                print('resource monitor sample failed: '+str(exc)[:1500],flush=True)
            self.stopping.wait(self.interval_s)

    def start(self):
        self.thread=threading.Thread(target=self._loop,name='resource-monitor',daemon=True)
        self.thread.start()

    def close(self):
        self.stopping.set()
        if self.thread is not None:self.thread.join(timeout=self.timeout_s+self.interval_s+1)


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(10);descriptors=[];process=None;request=None
        pid,uid,_=struct.unpack('3i',self.request.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
        try:
            raw,ancillary,flags,_=self.request.recvmsg(65537,socket.CMSG_SPACE(16*array.array('i').itemsize))
            for level,kind,data in ancillary:
                if level==socket.SOL_SOCKET and kind==socket.SCM_RIGHTS:
                    fds=array.array('i');fds.frombytes(data[:len(data)-len(data)%fds.itemsize]);descriptors.extend(fds)
            while not raw.endswith(b'\n') and len(raw)<=65536:
                more=self.request.recv(65537-len(raw))
                if not more:break
                raw+=more
            if flags&socket.MSG_CTRUNC or len(raw)>65536 or not raw.endswith(b'\n'):
                raise ValueError('request exceeds framing limit')
            request=json.loads(raw)
            if isinstance(request,dict) and request.get('op')=='run':
                process=self.server.authority.run(uid,pid,request,descriptors)
                # Close our duplicates; only the launched child needs stdio.
                for fd in descriptors:os.close(fd)
                descriptors=[];self.request.settimeout(None)
                exit_fd=os.pidfd_open(process.pid)
                try:
                    while process.poll() is None:
                        ready=select.select([self.request,exit_fd],[],[],1)[0]
                        if self.request in ready:
                            extra=self.request.recv(1)
                            # Both EOF and unexpected in-flight protocol data
                            # end only this attempt; never spin on unread bytes.
                            self.server.authority.handle(uid,pid,{k:v for k,v in {
                                **request,'op':'stop','reason':'execution client disconnected or sent unexpected data'}.items()
                                if k in {'op','action_key','nonce','token','reason'}})
                            break
                    code=process.wait(timeout=15)
                finally:os.close(exit_fd)
                answer={'ok':True,'returncode':code}
            else:
                if descriptors:raise ValueError('descriptors only accepted for run')
                answer=self.server.authority.handle(uid,pid,request)
        except (ValueError,PermissionError,OSError,KeyError,TypeError,subprocess.SubprocessError) as exc:
            if process is not None and process.poll() is None and isinstance(request,dict):
                try:
                    self.server.authority.handle(uid,pid,{k:v for k,v in {
                        **request,'op':'stop','reason':'execution control connection failed'}.items()
                        if k in {'op','action_key','nonce','token','reason'}})
                    process.wait(timeout=15)
                except (OSError,ValueError,PermissionError,subprocess.SubprocessError):pass
            answer={'ok':False,'error':str(exc)[:1500]}
        finally:
            for fd in descriptors:os.close(fd)
        try:self.request.sendall((json.dumps(answer)+'\n').encode())
        except OSError:pass

class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads=True

def main():
    parser=argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--socket',default='/run/prismabuild/resources.sock')
    parser.add_argument('--state-dir',default='/run/prismabuild/jobs')
    parser.add_argument('--uid',type=int,default=1000)
    args=parser.parse_args()
    if os.geteuid()!=0:raise SystemExit('resource broker must run as root')
    total=next(int(x.split()[1])*1024 for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemTotal:'))
    authority=Authority(args.state_dir,args.uid,SystemdBackend(),max_memory_bytes=total)
    endpoint=Path(args.socket);endpoint.parent.mkdir(mode=0o755,parents=True,exist_ok=True)
    if endpoint.exists():
        # systemd owns one broker; refuse an active endpoint rather than take it.
        probe=socket.socket(socket.AF_UNIX)
        try:
            probe.connect(str(endpoint));raise SystemExit('resource broker socket already active')
        except ConnectionRefusedError:endpoint.unlink()
        finally:probe.close()
    with Server(str(endpoint),Handler) as server:
        server.authority=authority
        os.chown(endpoint,0,pwd.getpwuid(args.uid).pw_gid);endpoint.chmod(0o660)
        monitor=ResourceMonitor(authority,_load_gpu_module())
        for name in ('resource_broker.py','resource_payload.py','gpu_memory.py'):
            path=_trusted_file(Path(__file__).resolve().with_name(name))
            authority.installation_paths[name]=path
            authority.installed_sha256[name]=hashlib.sha256(path.read_bytes()).hexdigest()
        authority.health_check=monitor.healthy
        monitor.start()
        print('PrismaBuild resource broker ready',flush=True)
        try:server.serve_forever(poll_interval=0.5)
        finally:monitor.close()

if __name__=='__main__':main()
