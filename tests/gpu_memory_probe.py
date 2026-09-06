"""Bounded admitted CUDA accounting observation; no enforcement or host mutation."""
import json, os, pathlib, subprocess, time
import torch

def observe(label):
    cgroup=next(line.split(':',2)[2] for line in pathlib.Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    path=pathlib.Path('/sys/fs/cgroup')/cgroup.lstrip('/')
    def read(name):
        try:return (path/name).read_text().strip()
        except OSError:return None
    out=subprocess.run(['/usr/bin/nvidia-smi','--query-compute-apps=pid,gpu_uuid,used_memory','--format=csv,noheader,nounits'],capture_output=True,text=True,timeout=5)
    print(json.dumps({'label':label,'pid':os.getpid(),'cgroup':cgroup,'memory_current':read('memory.current'),'memory_stat':read('memory.stat'),'meminfo':pathlib.Path('/proc/meminfo').read_text(),'query_rc':out.returncode,'gpu_processes':out.stdout,'gpu_stderr':out.stderr,'cuda_allocated':torch.cuda.memory_allocated(),'cuda_reserved':torch.cuda.memory_reserved()}),flush=True)

torch.set_num_threads(1)
torch.cuda.init()
observe('context')
x=torch.ones(512*1024**2,dtype=torch.uint8,device='cuda');torch.cuda.synchronize()
time.sleep(1)
observe('512MiB_tensor')
del x;torch.cuda.empty_cache();torch.cuda.synchronize()
time.sleep(1)
observe('released')
