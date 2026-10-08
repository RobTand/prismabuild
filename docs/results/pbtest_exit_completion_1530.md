# pbtest exit completion evidence (#1530)

## Scope

The change separates selected-test completion from final pytest outcomes and process exit.
The green gate still requires exit zero, a terminal summary, and reconciled final outcomes.
This record covers source tests and a pool-scratch reproduction. It does not claim runtime deployment.

## Regression evidence

The actions used the published PrismaBuild client, the `x86` class, two CPUs, and 4 GiB of memory.
Each native thread limit was one. The regression suite used `/tmp`.

- Before the fix: `fb1e94005d5bda3cfd7ee3306d2288c37b0c80cadee9cd712ec0a9d5afa505ac`.
  All five regression cases failed because the report lacked test completion evidence.
  The action returned one. No CAS receipt exists for this failed action.
  The retained stdout digest is `9ca28b1730057931e9011ff4a872eb18fa977d8be912704bb73c1d5dbc5e6d8d`.
- After the fix: `1fd86733546d5cc92fcc0a7a03f19c96c7fbd8571118dfb0d16e86c8ce5372a5`.
  The affected suite passed 98 tests, with no skips, in 27.51 seconds.
  Its cases cover blocked controller hooks, blocked xdist worker hooks, early stop, and invalid completion records.
  They check console text, JSON fields, and the non-green verdict.
  They also check that progress dots and trace events cannot prove completion.

  The receipt digest is `de5d784725185fc07148052454b5dbc1b4e70ac8d47e1102fb6da13737dcd133`.
  The payload digest is `7dd52977c5fe5bc36a8944d61f58e75c776c0ab46fde2fb39b4208da05bec3b5`.
  A direct SHA-256 check matched the payload address.
- Extended fixture run: `31fbec2d8c3089653090a82a9fdf3b3f1496c3f29865c50fb0a6027e089b5c7c`.
  It returned one, with 99 passes and two fixture failures.
  The final-summary fixtures signaled their block before Python flushed the summary.
  The correction selects unbuffered output for the child fixture. It preserves all assertions.
- Final source run: `423103a1559a0363fa28a80bddeae3034869554272ed989ef8b591831259246b`.
  All 101 affected tests passed, with no skips, in 24.73 seconds.
  This run also covers timeouts after a final summary, in single-process and xdist shards.
  It proves that complete tests without final outcomes remain non-green, even with exit zero.

  The receipt digest is `1bdff29cba16fccf72c99e1ccb4c43d8e24085fdbed9dfadfd9e08c9d8605670`.
  The payload digest is `6e14a577d56af71a7f601deedca4d8b52879d59a8ecf105493c7ba84c91f75aa`.
  A direct SHA-256 check matched the payload address.

## Pool-scratch reproduction

Action: `1e32cf94bccacf4b54ff2e852f4e567eeb12a58775e0fe7ffc6dcbe2e2c40ab0`.
Host: `dl380g10`, with 80 logical CPUs.
Scratch: `/storage_pool/shared/pb-scratch`, on the ZFS dataset `storage_pool/shared`.
The mount options were `rw,noatime,xattr,noacl,casesensitive`.
The initial load averages were 22.54, 24.42, and 24.75.

The probe ran `tests/test_publish_runtime.py` and `tests/test_fleet_tool_flags_have_help.py` in one shard.
It tested one pytest process and then two xdist workers.
Both children reached `[100%]`, then failed to exit within the probe's 100-second child deadline.
Neither child printed a terminal summary or a final outcome record.
The probe captured Python stacks every 20 seconds. It then killed each child process group and collected its output.

Both controller stacks showed this exit path:

```text
shutil.py:778 or :791 in _rmtree_safe_fd_step
shutil.py:721 in _rmtree_safe_fd
shutil.py:852 in rmtree
_pytest/pathlib.py:167 in rm_rf
_pytest/pathlib.py:290 in maybe_delete_a_numbered_dir
_pytest/pathlib.py:338 in try_cleanup
_pytest/pathlib.py:367 in cleanup_numbered_dir
contextlib.py:627 in close
_pytest/tmpdir.py:340 in pytest_sessionfinish
_pytest/main.py:365 in wrap_session
```

The observed exit blocker was pytest's numbered temporary-directory cleanup, before the terminal summary hook.
The single-process load averages at termination were 21.07, 23.67, and 24.47.
The xdist load averages at termination were 23.35, 23.43, and 24.28.
The stacks identify the cleanup path. They do not prove a stuck kernel syscall, an NFS fault, or the October 5 cause.
The probe did not add synthetic filesystem delays or artificial pool load.

The diagnostic parent returned zero because it completed the probe and retained both child timeouts.
That parent receipt does not certify either child suite as green.
The receipt digest is `4bfec59f9f9dfeb61ba9df8b5a23709fc49025e69edada795669a19364ab78e5`.
The payload digest is `8aed90d0572fb288d6a0e795c32f18d4156115aada47b7facca790c673fe10db`.
A direct SHA-256 check matched the payload address.
The payload retains the mount data, host load, progress output, and all stack samples.
Its CAS path is `/mnt/shared/prismabuild-fleet/cas/blobs/8a/8aed90d0572fb288d6a0e795c32f18d4156115aada47b7facca790c673fe10db`.

## Integrated completion and scratch proof

Action: `995d3e88dbf1acd0113a7a27c03f24f860912401e5da6954568bee650a7e12cd`.
The probe used the fixed recorder on dl380g10, with the same two source files.
It alternated pool scratch and `/tmp` for each worker count.
The initial load averages were 19.45, 21.25, and 23.12.

| Scratch | Pytest workers | Child ending | Completed selected tests | Final outcomes |
| --- | ---: | --- | ---: | --- |
| ZFS pool | 1 | Timeout at the 100-second child deadline | 120 | Absent |
| `/tmp` tmpfs | 1 | Exit zero | 120 | 120 passed |
| ZFS pool | 2 | Exit zero | 120 | 120 passed |
| `/tmp` tmpfs | 2 | Exit zero | 120 | 120 passed |

The timed-out pool child flushed its completion record before the cleanup hook stalled.
It still had no terminal summary or final outcome record.
The other children produced reconciled final outcomes.
The probe asserted these distinctions and returned zero.
Load changed between arms. These diagnostic runs do not establish a controlled performance comparison.

The target interpreter also supplied the source at the sampled stack positions:

```python
# /usr/lib/python3.14/shutil.py
# line 778
entries = list(scandir_it)
# line 791
os.unlink(entry.name, dir_fd=topfd)
```

The receipt digest is `f33c12087178574b8eadc656bb8f50ac86a267c6c2f0fe9e0ffb2ec14f93ce74`.
The payload digest is `d12a54e02464339c4ec9f90b17b6e5e0477a44aaed6d8881cf48c1db8d417173`.
A direct SHA-256 check matched the payload address.
The payload retains all four child outputs and their structured completion checks.
The temporary probe source remains in the sealed snapshot, not in the final branch.

## Scratch choice and limits

Use `/tmp` for small CPU suites on dl380g10 that do not require disk semantics.
Select it with `pbtest --tag x86 --tmpdir /tmp` or `pbrun --tag x86 --env TMPDIR=/tmp`.
The October 5 observations in #1530 show the named small suites exit on `/tmp`.
The new affected suite also exits on `/tmp`.
These observations support a scratch choice, not a controlled estimate of filesystem performance.

The probe observed a RAM-backed tmpfs with 158120488960 bytes of capacity and 121960222720 bytes available.
Capacity and free space can change. TMPDIR does not reserve bytes or inodes.
Declare memory for all test processes and their peak tmpfs files together.
Check byte and inode headroom before a run. Keep total scratch demand below available capacity.

Do not use tmpfs for spool tests, disk durability tests, or tests that require a supported local disk.
Keep disk capacity, filesystem-specific, and ZFS/NFS tests on their required filesystem.
Do not change scratch semantics to obtain a green result for those tests.
A timeout remains non-green, even when the selected test population has verified completion evidence.
