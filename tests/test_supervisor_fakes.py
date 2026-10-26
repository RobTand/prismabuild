"""Fake process identities must distinguish missing, empty and foreign ownership."""
import pytest

from supervisor_fakes import write_process


@pytest.mark.parametrize(('options', 'ownership'), [
    ({}, b'PRISMABUILD_SUPERVISED_WORKER=dl380g10\0'),
    ({'mark': None}, b''),
    ({'mark': ''}, b'PRISMABUILD_SUPERVISED_WORKER=\0'),
    ({'mark': 'foreign'}, b'PRISMABUILD_SUPERVISED_WORKER=foreign\0'),
], ids=['default', 'absent', 'empty', 'foreign'])
def test_fake_process_preserves_ownership_bytes(tmp_path, options, ownership):
    pid = 1000000091
    write_process(tmp_path, pid, ['/python', '/gen/tier_loop.py'], **options)

    process = tmp_path / str(pid)
    assert (process / 'environ').read_bytes() == (
        b'HOME=/home/rob\0PATH=/usr/bin:/bin\0' + ownership)
