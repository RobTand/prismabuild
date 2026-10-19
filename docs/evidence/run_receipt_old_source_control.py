"""Run only inside PB: exact old-source negative through the existing recorder."""
from pathlib import Path
import runpy


root = Path.cwd()
files = ['tests/test_generation_receipt_source_contract.py']
owner = runpy.run_path(str(root / 'tools/fleet/pbtest_outcomes.py'))
raise SystemExit(owner['main'](
    ['-q', '-rA', '-k', 'selected_updater_cannot_establish or declaration_bytes_are_bound', *files],
    collection_spec={'files': files, 'roots': [[name, 'file'] for name in files]},
    collection_source=(root / 'tools/fleet/pbtest_collection.py').read_text()))
