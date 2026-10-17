"""Rebind exact current compatibility calls and record source occurrences."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import json
import sys
from pathlib import Path

from compose_compat_tier import Replacement, syntax_records, transformed

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests/compat/support"))
from rebind_source_reviews import review_source_hash


ENTRY_MOVES = {
    'pbrun:build_git_checkout_snapshot': 'prismabuild.submission:snapshot_checkout',
    'pbrun:residency_stage_rows': 'prismabuild.residency_plan:prepare_window',
    'pbrun:residency_stage_rows.<locals>.seal_stage_mover': 'prismabuild.residency_plan:prepare_window.<locals>.seal_stage_mover',
    'pbrun:residency_stage_rows.<locals>.seal_ram_chunk': 'prismabuild.residency_plan:prepare_window.<locals>.seal_ram_chunk',
    'stage_move:_LandingReport.write': 'prismabuild.stage_move:_LandingReport.write',
    'pbstatus:_transport': 'prismabuild.census:_transport',
    'prismabuild.local_scratch:_scratch_lifetime_selections': 'prismabuild.scratch_lifetime:selections',
    'prismabuild.local_scratch:build_scratch_lifetime_selection': 'prismabuild.scratch_lifetime:build_scratch_lifetime_selection',
    'prismabuild.local_scratch:_scratch_lifetime_record': 'prismabuild.scratch_lifetime:validate_record',
    'prismabuild.pool:PoolQueue._record_scratch_lifetimes': 'prismabuild.scratch_lifetime:register_attempt',
    'd38_gate:load_plan': 'prismabuild.d38_gate:load_plan',
    'd38_gate:_parse_receipt': 'prismabuild.d38_gate:_parse_receipt',
    'pbrun:parse_progress_phases': 'prismabuild.submission:parse_progress_phases',
    'pbrun:parse_awaited_batch': 'prismabuild.submission:parse_awaited_batch',
    'prismabuild.pool:PoolQueue._file_unreadable': 'prismabuild.lifecycle:_file_unreadable',
    'stage_move:move': 'prismabuild.stage_move:move',
    'ram_promote:promote': 'prismabuild.ram_promote:promote',
    'ram_promote:_refused': 'prismabuild.ram_promote:_refused',
    'pbrun:seal_action_from_template': 'prismabuild.submission:seal',
    'pbrun:build_stamp_closure': 'prismabuild.submission:build_stamp_closure',
    **{'stage_release:' + name: 'prismabuild.stage_release:' + name for name in (
        '_ledger_unreadable_receipt', '_receiptless_refusal', '_unresolved_reports',
        '_uncharged_owner_report', 'sweep_dead_owner_fragments', '_evict_uncharged_owner',
        '_retire_produced_orphan')},
}

RULES = (
    Replacement('compat_core_adapters.py', 'load("pbrun")', 'load("prismabuild.submission")', 3),
    Replacement('compat_builder_adapters.py', 'load("pbrun").seal_action_from_template(template)',
                'load("prismabuild.submission").seal(template)'),
    Replacement('compat_snapshot_adapters.py', 'pbrun.build_git_checkout_snapshot(work, cas=cas)',
                'load("prismabuild.submission").snapshot_checkout(work, cas=cas)'),
    Replacement('compat_recovery_adapters.py', '"prismabuild.pool", "pbrun"',
                '"prismabuild.pool", "prismabuild.submission"'),
    Replacement('compat_recovery_adapters.py', 'pbrun.build_git_checkout_snapshot', 'pbrun.snapshot_checkout'),
    Replacement('compat_input_adapters.py', 'load("d38_gate")', 'load("prismabuild.d38_gate")', 2),
    Replacement('compat_slurm_adapters.py', 'status = load("pbstatus")', 'status = load("prismabuild.census")'),
    Replacement('compat_publication_adapters.py', 'mover = load("stage_move")', 'mover = load("prismabuild.stage_move")'),
    Replacement('compat_lease_adapters.py', 'load("stage_release")', 'load("prismabuild.staged_path")'),
    Replacement('compat_state_adapters.py', 'load("stage_release")', 'load("prismabuild.staged_path")'),
    Replacement('compat_terminal_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_terminal_adapters.py', 'queue._file_unreadable(ready, reason="private syntax fixture")',
                'load("prismabuild.lifecycle")._file_unreadable(queue, ready, reason="private syntax fixture")'),
    Replacement('compat_terminal_adapters.py', '''def terminal_reader(load):
    frontend = load("pbrun")
    if hasattr(frontend, "terminal_record"):
        return frontend.terminal_record
    return load("prismabuild.action_wait").terminal_record''',
                '''def terminal_reader(load):
    return load("prismabuild.action_wait").terminal_record'''),
    Replacement('compat_movement_writer_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_movement_writer_adapters.py', '"stage_move", "prismabuild.reader_lease", "stage_release",',
                '"prismabuild.stage_move", "prismabuild.reader_lease", "prismabuild.staged_path",'),
    Replacement('compat_movement_writer_adapters.py', 'load("ram_promote")', 'load("prismabuild.ram_promote")'),
    Replacement('compat_egress_writer_adapters.py', '"prismabuild.pool", "stage_release", "prismabuild.residency_map"',
                '"prismabuild.pool", "prismabuild.stage_release", "prismabuild.residency_map"'),
    *(Replacement('compat_egress_writer_adapters.py', 'release.' + name,
                  'load("prismabuild.staged_path").' + name, count) for name, count in (
        ('_refused_receipt', 1), ('prune_stale_mentions', 1), ('recover_orphaned_range', 1),
        ('register_stage_root', 1), ('reconcile', 1), ('evict', 2))),
    Replacement('compat_scratch_adapters.py', 'scratch.build_scratch_lifetime_selection',
                'load("prismabuild.scratch_lifetime").build_scratch_lifetime_selection'),
    Replacement('compat_scratch_adapters.py', 'scratch._scratch_lifetime_selections',
                'load("prismabuild.scratch_lifetime").selections'),
    Replacement('compat_scratch_adapters.py', '    elif family == "scratch-lifetime":\n        if writing:',
                '    elif family == "scratch-lifetime":\n        lifetime = load("prismabuild.scratch_lifetime")\n        if writing:'),
    Replacement('compat_scratch_adapters.py', 'scratch._prepare_scratch_directory', 'lifetime._prepare_scratch_directory', 3),
    Replacement('compat_scratch_adapters.py', 'queue._record_scratch_lifetimes(record["action_key"],',
                'lifetime.register_attempt(queue, record["action_key"],', 2),
    Replacement('compat_scratch_adapters.py', 'scratch.SCRATCH_LIFETIME_FIELD', 'lifetime.SCRATCH_LIFETIME_FIELD'),
    Replacement('compat_scratch_adapters.py', 'scratch._scratch_lifetime_record', 'lifetime.validate_record'),
    Replacement('compat_residency_writer_adapters.py', 'queue.ensure_layout()', 'queue.records.ensure_layout()'),
    Replacement('compat_residency_writer_adapters.py', '        run = load("pbrun")\n        run.SH = store',
                '        run = load("prismabuild.submission")\n        rp = load("prismabuild.residency_plan")'),
    Replacement('compat_residency_writer_adapters.py', 'args = SimpleNamespace(priority=-10,',
                'args = load("prismabuild.submission_types").SubmissionRequest(cwd="/compat/checkout", detach=True, priority=-10,'),
    Replacement('compat_residency_writer_adapters.py', '        staged = run.residency_stage_rows(template, consumer_action_key=C, tier=tier, args=args, queue=queue, cas=cas)\n        rp = load("prismabuild.residency_plan")',
                '        publication = load("prismabuild.queue_publication").RowPublication(\n            queue.root, cas.root, store / "tools/prismabuild_worker.py", -10, 1, False)\n        options = run.filing_options(args)\n        staged = rp.prepare_window(template, consumer_action_key=C, tier=tier, options=options,\n            publication=publication, queue=queue, cas=cas)'),
    Replacement('compat_residency_writer_adapters.py', '            staged = run.residency_stage_rows(template, consumer_action_key=C, tier=tier, args=args, queue=queue, cas=cas)',
                '            staged = rp.prepare_window(template, consumer_action_key=C, tier=tier, options=options,\n                publication=publication, queue=queue, cas=cas)'),
    Replacement('compat_residency_writer_adapters.py', 'run.seal_action_from_template(template)', 'run.seal(template)'),
    Replacement('compat_residency_writer_adapters.py', '            report = load("pbcampaign").child_record(child, args=args, queue=queue, cas=cas, staged_plan=plan)',
                '            campaign = load("pbcampaign")\n            prior_store = campaign.store_for\n            private_store = load("prismabuild.submission_types").FleetStore(\n                store, store, store / "boxes.json", queue_root=queue.root, cas_root=cas.root)\n            campaign.store_for = lambda _: private_store\n            try:\n                report = campaign.child_record(child, args=args, queue=queue, cas=cas,\n                    staged_plan=plan, template=template)\n            finally:\n                campaign.store_for = prior_store'),

)


def definitions(raw):
    result = {}

    def visit(node, prefix=''):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            name = prefix + node.name
            result.setdefault(name, []).append(node)
            prefix = name + ('.' if isinstance(node, ast.ClassDef) else '.<locals>.')
        for child in ast.iter_child_nodes(node):
            visit(child, prefix)

    visit(ast.parse(raw))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    root = args.root.resolve()
    catalogue = root / 'src/prismabuild/record_kinds.py'
    before = catalogue.read_text()
    occurrences = []
    after = before
    for old, current in ENTRY_MOVES.items():
        module, name = current.split(':')
        source = root / ('src/' + module.replace('.', '/') + '.py')
        raw = source.read_bytes()
        targets = definitions(raw).get(name, [])
        if len(targets) != 1:
            raise ValueError(('current source owner is not unique', current))
        prior = after.count('"' + old + '"')
        present = after.count('"' + current + '"')
        if prior:
            if present:
                raise ValueError(('mixed catalogue preimage', old))
            after = after.replace('"' + old + '"', '"' + current + '"')
        elif not present:
            raise ValueError(('catalogue occurrence disappeared', old))
        occurrences.append(dict(before=old, current=current, occurrences=prior or present,
            source=str(source.relative_to(root)), source_sha256=review_source_hash(raw), line=targets[0].lineno,
            target_ast_sha256=review_source_hash(ast.dump(targets[0], include_attributes=False).encode())))
    normalized = ast.parse(after)
    reverse = {new: old for old, new in ENTRY_MOVES.items()}
    for node in ast.walk(normalized):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in reverse:
            node.value = reverse[node.value]
    if ast.dump(normalized) != ast.dump(ast.parse(before)) and after != before:
        raise ValueError('catalogue changed beyond exact source references')
    files = []
    replacements = {catalogue: after}
    for filename in dict.fromkeys(rule.file for rule in RULES):
        path = root / 'tests/compat/support' / filename
        rules = [rule for rule in RULES if rule.file == filename]
        text = path.read_text()
        updated = transformed(text, rules)
        if syntax_records(text, rules) != syntax_records(updated):
            raise ValueError(('original definitions/assertions/raises/decorators changed', filename))
        files.append(dict(path=str(path.relative_to(root)), before_sha256=review_source_hash(text.encode()),
            after_sha256=review_source_hash(updated.encode()), occurrences=[dict(before=r.before, after=r.after, count=r.count) for r in rules],
            definitions_assertions_raises_decorators_equal_after_exact_binding=True))
        replacements[path] = updated
    report = dict(catalogue_occurrences=occurrences, files=files, apply=args.apply)
    with args.report.open('x') as stream:
        json.dump(report, stream, indent=2)
        stream.write('\n')
    if args.apply:
        for path, text in replacements.items():
            path.write_text(text)
    print(json.dumps(dict(current_source_occurrences=sum(row['occurrences'] for row in occurrences),
                         current_driver_files=len(files), apply=args.apply)))


if __name__ == '__main__':
    main()
