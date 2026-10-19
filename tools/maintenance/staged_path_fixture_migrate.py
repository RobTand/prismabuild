"""Migrate equal-fact positive fixture pairs; preserve deliberate raw fault inputs.

Run from the checkout root. --check is a read-only AST idempotence audit.
Every binding and optional epoch must agree before a pair can be composed.
"""
import argparse
import os
import ast
import subprocess
from pathlib import Path
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", type=Path, default=Path.cwd())
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
os.chdir(args.root)
remaining = 0
wanted={
 'ram_promotion_reservation_acceptance.py':{'run'},
 'test_a_consumer_stages_only_to_its_refill_horizon.py':{'_land'},
 'test_a_divergent_ram_name_is_arbitrated_by_its_owners.py':{'_foreign_owner'},
 'test_a_failed_release_names_its_step.py':{'world'},
 'test_a_ram_promotion_holds_its_tokens_until_the_egress_frees_them.py':{'_publish_stage'},
 'test_a_shared_staged_path_has_two_owners.py':{'test_a_split_range_promotes_from_its_staged_shard_path'},
 'test_a_stage_proof_runs_outside_the_ownership_lock.py':{'vouch'},
 'test_an_egress_holds_the_stage_lock_only_for_its_act.py':{'_land_range'},
 'test_an_unreadable_tier_announcement_is_not_an_epoch_move.py':{'held'},
 'test_canary_leg3_staged_read.py':{'publish'},
 'test_egress_reclaim_takes_one_stage_root_at_a_time.py':{'_publish'},
 'test_pbrun_residency_stage_submission.py':{'_resident_donor'},
 'test_prelaunch_shared_pinned_begin_1690.py':{'_land_range','_finish_claimed'},
 'test_publication_refusal_stops_the_range.py':{'_write_owner'},
 'test_the_ram_window_promotes_only_to_its_refill_horizon.py':{'_ram_range'},
 'test_uncharged_dead_owners_give_room_under_pressure.py':{'dead_owner'},
 'test_a_source_mark_copy_with_a_proven_original_is_reclaimed.py':{'test_a_pin_owner_never_moves'},
 'test_a_stale_donor_declines_and_current_material_is_reused.py':{'_publish_donor_record'},
 'test_acquire_reuses_unchanged_cover_documents.py':{'_publish'},
 'test_pb823_same_generation_republish.py':{'_republish'},
 'test_pb893_cover_lookup_reuses_unchanged_documents.py':{'_publish'},
 'test_ram_shared_source.py':{'_refile'},
 'test_reader_lease_lifetime.py':{'test_cover_proves_full_window_across_movers',
     'test_open_revalidates_ram_epoch','test_resolve_window_covers_from_published_material',
     'test_distinct_same_shape_files_never_share_a_pin','test_covers_for_keys_both_tiers_minimal',
     'test_contradictory_covers_refuse'},
}
def staged_fixture_apply(path, source, tree, edits):
    """Apply source edits without losing literal guards or duplicate occurrences."""
    changes = list({(a, b): text for a, b, text in edits}.items())
    if not changes:
        return 0
    lines = source.splitlines(True)
    for (a, b), text in sorted(changes, reverse=True):
        lines[a:b] = [text]
    new = ''.join(lines)
    if not any(isinstance(n, ast.ImportFrom) and n.module == 'prismabuild'
               and any(a.name == 'staged_path' for a in n.names) for n in tree.body):
        at = next(n.lineno - 1 for n in tree.body if isinstance(n, ast.ImportFrom)
                  and (n.module or '').startswith('prismabuild'))
        lines = new.splitlines(True)
        lines.insert(at, 'from prismabuild import staged_path\n')
        new = ''.join(lines)
    ast.parse(new)
    if not args.check:
        path.write_text(new)
    print(str(path), len(changes))
    return len(changes)


for filename,names in wanted.items():
 p=Path('tests')/filename;s=p.read_text();t=ast.parse(s);lines=s.splitlines(True);edits=[]
 for f in ast.walk(t):
  if not isinstance(f,ast.FunctionDef) or f.name not in names:continue
  for block in ast.walk(f):
   body=getattr(block,'body',None)
   if not isinstance(body,list):continue
   for first,second in [(a,b) for i,a in enumerate(body) for b in body[i+1:]
                         if isinstance(a,ast.Expr) and isinstance(b,ast.Expr)]:
    if not all(isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) for n in (first,second)):continue
    calls={ast.unparse(n.value.func):n.value for n in (first,second)}
    if not {'residency_map.write_fragment','reader_lease.write_material'} <= calls.keys():continue
    frag=calls['residency_map.write_fragment'];mat=calls['reader_lease.write_material'];kw={k.arg:k.value for k in mat.keywords}
    if ast.dump(frag.args[0]) != ast.dump(mat.args[0]):continue
    if not isinstance(frag.args[1],ast.Dict):continue
    facts={k.value:v for k,v in zip(frag.args[1].keys,frag.args[1].values) if isinstance(k,ast.Constant)}
    same=all(ast.dump(facts[k])==ast.dump(kw[k]) for k in ('consumer_action_key','mover_action_key','tier_id','stage_root','manifest_sha256'))
    if not same:continue
    epoch=kw.get('epoch',ast.Constant(None));fragment_epoch=facts.get('epoch',ast.Constant(None))
    if ast.dump(epoch)!=ast.dump(fragment_epoch):continue
    pad=' '*first.col_offset
    new=pad+'staged_path.publish_vouch(\n'+pad+'    '+ast.get_source_segment(s,frag.args[0])+', '+ast.get_source_segment(s,frag.args[1])+',\n'+pad+'    generation='+ast.get_source_segment(s,kw['generation'])+',\n'+pad+'    material_entries='+ast.get_source_segment(s,kw['entries'])+')\n'
    edits.append((first.lineno-1,first.end_lineno,''))
    edits.append((second.lineno-1,second.end_lineno,new))
 remaining += staged_fixture_apply(p, s, t, edits)

# An adjacent pair-only withdrawal has the same exact path, missing-file and
# OSError behavior as queue=None. Partial or reordered fault inputs stay raw.
for filename in subprocess.check_output(['git', 'ls-files', 'tests/*.py'], text=True).splitlines():
    if filename.startswith('tests/compat/'):
        continue
    p = Path(filename);s = p.read_text();t = ast.parse(s);edits = []
    for block in ast.walk(t):
        body = getattr(block, 'body', None)
        if not isinstance(body, list):
            continue
        for first, second in zip(body, body[1:]):
            if not all(isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
                       and isinstance(n.value.func, ast.Attribute) and n.value.func.attr == 'unlink'
                       and isinstance(n.value.func.value, ast.Call) for n in (first, second)):
                continue
            fragment, material = first.value.func.value, second.value.func.value
            if (ast.unparse(fragment.func) != 'residency_map.fragment_path'
                    or ast.unparse(material.func) != 'reader_lease.material_path'
                    or len(fragment.args) != 3 or fragment.keywords or material.keywords
                    or [ast.dump(a) for a in fragment.args] != [ast.dump(a) for a in material.args]):
                continue
            fk = {k.arg: k.value for k in first.value.keywords}
            mk = {k.arg: k.value for k in second.value.keywords}
            if set(fk) - {'missing_ok'} or set(mk) - {'missing_ok'}:
                continue
            missing = fk.get('missing_ok', ast.Constant(False))
            if ast.dump(missing) != ast.dump(mk.get('missing_ok', ast.Constant(False))):
                continue
            pad = ' ' * first.col_offset
            new = (pad + 'staged_path.withdraw_vouch(None, ' + ', '.join(ast.unparse(a) for a in fragment.args)
                   + ', missing_ok=' + ast.unparse(missing) + ')\n')
            edits.append((first.lineno - 1, second.end_lineno, new))
    remaining += staged_fixture_apply(p, s, t, edits)

if args.check and remaining:raise SystemExit(f"{remaining} positive pairs still need migration")
