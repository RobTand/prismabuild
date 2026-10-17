"""Concrete source locations for the modules selected by the isolated loader."""


def module_paths(module):
    relative = module.replace(".", "/")
    if module.startswith("prismabuild."):
        return ("src/" + relative + ".py",)
    paths = ("tools/fleet/" + relative + ".py", "tools/" + relative + ".py")
    if module == "docker":
        paths += ("tools/fleet/docker", "tools/docker")
    return paths


# Exact C13 catalogue relocation, used only for independent pre-C13 closures.
C13_PRIOR_ENTRIES = {'prismabuild.batch_ledger:BatchLedger.capture': 'prismabuild.produced_output:_read_commitments', 'prismabuild.batch_ledger:_declared_consumers': 'prismabuild.produced_output:_declared_consumers', 'prismabuild.batch_ledger:_consumer_release': 'prismabuild.produced_output:_consumer_release', 'prismabuild.batch_ledger:BatchLedger.record': 'prismabuild.produced_output:batch_record', 'prismabuild.output_contract:validate_descriptor': 'prismabuild.produced_output:validate_descriptor', 'prismabuild.output_contract:_checked_origin_ref': 'prismabuild.produced_output:_checked_origin_ref', 'prismabuild.output_contract:validate_template': 'prismabuild.produced_output:validate_template', 'prismabuild.output_contract:validate_instance': 'prismabuild.produced_output:validate_instance', 'prismabuild.batch_ledger:BatchLedger.file': 'prismabuild.produced_output:_write_commitments', 'prismabuild.batch_ledger:BatchLedger.commit_origin': 'prismabuild.produced_output:commit_origin_batch', 'prismabuild.output_contract:origin_batch_ref': 'prismabuild.produced_output:origin_batch_ref', 'prismabuild.batch_ledger:declare_instance': 'prismabuild.produced_output:declare_instance', 'prismabuild.batch_ledger:BatchLedger.due_mover_rows': 'prismabuild.produced_output:due_mover_rows'}

def generation_reference(root, reference):
    if not (root / "src/prismabuild/batch_ledger.py").is_file():
        return C13_PRIOR_ENTRIES.get(reference, reference)
    return reference
