"""Produced-output physical namespaces, shared below orchestration."""
#: The queue item's projection of the sealed declaration: which immutable
#: template this action's tier demand was derived from. The runtime binds
#: from this ref (filed body + live claim), never from a caller-supplied
#: template object.
PRODUCED_OUTPUT_REF_SCHEMA_V1 = "prismabuild.produced_output_ref.v1"

OUTPUT_TEMPLATES_SUBDIR = "produced-output-templates"
OUTPUT_SCOPES_SUBDIR = "produced-output-scopes"
OUTPUT_BATCHES_SUBDIR = "produced-output-batches"
OUTPUT_FRAGMENTS_SUBDIR = "produced-output-fragments"
