"""Pure failure attribution policy for the merge queue."""


def failure_sets(original: set[str], baseline: set[str], rerun: set[str]):
    candidate_new = original - baseline
    return (sorted(candidate_new & rerun), sorted(original & baseline),
            sorted(candidate_new - rerun))
