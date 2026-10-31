"""Select the evidence-backed merge gate population."""


def create_plan(root, changed, *, durations, quarantine):
    raise NotImplementedError('gate selection not implemented')


def changed_files(root, base, head):
    raise NotImplementedError('gate diff not implemented')
