"""A valid receipt value shared by fixtures with the same field and byte order."""


def generation_receipt(generation, commit, files):
    return {"schema": "prismaquant.prismabuild.runtime_version.v1",
            "generation": generation, "commit": commit, "files": files}
