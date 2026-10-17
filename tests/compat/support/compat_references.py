"""Concrete source locations for the modules selected by the isolated loader."""


def module_paths(module):
    relative = module.replace(".", "/")
    if module.startswith("prismabuild."):
        return ("src/" + relative + ".py",)
    paths = ("tools/fleet/" + relative + ".py", "tools/" + relative + ".py")
    if module == "docker":
        paths += ("tools/fleet/docker", "tools/docker")
    return paths
