package(default_visibility = ["//visibility:public"])

container_image(
    name = "heron",
    base = "@heron-base//image",
    tars = [
        "//scripts/packages:heron-core",
        "//scripts/packages:heron-tools",
    ],
    symlinks = {
        "/usr/local/bin/heron-tracker"   : "/heron/heron-tools/bin/heron-tracker",
        "/usr/local/bin/heron-ui"        : "/heron/heron-tools/bin/heron-ui",
        "/usr/local/bin/heron-apiserver" : "/heron/heron-tools/bin/heron-apiserver",
    },
    directory = "/heron",
    workdir = "/heron",
    cmd = ["supervisord", "-n"]
)

container_push(
    name = "publish-dev",
    format = "Docker",
    image = ":heron",
    registry = "index.docker.io",
    repository = "tmpscratch/heron",
    tag = '{BUILD_USER}',
    stamp = True,
)
