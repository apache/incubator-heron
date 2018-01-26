package(default_visibility = ["//visibility:public"])

container_image(
    name = "heron",
    base = "@heron-base//image",
    tars = [
        "//scripts/packages:heron-core",
        "//scripts/packages:heron-tools",
    ],
    symlinks = {
        "/usr/local/bin/heron"                   : "/heron/heron-tools/bin/heron",
        "/usr/local/bin/heron-explorer"          : "/heron/heron-tools/bin/heron-explorer",
        "/usr/local/bin/heron-tracker"           : "/heron/heron-tools/bin/heron-tracker",
        "/usr/local/bin/heron-ui"                : "/heron/heron-tools/bin/heron-ui",
        "/usr/local/bin/heron-apiserver"         : "/heron/heron-tools/bin/heron-apiserver",

        "/heron/heron-tools/lib/metricscachemgr" : "/heron/heron-core/lib/metricscachemgr",
        "/heron/heron-tools/lib/packing"         : "/heron/heron-core/lib/packing", 
        "/heron/heron-tools/lib/scheduler"       : "/heron/heron-core/lib/scheduler", 
        "/heron/heron-tools/lib/statemgr"        : "/heron/heron-core/lib/statemgr", 
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

container_push(
    name = "publish",
    format = "Docker",
    image = ":heron",
    registry = "index.docker.io",
    repository = "heron/heron",
    tag = '{BUILD_USER}',
    stamp = True,
)
