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
