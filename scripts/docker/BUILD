package(default_visibility = ["//visibility:public"])

container_image(
    name = "heron",
    base = "@heron-base//image",
    tars = [
        "//scripts/packages:heron-core",
        "//scripts/packages:heron-tools",
    ],
    directory = "/heron",
    workdir = "/heron",
    cmd = ["supervisord", "-n"]
)
