package(default_visibility = ["//visibility:public"])

container_image(
    name = "heron",
    base = "@heron-base//image",
    tars = [
        "//scripts/packages:heron",
    ],
    directory = "/heron",
    workdir = "/heron",
    cmd = ["supervisord", "-n"]
)
