package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-shell-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"]
    ),
    reqs = [
        "requests==2.3.0",
        "tornado==4.0.2",
    ],
)

pex_binary(
    name = "heron-shell",
    srcs = ["main.py"],
    deps = [
        ":heron-shell-lib",
    ],
    data = [
        "//heron/shell/assets:assets",
    ],
)
