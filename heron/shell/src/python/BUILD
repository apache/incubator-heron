package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-shell-lib",
    srcs = glob(
        ["**/*.py"],
    ),
    reqs = [
        "requests==2.12.3",
        "tornado==4.0.2",
        "future==0.18.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
    ],
)

pex_binary(
    name = "heron-shell",
    srcs = ["main.py"],
    resources = [
        "//heron/shell/assets",
    ],
    deps = [
        ":heron-shell-lib",
    ],
)
