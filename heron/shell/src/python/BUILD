package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-shell-lib",
    srcs = glob(
        ["**/*.py"],
    ),
    reqs = [
        "logging-formatter-anticrlf==1.2",
        "requests==2.27.1",
        "tornado==6.1",
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
