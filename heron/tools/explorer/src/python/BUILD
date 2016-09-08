package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "explorer-py",
    srcs = glob(["**/*.py"],),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/common/src/python:tracker-py",
        "//heron/statemgrs/src/python:statemgr-py",
        "//heron/proto:proto-py",
    ],
    reqs = [
        "tornado==4.0.2",
        "tabulate==0.7.4",
    ],
)

pex_binary(
    name = "heron-explorer",
    srcs = ["main.py"],
    deps = [
        ":explorer-py",
    ],
)
