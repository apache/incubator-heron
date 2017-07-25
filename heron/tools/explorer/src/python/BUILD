package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

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
