package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "tracker-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/api/src/python:heron-python-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/statemgrs/src/python:statemgr-py",
        "//heron/proto:proto-py",
    ],
    reqs = [
        "protobuf==2.5.0",
        "tornado==4.0.2",
    ],
)

pex_binary(
    name = "heron-tracker",
    srcs = ["main.py"],
    deps = [
        ":tracker-py",
    ],
)
