package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "explorer-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
        "//heron/statemgrs/src/python:statemgr-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/common/src/python:tracker-py",
        requirement("tabulate"),
        requirement("click"),
        requirement("requests")
    ],
)

py_binary(
    name = "heron-explorer",
    srcs = ["main.py"],
    deps = [
        ":explorer-py",
    ],
)





