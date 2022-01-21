package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "executor-py",
    srcs = ["heron_executor.py"],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/statemgrs/src/python:statemgr-py",
        requirement("PyYAML"),
        requirement("click"),

    ],
)

py_binary(
    name = "heron-executor",
    srcs = ["heron_executor.py"],
    deps = [
        ":executor-py",
    ],
)


