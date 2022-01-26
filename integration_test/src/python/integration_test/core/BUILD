package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")

py_library(
    name = "heron-integration-core-py",
    srcs = glob(["**/*.py"]),
    reqs = [],
    deps = [
        "//heron/common/src/python:common-py",
        "//heronpy/api:heron-python-py",
    ],
)
