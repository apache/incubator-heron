package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "instance-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/common/src/python:common-py",
        "//heronpy/api:heron-python-py",
        "//heronpy/connectors:heron-pythonconnectors-py",
        "//heronpy/proto:proto-py",
        "//heronpy/streamlet:heron-python-streamlet-py",
    ],
)

# build binary for single thread python heron instance
py_binary(
    name = "heron-python-instance",
    srcs = ["instance.py"],
    deps = [
        ":instance-py",
        requirement("PyYAML"),
        requirement("click"),
        requirement("colorlog"),
        ],
)






