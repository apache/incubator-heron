package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "instance-tests-py",
    srcs = ["mock_protobuf.py"],
    deps = [
        "//heron/proto:proto-py",
        "//heronpy/api:heron-python-py",
        requirement("pytest"),
    ],
)
