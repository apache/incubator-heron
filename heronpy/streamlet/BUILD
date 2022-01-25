package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")

py_library(
    name = "heron-python-streamlet-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heronpy/api:heron-python-py",
    ],
)

# for egg production
py_binary(
    name = "heron-python-streamlet-api-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":heron-python-streamlet-py",
        "//heronpy/api:heron-python-py",
    ],
)

