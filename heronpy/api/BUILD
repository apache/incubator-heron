package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "heron-python-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["**/tests/**"],
    ),
    deps = [
        "//heronpy/proto:proto-py",
        requirement(cloudpickle)
    ],
)

# for egg production
py_binary(
    name = "heron-python-package",
    srcs = glob(
        ["**/*.py"],
        exclude = ["**/tests/**"],
    ),
    deps = [
        ":heron-python-py",
    ],
)
