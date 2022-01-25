package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "heron-shell-lib",
    srcs = glob(
        ["**/*.py"],
    ),
    deps = [
        "//heron/common/src/python:common-py",
        requirement(logging-formatter-anticrlf),
        requirement(requests),
        requirement(tornado),
    ],
)

py_binary(
    name = "heron-shell",
    main = "main.py"
    srcs = ["main.py"],
    resources = [
        "//heron/shell/assets",
    ],
    deps = [
        ":heron-shell-lib",
    ],
)
