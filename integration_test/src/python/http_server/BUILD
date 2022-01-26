package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_binary(
    name = "http-server",
    srcs = [
        "main.py",
    ],
    main = "main.py",
    deps = [
        "//heron/common/src/python:common-py",
        requirement(tornado),
        requirement(werkzeug),
    ],
)
