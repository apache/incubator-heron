package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "test-runner",
    srcs = [
        "main.py",
    ],
    main = "main.py",
    reqs = [
    ],
    resources = [
        "resources/test.json",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//integration_test/src/python/common",
    ],
)
