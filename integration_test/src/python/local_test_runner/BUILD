package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "local-test-runner",
    srcs = glob(["*.py"]),
    main = "main.py",
    data = [
        "resources/test.conf",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//integration_test/src/python/common",
    ],
)
