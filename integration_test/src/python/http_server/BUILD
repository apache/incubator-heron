package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_binary(
    name = "http-server",
    srcs = [
        "main.py"
    ],
    main = "main.py",
    reqs = [
        "tornado==4.0.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
    ],
)
