package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "cli-py",
    srcs = glob(
        ["**/*.py"],
    ),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/proto:proto-py",
    ],
    reqs = ["PyYAML==3.10", "enum34==1.1.6"],
)

pex_binary(
    name = "heron",
    srcs = [
        "main.py",
    ],
    deps = [":cli-py"],
)
