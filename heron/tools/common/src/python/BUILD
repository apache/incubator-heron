package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "tracker-py",
    srcs = glob(["access/*.py"]),
)

pex_library(
    name = "common-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["access"],
        exclude_directories = 1,
    ),
    deps = [
        "//heron/common/src/python:common-py",
    ],
    reqs = ["PyYAML==3.10"],
)
