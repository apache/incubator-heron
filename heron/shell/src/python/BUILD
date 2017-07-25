package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "heron-shell-lib",
    srcs = glob(
        ["**/*.py"],
    ),
    deps = [
        "//heron/common/src/python:common-py",
    ],
    reqs = [
        "requests==2.11.1",
        "tornado==4.0.2",
    ],
)

pex_binary(
    name = "heron-shell",
    srcs = ["main.py"],
    deps = [
        ":heron-shell-lib",
    ],
    resources = [
        "//heron/shell/assets:assets",
    ],
)
