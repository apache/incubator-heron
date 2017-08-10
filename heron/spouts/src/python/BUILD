package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "heron-pythonspouts-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/api/src/python:heron-python-py",
    ],
    # TODO:- We should turn this on as soon as bazel issues are resolved
    # reqs = ['pulsar-client==1.19.dev2']
)

# for egg production
pex_binary(
    name = "heron-pythonspouts-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":heron-pythonspouts-py",
        "//heron/api/src/python:heron-python-py",
    ]
)
