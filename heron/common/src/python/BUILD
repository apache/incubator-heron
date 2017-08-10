package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = 'common-py',
    srcs = glob(['**/*.py']),
    deps = [
        '//heron/proto:proto-py',
        '//heron/api/src/python:heron-python-py',
    ],
    reqs = [
        'colorlog==2.6.1',
        'psutil==4.3.1',
    ]
)
