package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_binary(
    name = "local-test-runner",
    srcs = glob(["*.py"]),
    main = "main.py",
    resources = [
        "resources/test.conf",
    ],
    reqs = ["argparse==1.4.0"],
    deps = [
        "//heron/common/src/python:common-py",
        "//integration_test/src/python/common",
    ]
)
