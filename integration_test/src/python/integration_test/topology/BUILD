package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

filegroup(
    name = "test-data-files",
    srcs = glob(["**/*.json"]),
)

pex_library(
    name = "integration-topologies-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["test_topology_main.py"],
    ),
    deps = [
        "//heron/api/src/python:heron-python-py",
        "//integration_test/src/python/integration_test/common:heron-integration-common-py",
        "//integration_test/src/python/integration_test/core:heron-integration-core-py",
    ],
)

pex_binary(
    name = "heron_integ_topology",
    srcs = ["test_topology_main.py"],
    main = "test_topology_main.py",
    deps = [
        ":integration-topologies-py",
    ],
)
