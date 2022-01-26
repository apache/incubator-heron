package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")

filegroup(
    name = "test-data-files",
    srcs = glob(["**/*.json"]),
)

py_library(
    name = "integration-topologies-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["test_topology_main.py"],
    ),
    deps = [
        "//heronpy/api:heron-python-py",
        "//heronpy/connectors:heron-pythonconnectors-py",
        "//heronpy/streamlet:heron-python-streamlet-py",
        "//integration_test/src/python/integration_test/common:heron-integration-common-py",
        "//integration_test/src/python/integration_test/core:heron-integration-core-py",
    ],
)

py_binary(
    name = "heron_integ_topology",
    srcs = ["test_topology_main.py"],
    main = "test_topology_main.py",
    deps = [
        ":integration-topologies-py",
    ],
)
