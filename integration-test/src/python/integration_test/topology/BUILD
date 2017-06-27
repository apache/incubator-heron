package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "integration-topologies-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/api/src/python:heron-python-py",
        "//integration-test/src/python/integration_test/common:heron-integration-common-py",
        "//integration-test/src/python/integration_test/core:heron-integration-core-py",
    ],
)

pex_binary(
    name = "heron_integ_topology",
    srcs = ["test_topology_main.py"],
    main = "test_topology_main.py",
    resources = glob(["**/resources/*.json"]),
    deps = [
        ":integration-topologies-py",
    ],
)
