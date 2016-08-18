package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "integration-topologies-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/pyheron/src/python:pyheron-py",
        "//integration-test/src/python/integration_test/common:pyheron-integration-common-py",
        "//integration-test/src/python/integration_test/core:pyheron-integration-core-py",
    ],
)

pex_binary(
    name = "pyheron_integ_topology",
    srcs = ["test_topology_main.py"],
    main = "test_topology_main.py",
    resources = glob(["**/resources/*.json"]),
    deps = [
        ":integration-topologies-py",
    ],
)
