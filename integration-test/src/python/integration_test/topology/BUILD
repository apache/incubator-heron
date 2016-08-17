package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "integ_topology_lib",
  srcs = glob(["**/*.py"]),
  deps = [
    "//heron/pyheron/src/python:pyheron_lib",
    "//integration-test/src/python/integration_test/common:pyheron_integration_common",
    "//integration-test/src/python/integration_test/core:pyheron_integration_core",
  ],
)

pex_binary(
  name = "pyheron_integ_topology",
  srcs = ["test_topology_main.py"],
  main = "test_topology_main.py",
  resources = glob(["**/resources/*.json"]),
  deps = [
    ":integ_topology_lib",
  ],
)
