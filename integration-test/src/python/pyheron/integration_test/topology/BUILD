package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "integ_topology_lib",
  srcs = glob(["**/*.py"]),
  deps = [
    "//heron/streamparse/src/python:pyheron_lib",
    "//integration-test/src/python/pyheron/integration_test/common:pyheron_integ_common",
    "//integration-test/src/python/pyheron/integration_test/core:pyheron_integ_core",
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
