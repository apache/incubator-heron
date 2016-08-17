package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "pyheron_integration_common",
  srcs = glob(["**/*.py"]),
  deps = [
    "//heron/pyheron/src/python:pyheron_lib"
  ],
)
