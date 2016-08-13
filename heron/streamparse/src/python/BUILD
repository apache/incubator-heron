package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "pyheron_lib",
  srcs = glob(["**/*.py"]),
  deps = [
    "//heron/common/src/python:pyheron_common",
    "//heron/proto:proto-py",
  ],
)

# for pyheron egg production
pex_binary(
  name = "pyheron_pkg",
  srcs = glob(["**/*.py"]),
  deps = [
    ":pyheron_lib",
    "//heron/examples/src/python:pyheron_example_pkg"
  ]
)