package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "pyheron_nw_lib",
  srcs = glob(["**/*.py"]),
  deps = [
    "//heron/common/src/python:common",
    "//heron/proto:proto-py",
  ],
)
