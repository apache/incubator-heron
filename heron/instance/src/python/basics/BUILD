package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "pyheron_basics_lib",
  srcs = glob(["**/*.py"]),
  deps = [
    "//heron/common/src/python:common",
    "//heron/streamparse/src/python:pyheron_lib",
    "//heron/proto:proto-py",
  ],
  reqs = ['colorlog==2.6.1', 'pyyaml==3.10'],
)
