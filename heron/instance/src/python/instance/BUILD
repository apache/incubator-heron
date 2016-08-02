package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
  # build binary for single thread python heron instance
  name = "pyheron_st_instance",
  srcs = ["st_heron_instance.py"],
  deps = [
    "//heron/instance/src/python/basics:pyheron_lib",
    "//heron/instance/src/python/network:pyheron_nw_lib",
    "//heron/common/src/python:common",
    "//heron/proto:proto-py",
  ],
  reqs = ['colorlog==2.6.1', 'pyyaml==3.10'],
)
