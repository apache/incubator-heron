package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
  # build binary for single thread python heron instance
  name = "heron-python-instance",
  srcs = ["st_heron_instance.py"],
  deps = [
    "//heron/pyheron/src/python:pyheron_lib",
    "//heron/instance/src/python/basics:pyheron_basics_lib",
    "//heron/instance/src/python/network:pyheron_nw_lib",
    "//heron/common/src/python:common",
    "//heron/proto:proto-py",
  ],
  reqs = ['colorlog==2.6.1', 'pyyaml==3.10'],
)
