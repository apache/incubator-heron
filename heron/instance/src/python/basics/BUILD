package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
   name = "pyheron-basics-py",
   srcs = glob(["**/*.py"]),
   deps = [
       "//heron/common/src/python:common-py",
       "//heron/pyheron/src/python:pyheron-py",
       "//heron/proto:proto-py",
   ],
   reqs = ['colorlog==2.6.1', 'pyyaml==3.10'],
)
