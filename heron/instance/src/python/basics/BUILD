package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
   name = "heron-basics-py",
   srcs = glob(["**/*.py"]),
   deps = [
       "//heron/common/src/python:common-py",
       "//heronpy/api:heron-python-py",
       "//heronpy/proto:proto-py",
   ],
   reqs = ['colorlog==2.6.1', 'PyYAML==3.10'],
)
