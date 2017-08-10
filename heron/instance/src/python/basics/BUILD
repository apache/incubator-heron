package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
   name = "heron-basics-py",
   srcs = glob(["**/*.py"]),
   deps = [
       "//heron/common/src/python:common-py",
       "//heron/api/src/python:heron-python-py",
       "//heron/proto:proto-py",
   ],
   reqs = ['colorlog==2.6.1', 'PyYAML==3.10'],
)
