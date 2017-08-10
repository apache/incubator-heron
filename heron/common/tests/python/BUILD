package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "pytest-py",
    srcs = ["mock_protobuf.py"],
    deps = [
      "//heron/common/src/python:common-py",
      "//heron/api/src/python:heron-python-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
)
