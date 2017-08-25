package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_library(
    name = "instance-tests-py",
    srcs = ["mock_protobuf.py"],
    deps = [
      "//heron/proto:proto-py",
      "//heron/api/src/python:heron-python-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
        "mock==1.0.1",
    ],
)
