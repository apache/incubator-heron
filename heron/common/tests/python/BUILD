package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_library(
  name = "pytest-lib",
  srcs = ["mock_generator.py", "mock_protobuf.py"],
  deps = [
    "//heron/common/src/python:common",
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
)

pex_test(
  name = "pex_loader_unittest",
  srcs = ["pex_loader_unittest.py"],
  deps = [
    "//heron/common/src/python:common",
  ],
  reqs = [
           "py==1.4.27",
           "pytest==2.6.4",
           "unittest2==0.5.1",
  ],
  size = "small",
)