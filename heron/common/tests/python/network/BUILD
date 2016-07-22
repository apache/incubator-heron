package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_test(
  name = "protocol_unittest",
  srcs = ["protocol_unittest.py"],
  deps = [
    "//heron/common/tests/python:pytest-lib",
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
  size = "small",
)

pex_test(
  name = "heron_client_unittest",
  srcs = ["heron_client_unittest.py"],
  deps = [
    "//heron/common/tests/python:pytest-lib",
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
  size = "small",
)
