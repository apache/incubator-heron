package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_test(
  name = "serializer_unittest",
  srcs = ["serializer_unittest.py"],
  deps = [
    "//heron/common/src/python:common"
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
  size = "small",
)

pex_test(
  name = "pplan_helper_unittest",
  srcs = ["pplan_helper_unittest.py"],
  deps = [
    "//heron/common/src/python:common"
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
  size = "small",
)
