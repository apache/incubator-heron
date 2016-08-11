package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_test(
  name = "bolt_instance_unittest",
  srcs = ["bolt_instance_unittest.py"],
  deps = [
    "//heron/common/tests/python:pytest-lib",
    "//heron/instance/src/python/basics:pyheron_basics_lib",
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
  size = "small",
)

