package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_test(
  name = "bolt_unittest",
  srcs = ["bolt_unittest.py"],
  deps = [
    "//heron/instance/tests/python:pytest-lib",
  ],
  size = "small",
)
