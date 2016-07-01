package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_library(
  name = "pytest-lib",
  srcs = ["mock_generator.py"],
  deps = [
    "//heron/instance/src/python:pyinstance_lib",
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
)
