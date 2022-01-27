package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
  name = "tracker-py",
  srcs = glob(
                 ["clients/*.py"],
             ),
)

py_library(
  name = "common-py",
  srcs = glob(
                 ["**/*.py"],
                 exclude = ["clients"],
                 exclude_directories = 1,
             ),
  deps = [
      "//heron/tools/common/src/python:common-py",
      requirement("PyYAML"),
  ]
)
