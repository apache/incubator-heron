load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

package(default_visibility = ["//visibility:public"])

py_library(
  name = "cli-py",
  srcs = glob(
                 ["**/*.py"],
             ),
  deps = [
      "//heron/common/src/python:common-py",
      "//heron/proto:proto-py",
      "//heron/tools/common/src/python:common-py",
      requirement("PyYAML"),
      requirement("requests"),
      requirement("netifaces")
  ]
)

py_binary(
    name = "heron",
    srcs = [
        "main.py"
    ],
    deps = [
        "cli-py"
    ]
)
