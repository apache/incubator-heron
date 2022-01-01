load("@rules_python//python:defs.bzl", "py_library")
load("@heron_py_deps//:requirements.bzl", "requirement")

package(default_visibility = ["//visibility:public"])

py_library(
  name = "cli_py_2",
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

pex_library(
    name = "cli-py",
    srcs = glob(
        ["**/*.py"],
    ),
    reqs = [
        "PyYAML==3.13",
        "requests==2.12.3",
        "netifaces==0.10.6",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
        "//heron/tools/common/src/python:common-py",
    ],
)

pex_binary(
    name = "heron",
    srcs = [
        "main.py",
    ],
    deps = [":cli-py"],
)
