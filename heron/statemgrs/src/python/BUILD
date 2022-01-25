package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "statemgr-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/proto:proto-py",
        requirement("PyYaml"),
        requirement("kazoo"),
        requirement("zope.interface"),
    ],
)


