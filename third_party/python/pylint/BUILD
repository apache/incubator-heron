licenses(["notice"])  # apache 2.0

package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_binary(
    name = "pylint",
    main = "pylint",
    deps = [
        requirement("pylint"),
    ],
)
