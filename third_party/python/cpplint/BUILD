licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_binary(
    name = "cpplint",
    entrypoint = "cpplint",
    deps = [
        requirement(cpplint),
    ],
)
