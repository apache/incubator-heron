package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "tracker-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
        "//heron/statemgrs/src/python:statemgr-py",
        "//heron/tools/common/src/python:common-py",
        "//heronpy/api:heron-python-py",
        requirement(click),
        requirement(fastapi),
        requirement(httpx),
        requirement(javaobj),
        requirement(networkx),
        requirement(protobuf),
        requirement(uvicorn),
    ],
)

py_binary(
    name = "heron-tracker",
    main = "main.py",
    srcs = ["main.py"],
    deps = [
        ":tracker-py",
    ],
)
