package(default_visibility = ["//visibility:public"])

pex_library(
    name = "executor-py",
    srcs = ["heron_executor.py"],
    reqs = ["PyYAML==3.13"],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/statemgrs/src/python:statemgr-py",
    ],
)

pex_binary(
    name = "heron-executor",
    srcs = ["heron_executor.py"],
    deps = [
        ":executor-py",
    ],
)
