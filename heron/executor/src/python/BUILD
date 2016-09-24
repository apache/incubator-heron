package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "executor-py",
    srcs = ["heron_executor.py"],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
        "//heron/common/src/python:common-log-py",
    ],
    reqs = ["pyyaml==3.10"],
)

pex_binary(
    name = "heron-executor",
    srcs = ["heron_executor.py"],
    deps = [
        ":executor-py",
    ],
)
