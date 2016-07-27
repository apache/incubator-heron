package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-executor-lib",
    srcs = ["heron_executor.py"],
    deps = ["//heron/statemgrs/src/python:zk-statemgr-py"],
    reqs = ["pyyaml==3.10"],
)

pex_binary(
    name = "heron-executor",
    srcs = ["heron_executor.py"],
    deps = [":heron-executor-lib"],
)
