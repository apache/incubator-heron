package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-executor-core",
    srcs = ["heron_executor_core.py"],
    deps = ["//heron/statemgrs/src/python:zk-statemgr-py"],
    reqs = ["pyyaml==3.10"],
)

pex_binary(
    name = "heron-executor",
    srcs = ["heron-executor.py"],
    deps = [":heron-executor-core"],
)
