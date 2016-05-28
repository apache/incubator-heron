package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-explore-lib",
    srcs = [
        "args.py",
        "help.py",
        "opts.py",
        "show.py",
        "utils.py",
        "topology.py",
        "logicalplan.py",
        "physicalplan.py",
    ],
    deps = [
        "//heron/common/src/python:common",
        "//heron/ui/src/python:heron-ui-lib",
        "//heron/statemgrs/src/python:zk-statemgr-py",
        "//heron/proto:proto-py",
    ],
    reqs = [
        "tornado==4.0.2",
        "tabulate==0.7.5",
    ],
)

pex_binary(
    name = "heron-explore",
    srcs = ["main.py"],
    deps = [
        ":heron-explore-lib",
    ],
)
