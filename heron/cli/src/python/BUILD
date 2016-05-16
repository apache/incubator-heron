package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-lib",
    srcs = [
        "activate.py",
        "args.py",
        "deactivate.py",
        "execute.py",
        "help.py",
        "jars.py",
        "kill.py",
        "opts.py",
        "restart.py",
        "submit.py",
        "utils.py",
        "version.py",
    ], 
    deps = [
        "//heron/common/src/python:common",
        "//heron/proto:proto-py",
    ],
    reqs = ["pyyaml==3.10"],
)

pex_binary(
    name = "heron",
    srcs = [
        "main.py",
    ],
    deps = [
        ":heron-lib",
    ],
)
