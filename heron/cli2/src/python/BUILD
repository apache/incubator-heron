package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
    name = "heron-cli2",
    srcs = [
        "cli.py",
    ], 
    deps = [
        "//heron/proto:proto-py",
    ],
)

pex_library(
    name = "heron-cli-lib",
    srcs = [
        "activate.py",
        "args.py",
        "classpath.py",
        "deactivate.py",
        "help.py",
        "kill.py",
        "opts.py",
        "restart.py",
        "submit.py",
        "utils.py",
        "version.py",
    ], 
    deps = [
        "//heron/proto:proto-py",
    ],
)

pex_binary(
    name = "heron-cli3",
    srcs = [
        "main.py",
    ],
    deps = [
        ":heron-cli-lib",
    ],
)

filegroup(
    name = "scheduler-config",
    srcs = ["scheduler.conf"],
)

filegroup(
    name = "local-scheduler-config",
    srcs = ["local_scheduler.conf"],
)
