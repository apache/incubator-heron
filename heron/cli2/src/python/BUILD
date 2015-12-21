package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
    name = "heron-cli2",
    srcs = ["cli.py"],
    deps = [
        "//heron/proto:proto-py",
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
