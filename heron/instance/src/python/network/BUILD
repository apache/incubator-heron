package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-network-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/instance/src/python:instance-py",
        "//heron/proto:proto-py",
    ],
)
