package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "pyheron-network-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
    ],
)
