package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "pyheron-integration-core-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/pyheron/src/python:pyheron-py"
    ],
)
