package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-integration-core-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/api/src/python:heron-python-py",
        "//heron/common/src/python:common-py"
    ],
)
