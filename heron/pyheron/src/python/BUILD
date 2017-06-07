package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "pyheron-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
    ],
    reqs = ['colorlog==2.6.1', 'pyyaml==3.10'],
)

# for pyheron egg production
pex_binary(
    name = "pyheron-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":pyheron-py",
    ]
)
