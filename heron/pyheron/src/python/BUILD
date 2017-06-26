package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "pyheron-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/api/src/python:heron-python-py",
        "//heron/proto:proto-py",
    ],
    reqs = ['colorlog==2.6.1', 'PyYAML==3.10'],
)

# for pyheron egg production
pex_binary(
    name = "pyheron-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":pyheron-py",
    ]
)
