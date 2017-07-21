package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-pythondsl-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/proto:proto-py",
    ],
)

# for egg production
pex_binary(
    name = "heron-pythondsl-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":heron-pythondsl-py",
        "//heron/api/src/python:heron-python-py",
    ]
)
