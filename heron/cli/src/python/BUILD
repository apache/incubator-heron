package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
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
