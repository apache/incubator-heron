package(default_visibility = ["//visibility:public"])

pex_library(
    name = "admin-py",
    srcs = glob(
        ["**/*.py"],
    ),
    reqs = [
        "PyYAML==3.13",
        "enum34==1.1.6",
        "requests==2.12.3",
        "netifaces==0.10.6",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
        "//heron/tools/cli/src/python:cli-py",
        "//heron/tools/common/src/python:common-py",
    ],
)

pex_binary(
    name = "heron-admin",
    srcs = [
        "main.py",
    ],
    deps = [":admin-py"],
)
