package(default_visibility = ["//visibility:public"])

pex_library(
    name = "cli-py",
    srcs = glob(
        ["**/*.py"],
    ),
    reqs = [
        "PyYAML==5.4.1",
        "requests==2.27.1",
        "netifaces==0.10.6",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
        "//heron/tools/common/src/python:common-py",
    ],
)

pex_binary(
    name = "heron",
    srcs = [
        "main.py",
    ],
    deps = [":cli-py"],
)
