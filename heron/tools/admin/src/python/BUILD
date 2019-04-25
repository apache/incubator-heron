package(default_visibility = ["//visibility:public"])

pex_library(
    name = "admin-py",
    srcs = glob(
        ["**/*.py"],
    ),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/cli/src/python:cli-py",
        "//heron/proto:proto-py",
    ],
    reqs = [
      "PyYAML==3.13",
      "enum34==1.1.6",
      "requests==2.12.3",
      "netifaces==0.10.6"
    ],
)

pex_binary(
    name = "heron-admin",
    srcs = [
        "main.py",
    ],
    deps = [":admin-py"],
)
