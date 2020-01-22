package(default_visibility = ["//visibility:public"])

pex_library(
    name = "explorer-py",
    srcs = glob(["**/*.py"]),
    reqs = [
        "tornado==4.0.2",
        "tabulate==0.7.4",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
        "//heron/statemgrs/src/python:statemgr-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/common/src/python:tracker-py",
    ],
)

pex_binary(
    name = "heron-explorer",
    srcs = ["main.py"],
    deps = [
        ":explorer-py",
    ],
)
