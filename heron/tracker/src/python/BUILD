package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-tracker-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    deps = [
        "//heron/common/src/python:common",
        "//heron/statemgrs/src/python:zk-statemgr-py",
        "//heron/proto:proto-py",
    ],
    eggs = [
        "//3rdparty/eggs:protobuf",
        "//3rdparty/eggs:tornado",
    ]
)

pex_binary(
    name = "heron-tracker",
    srcs = ["main.py"],
    deps = [
        ":heron-tracker-lib",
    ],
)
