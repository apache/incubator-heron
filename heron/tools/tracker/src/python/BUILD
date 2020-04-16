package(default_visibility = ["//visibility:public"])

pex_library(
    name = "tracker-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    reqs = [
        "protobuf==3.8.0",
        "tornado==4.0.2",
        "future==0.18.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
        "//heron/statemgrs/src/python:statemgr-py",
        "//heron/tools/common/src/python:common-py",
        "//heronpy/api:heron-python-py",
    ],
)

pex_binary(
    name = "heron-tracker",
    srcs = ["main.py"],
    deps = [
        ":tracker-py",
    ],
)
