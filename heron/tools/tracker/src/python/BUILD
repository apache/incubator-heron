package(default_visibility = ["//visibility:public"])

pex_library(
    name = "tracker-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    reqs = [
        "click==7.1.2",
        "fastapi==0.60.1",
        "httpx==0.16.1",
        "javaobj-py3==0.4.1",
        "networkx==2.4",
        "protobuf==3.8.0",
        "uvicorn==0.11.7",
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
