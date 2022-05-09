package(default_visibility = ["//visibility:public"])

pex_library(
    name = "instance-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/common/src/python:common-py",
        "//heronpy/api:heron-python-py",
        "//heronpy/connectors:heron-pythonconnectors-py",
        "//heronpy/proto:proto-py",
        "//heronpy/streamlet:heron-python-streamlet-py",
    ],
)

# build binary for single thread python heron instance
pex_binary(
    name = "heron-python-instance",
    srcs = ["instance.py"],
    reqs = [
        "PyYAML==5.4.1",
        "click==7.1.2",
        "colorlog==2.6.1",
    ],
    deps = [":instance-py"],
)
