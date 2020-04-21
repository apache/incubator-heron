package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-integration-core-py",
    srcs = glob(["**/*.py"]),
    reqs = ["future==0.18.2"],
    deps = [
        "//heron/common/src/python:common-py",
        "//heronpy/api:heron-python-py",
    ],
)
