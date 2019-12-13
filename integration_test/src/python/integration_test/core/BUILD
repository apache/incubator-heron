package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-integration-core-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heronpy/api:heron-python-py",
        "//heron/common/src/python:common-py"
    ],
    reqs = ["future==0.18.2"]
)
