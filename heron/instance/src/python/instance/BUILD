package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

# build binary for single thread python heron instance
pex_binary(
    name = "heron-python-instance",
    srcs = ["st_heron_instance.py"],
    deps = [
        "//heronpy/api:heron-python-py",
        "//heronpy/dsl:heron-pythondsl-py",
        "//heronpy/connectors:heron-pythonconnectors-py",
        "//heron/instance/src/python/basics:heron-basics-py",
        "//heron/instance/src/python/network:heron-network-py",
        "//heron/instance/src/python:instance-py",
        "//heron/common/src/python:common-py",
        "//heronpy/proto:proto-py",
    ],
    reqs = ['colorlog==2.6.1', 'PyYAML==3.10'],
)
