package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

# build binary for single thread python heron instance
pex_binary(
    name = "heron-python-instance",
    srcs = ["st_heron_instance.py"],
    deps = [
        "//heron/api/src/python:heron-python-py",
        "//heron/instance/src/python/basics:heron-basics-py",
        "//heron/instance/src/python/network:heron-network-py",
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
    ],
    reqs = ['colorlog==2.6.1', 'PyYAML==3.10'],
)
