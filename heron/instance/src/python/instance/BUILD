package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

# build binary for single thread python heron instance
pex_binary(
    name = "heron-python-instance",
    srcs = ["st_heron_instance.py"],
    deps = [
        "//heron/pyheron/src/python:pyheron-py",
        "//heron/instance/src/python/basics:pyheron-basics-py",
        "//heron/instance/src/python/network:pyheron-network-py",
        "//heron/common/src/python:common-py",
        "//heron/proto:proto-py",
    ],
    reqs = ['colorlog==2.6.1', 'pyyaml==3.10'],
)
