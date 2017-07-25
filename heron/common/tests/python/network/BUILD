package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "pytest-network-py",
    srcs = ["mock_generator.py"],
    deps = [
        "//heron/common/tests/python:pytest-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
)

pex_test(
    name = "protocol_unittest",
    srcs = ["protocol_unittest.py"],
    deps = [
        ":pytest-network-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "heron_client_unittest",
    srcs = ["heron_client_unittest.py"],
    deps = [
        ":pytest-network-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
