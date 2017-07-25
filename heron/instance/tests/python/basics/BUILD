package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_test(
    name = "bolt_instance_unittest",
    srcs = ["bolt_instance_unittest.py"],
    deps = [
        "//heron/common/tests/python:pytest-py",
        "//heron/instance/src/python/basics:heron-basics-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
