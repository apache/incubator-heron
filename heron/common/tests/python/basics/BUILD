package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_test(
    name = "gateway_looper_unittest",
    srcs = ["gateway_looper_unittest.py"],
    deps = [
        "//heron/common/src/python:common-py"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "event_looper_unittest",
    srcs = ["event_looper_unittest.py"],
    deps = [
        "//heron/common/src/python:common-py"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
