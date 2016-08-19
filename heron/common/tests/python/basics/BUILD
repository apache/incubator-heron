package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

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
