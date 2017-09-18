package(default_visibility = ["//visibility:public"])

pex_test(
    name = "pex_loader_unittest",
    srcs = ["pex_loader_unittest.py", "constants.py"],
    deps = [
        "//heron/common/src/python:common-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
