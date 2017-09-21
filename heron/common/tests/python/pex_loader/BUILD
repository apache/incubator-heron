package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "pex_loader_unittest",
    srcs = ["pex_loader_unittest.py", "constants.py"],
    deps = [
        "//heron/common/src/python:common-py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)
