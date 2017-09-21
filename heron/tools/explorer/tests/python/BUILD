package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "explorer_unittest",
    srcs = ["explorer_unittest.py"],
    deps = [
        "//heron/tools/explorer/src/python:explorer-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/common/src/python:common-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)
