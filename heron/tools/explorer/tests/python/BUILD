load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "explorer_unittest",
    srcs = ["explorer_unittest.py"],
    deps = [
        "//heron/tools/explorer/src/python:explorer-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/common/src/python:common-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
