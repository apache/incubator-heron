load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "explorer_unittest",
    srcs = ["explorer_unittest.py"],
    deps = [
        "//heron/explorer/src/python:heron-explorer-lib",
    ],
    reqs = [
        "tabulate==0.7.5",
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
