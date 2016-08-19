load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "opts_unittest",
    srcs = ["opts_unittest.py"],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "heronparser_unittest",
    srcs = [ "heronparser_unittest.py"],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

