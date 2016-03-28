load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "opts_unittest",
    srcs = ["opts_unittest.py"],
    deps = [
        "//heron/cli/src/python:heron-lib",
    ],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
    ],
    size = "small",
)
