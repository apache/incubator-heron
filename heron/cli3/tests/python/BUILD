load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "opts_unittest",
    srcs = ["opts_unittest.py"],
    deps = [
        "//heron/cli3/src/python:heron-cli-lib",
    ],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
    ],
    size = "small",
)
