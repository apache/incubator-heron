load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "aurora_unittest",
    srcs = ["aurora_unittest.py"],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
    ],
    size = "small",
)
