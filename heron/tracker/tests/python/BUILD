load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "query_operator_unittest",
    srcs = ["query_operator_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
    ],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
        "//3rdparty/eggs:pymock",
    ],
    size = "small",
)

pex_test(
    name = "query_unittest",
    srcs = ["query_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
    ],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
        "//3rdparty/eggs:pymock",
    ],
    size = "small",
)

pex_test(
    name = "tracker_unittest",
    srcs = ["tracker_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
    ],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
        "//3rdparty/eggs:pymock",
    ],
    size = "small",
)
