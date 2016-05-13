load("/tools/rules/pex_rules", "pex_test")
load("/tools/rules/pex_rules", "pex_library")

pex_test(
    name = "sock_util",
    srcs = ["sock_util_unittest.py"],
    deps = [
        "//heron/common/src/python:heron-network-lib",
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
    name = "reqid",
    srcs = ["reqid_unittest.py"],
    deps = [
        "//heron/common/src/python:heron-network-lib",
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
    name = "packet",
    srcs = ["packet_unittest.py"],
    deps = [
        "//heron/common/src/python:heron-network-lib",
    ],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
        "//3rdparty/eggs:pymock",
    ],
    size = "small",
)