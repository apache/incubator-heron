load("/tools/rules/pex_rules", "pex_test")
load("/tools/rules/pex_rules", "pex_library")

pex_test(
    name = "sock_util",
    srcs = ["sock_util_unittest.py"],
    deps = [
        "//heron/common/src/python:heron-network-lib",
    ],
    reqs = [
        "pytest==2.6.4",
        "mock==1.0.1",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "reqid",
    srcs = ["reqid_unittest.py"],
    deps = [
        "//heron/common/src/python:heron-network-lib",
    ],
    reqs = [
        "pytest==2.6.4",
        "mock==1.0.1",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "packet",
    srcs = ["packet_unittest.py"],
    deps = [
        "//heron/common/src/python:heron-network-lib",
    ],
    reqs = [
        "pytest==2.6.4",
        "mock==1.0.1",
        "unittest2==0.5.1",
    ],
    size = "small",
)