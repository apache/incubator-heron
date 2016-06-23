load("/tools/rules/pex_rules", "pex_test")
load("/tools/rules/pex_rules", "pex_library")

pex_library(
  name = 'mock_proto',
  srcs = ["mock_proto.py"],
  deps = [
    '//heron/proto:proto-py',
  ],
)

pex_test(
    name = "topology_helpers_unittest",
    srcs = ["topology_helpers_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "topology_unittest",
    srcs = ["topology_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "query_operator_unittest",
    srcs = ["query_operator_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "query_unittest",
    srcs = ["query_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "tracker_unittest",
    srcs = ["tracker_unittest.py"],
    deps = [
        "//heron/tracker/src/python:heron-tracker-lib",
        "//heron/proto:proto-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
