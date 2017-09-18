package(default_visibility = ["//visibility:public"])

pex_library(
  name = 'mock_proto',
  srcs = ["mock_proto.py"],
  deps = [
    '//heron/proto:proto-py',
    '//heronpy/api:heron-python-py',
  ],
)

pex_test(
    name = "topology_unittest",
    srcs = ["topology_unittest.py"],
    deps = [
        ":mock_proto",
        "//heron/tools/tracker/src/python:tracker-py",
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
        "//heron/tools/tracker/src/python:tracker-py",
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
        "//heron/tools/tracker/src/python:tracker-py",
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
        "//heron/tools/tracker/src/python:tracker-py",
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
