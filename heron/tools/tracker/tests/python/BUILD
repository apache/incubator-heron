package(default_visibility = ["//visibility:public"])

pex_library(
  name = 'mock_proto',
  srcs = ["mock_proto.py"],
  deps = [
    '//heron/proto:proto-py',
    '//heronpy/api:heron-python-py',
  ],
)

pex_pytest(
    name = "topology_unittest",
    srcs = ["topology_unittest.py", "mock_proto.py"],
    deps = [
        "//heron/tools/tracker/src/python:tracker-py",
        "//heron/proto:proto-py",
        "//heronpy/api:heron-python-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "query_operator_unittest",
    srcs = ["query_operator_unittest.py"],
    deps = [
        "//heron/tools/tracker/src/python:tracker-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "query_unittest",
    srcs = ["query_unittest.py"],
    deps = [
        "//heron/tools/tracker/src/python:tracker-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "tracker_unittest",
    srcs = ["tracker_unittest.py", "mock_proto.py"],
    deps = [
        "//heron/tools/tracker/src/python:tracker-py",
        "//heron/proto:proto-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)
