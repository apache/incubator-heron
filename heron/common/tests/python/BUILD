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
        "//heron/common/src/python:common",
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
    name = "topology_unittest",
    srcs = ["topology_unittest.py"],
    deps = [
        "//heron/common/src/python:common",
    ],
    eggs = [
        "//3rdparty/eggs:pytest",
        "//3rdparty/eggs:py",
        "//3rdparty/eggs:unittest2",
        "//3rdparty/eggs:pymock",
    ],
    size = "small",
)
