package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_test(
    name = "component_unittest",
    srcs = ["component_unittest.py"],
    deps = [
      "//heron/common/tests/python:pytest-py",
      "//heron/pyheron/src/python:pyheron-py",
    ],
    reqs = [
      "py==1.4.27",
      "pytest==2.6.4",
      "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "stream_unittest",
    srcs = ["stream_unittest.py"],
    deps = [
      "//heron/common/tests/python:pytest-py",
      "//heron/pyheron/src/python:pyheron-py",
    ],
    reqs = [
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
      "//heron/common/tests/python:pytest-py",
      "//heron/pyheron/src/python:pyheron-py",
    ],
    reqs = [
      "py==1.4.27",
      "pytest==2.6.4",
      "unittest2==0.5.1",
    ],
    size = "small",
)
