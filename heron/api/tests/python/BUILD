package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_test(
    name = "component_unittest",
    srcs = ["component_unittest.py"],
    deps = [
      "//heron/api/src/python:heron-python-py",
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
      "//heron/api/src/python:heron-python-py",
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
      "//heron/api/src/python:heron-python-py",
    ],
    reqs = [
      "py==1.4.27",
      "pytest==2.6.4",
      "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "serializer_unittest",
    srcs = ["serializer_unittest.py"],
    deps = [
      "//heron/api/src/python:heron-python-py",
    ],
    reqs = [
      "py==1.4.27",
      "pytest==2.6.4",
      "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "metrics_unittest",
    srcs = ["metrics_unittest.py"],
    deps = [
      "//heron/api/src/python:heron-python-py",
    ],
    reqs = [
      "py==1.4.27",
      "pytest==2.6.4",
      "unittest2==0.5.1",
    ],
    size = "small",
)
