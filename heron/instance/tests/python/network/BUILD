package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "instance-network-mock",
    srcs = ["mock_generator.py"],
    reqs = [
    ],
    deps = [
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

py_library(
    name = "instance-network-mock-client",
    srcs = ["mock_generator_client.py"],
    reqs = [
    ],
    deps = [
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

pex_pytest(
    name = "st_stmgrcli_unittest",
    size = "small",
    srcs = ["st_stmgr_client_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        ":instance-network-mock",
        ":instance-network-mock-client",
        "//heron/instance/src/python:instance-py",
    ],
)

pex_pytest(
    name = "metricsmgr_client_unittest",
    size = "small",
    srcs = ["metricsmgr_client_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        ":pytest-network-py",
        "//heron/instance/src/python:instance-py",
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

py_library(
    name = "pytest-network-py",
    srcs = ["mock_generator.py"],
    reqs = [
    ],
    deps = [
        ":instance-network-mock-client",
        requirement(pytest),
    ],
)

pex_pytest(
    name = "protocol_unittest",
    size = "small",
    srcs = ["protocol_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        ":pytest-network-py",
        "//heron/instance/src/python:instance-py",
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

pex_pytest(
    name = "heron_client_unittest",
    size = "small",
    srcs = ["heron_client_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        ":pytest-network-py",
        "//heron/instance/src/python:instance-py",
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

pex_pytest(
    name = "gateway_looper_unittest",
    size = "small",
    srcs = ["gateway_looper_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/instance/src/python:instance-py",
    ],
)

pex_pytest(
    name = "event_looper_unittest",
    size = "small",
    srcs = ["event_looper_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/instance/src/python:instance-py",
    ],
)
