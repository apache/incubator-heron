package(default_visibility = ["//visibility:public"])

pex_library(
    name = "instance-network-mock",
    srcs = ["mock_generator.py"],
    reqs = [
        "mock==1.0.1",
    ],
    deps = [
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

pex_library(
    name = "instance-network-mock-client",
    srcs = ["mock_generator_client.py"],
    reqs = [
        "mock==1.0.1",
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
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
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
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        ":pytest-network-py",
        "//heron/instance/src/python:instance-py",
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

pex_library(
    name = "pytest-network-py",
    srcs = ["mock_generator.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        ":instance-network-mock-client",
    ],
)

pex_pytest(
    name = "protocol_unittest",
    size = "small",
    srcs = ["protocol_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
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
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
        "mock==1.0.1",
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
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
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
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/src/python:instance-py",
    ],
)
