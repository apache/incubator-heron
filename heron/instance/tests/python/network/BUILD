package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_library(
    name = "instance-network-mock",
    srcs = ["mock_generator.py"],
    deps = [
        "//heron/instance/tests/python:instance-tests-py",
        "//heron/instance/src/python/network:heron-network-py",
    ],
    reqs = [
        "mock==1.0.1"
    ]
)

pex_library(
    name = "instance-network-mock-client",
    srcs = ["mock_generator_client.py"],
    deps = [
        "//heron/instance/tests/python:instance-tests-py",
        "//heron/instance/src/python/network:heron-network-py",
    ],
    reqs = [
        "mock==1.0.1"
    ]
)

pex_test(
    name = "st_stmgrcli_unittest",
    srcs = ["st_stmgr_client_unittest.py"],
    deps = [
        "//heron/instance/src/python/network:heron-network-py",
        "//heron/instance/tests/python/network:instance-network-mock",
        "//heron/instance/tests/python:instance-tests-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "metricsmgr_client_unittest",
    srcs = ["metricsmgr_client_unittest.py"],
    deps = [
        "//heron/api/src/python:heron-python-py",
        "//heron/instance/src/python/network:heron-network-py",
        "//heron/instance/tests/python/network:instance-network-mock",
        ":pytest-network-py",
        "//heron/instance/tests/python:instance-tests-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_library(
    name = "pytest-network-py",
    srcs = ["mock_generator.py"],
    deps = [
        "//heron/api/src/python:heron-python-py",
        "//heron/instance/src/python:instance-py",
        "//heron/instance/src/python/network:heron-network-py",
        "//heron/instance/tests/python:instance-tests-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
)

pex_test(
    name = "protocol_unittest",
    srcs = ["protocol_unittest.py"],
    deps = [
        ":pytest-network-py",
        "//heron/instance/tests/python:instance-tests-py",
        ":instance-network-mock-client",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "heron_client_unittest",
    srcs = ["heron_client_unittest.py"],
    deps = [
        ":pytest-network-py",
        "//heron/instance/tests/python:instance-tests-py",
        ":instance-network-mock-client",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
        "mock==1.0.1",
    ],
    size = "small",
)
