package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_library(
    name = "instance-network-mock",
    srcs = ["mock_generator.py"],
    deps = [
        "//heron/common/tests/python:pytest-py",
        "//heron/instance/src/python/network:pyheron-network-py",
    ],
    reqs = [
        "mock==1.0.1"
    ]
)

pex_test(
    name = "st_stmgrcli_unittest",
    srcs = ["st_stmgr_client_unittest.py"],
    deps = [
        "//heron/common/tests/python:pytest-py",
        "//heron/instance/src/python/network:pyheron-network-py",
        "//heron/instance/tests/python/network:instance-network-mock",
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
        "//heron/common/tests/python:pytest-py",
        "//heron/instance/src/python/network:pyheron-network-py",
        "//heron/instance/tests/python/network:instance-network-mock",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
