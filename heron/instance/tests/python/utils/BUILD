package(default_visibility = ["//visibility:public"])

pex_library(
    name = "common-utils-mock",
    srcs = ["mock_generator.py"],
    deps = [
        "//heron/instance/tests/python:instance-tests-py",
        "//heron/instance/src/python:instance-py",
    ],
    reqs = [
        "mock==1.0.1",
    ]
)

pex_test(
    name = "communicator_unittest",
    srcs = ["communicator_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "custom_grouping_unittest",
    srcs = ["custom_grouping_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "metrics_helper_unittest",
    srcs = ["metrics_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "outgoing_tuple_helper_unittest",
    srcs = ["outgoing_tuple_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "pplan_helper_unittest",
    srcs = ["pplan_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "topology_context_impl_unittest",
    srcs = ["topology_context_impl_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "tuple_helper_unittest",
    srcs = ["tuple_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "global_metrics_unittest",
    srcs = ["global_metrics_unittest.py"],
    deps = [
        ":common-utils-mock",
        "//heron/instance/tests/python:instance-tests-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "py_metrics_unittest",
    srcs = ["py_metrics_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_test(
    name = "log_unittest",
    srcs = ["log_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)
