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

pex_pytest(
    name = "communicator_unittest",
    srcs = ["communicator_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "custom_grouping_unittest",
    srcs = ["custom_grouping_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "metrics_helper_unittest",
    srcs = ["metrics_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "outgoing_tuple_helper_unittest",
    srcs = ["outgoing_tuple_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "pplan_helper_unittest",
    srcs = ["pplan_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "topology_context_impl_unittest",
    srcs = ["topology_context_impl_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "tuple_helper_unittest",
    srcs = ["tuple_helper_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "global_metrics_unittest",
    srcs = ["global_metrics_unittest.py"],
    deps = [
        ":common-utils-mock",
        "//heron/instance/tests/python:instance-tests-py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "py_metrics_unittest",
    srcs = ["py_metrics_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "log_unittest",
    srcs = ["log_unittest.py"],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock"
    ],
    reqs = [
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)
