package(default_visibility = ["//visibility:public"])

pex_library(
    name = "common-utils-mock",
    srcs = ["mock_generator.py"],
    reqs = [
        "mock==1.0.1",
    ],
    deps = [
        "//heron/instance/src/python:instance-py",
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

pex_pytest(
    name = "communicator_unittest",
    size = "small",
    srcs = ["communicator_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "custom_grouping_unittest",
    size = "small",
    srcs = ["custom_grouping_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "metrics_helper_unittest",
    size = "small",
    srcs = ["metrics_helper_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "outgoing_tuple_helper_unittest",
    size = "small",
    srcs = ["outgoing_tuple_helper_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "pplan_helper_unittest",
    size = "small",
    srcs = ["pplan_helper_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "topology_context_impl_unittest",
    size = "small",
    srcs = ["topology_context_impl_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "tuple_helper_unittest",
    size = "small",
    srcs = ["tuple_helper_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "global_metrics_unittest",
    size = "small",
    srcs = ["global_metrics_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        ":common-utils-mock",
        "//heron/instance/tests/python:instance-tests-py",
    ],
)

pex_pytest(
    name = "py_metrics_unittest",
    size = "small",
    srcs = ["py_metrics_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)

pex_pytest(
    name = "log_unittest",
    size = "small",
    srcs = ["log_unittest.py"],
    reqs = [
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/instance/tests/python/utils:common-utils-mock",
    ],
)
