load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_test(
    name = "configloader_unittest",
    srcs = [
        "configloader_unittest.py",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "zkstatemanager_unittest",
    srcs = [
        "zkstatemanager_unittest.py",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "statemanagerfactory_unittest",
    srcs = [
        "statemanagerfactory_unittest.py",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

