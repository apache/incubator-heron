package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "configloader_unittest",
    srcs = [
        "configloader_unittest.py",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
    data = [
        "//heron/config/src/yaml:conf-yaml"
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "zkstatemanager_unittest",
    srcs = [
        "zkstatemanager_unittest.py",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "statemanagerfactory_unittest",
    srcs = [
        "statemanagerfactory_unittest.py",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

