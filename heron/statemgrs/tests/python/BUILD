package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "configloader_unittest",
    size = "small",
    srcs = [
        "configloader_unittest.py",
    ],
    data = [
        "//heron/config/src/yaml:conf-yaml",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
)

pex_pytest(
    name = "zkstatemanager_unittest",
    size = "small",
    srcs = [
        "zkstatemanager_unittest.py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
)

pex_pytest(
    name = "statemanagerfactory_unittest",
    size = "small",
    srcs = [
        "statemanagerfactory_unittest.py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
)
