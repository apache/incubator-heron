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
        "pytest==6.1.2",
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
        "pytest==6.1.2",
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
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/statemgrs/src/python:statemgr-py",
    ],
)
