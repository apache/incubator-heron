load("/tools/rules/pex_rules", "pex_test")

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

