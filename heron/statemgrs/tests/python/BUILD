load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "statemanager_unittest",
    srcs = ["configloader_unittest.py"],
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
