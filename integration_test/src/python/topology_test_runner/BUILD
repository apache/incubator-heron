package(default_visibility = ["//visibility:public"])

pex_binary(
    name = "topology-test-runner",
    srcs = [
        "main.py",
    ],
    main = "main.py",
    reqs = [
    ],
    resources = [
        "resources/test.json",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/statemgrs/src/python:statemgr-py",
        "//heronpy/proto:proto-py",
        "//integration_test/src/python/common",
    ],
)
