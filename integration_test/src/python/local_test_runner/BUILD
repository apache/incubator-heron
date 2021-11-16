package(default_visibility = ["//visibility:public"])

pex_binary(
    name = "local-test-runner",
    srcs = glob(["*.py"]),
    main = "main.py",
    reqs = [
    ],
    resources = [
        "resources/test.conf",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//integration_test/src/python/common",
    ],
)
