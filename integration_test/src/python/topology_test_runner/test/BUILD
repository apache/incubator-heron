package(default_visibility = ["//visibility:public"])

pex_binary(
    name = "topology-test-runner-test",
    srcs = [
        "test.py"
    ],
    main = "test.py",
    reqs = ["argparse==1.4.0"],
    deps = [
        "//heron/common/src/python:common-py",
        "//heronpy/proto:proto-py",
        "//integration_test/src/python/common",
    ],
)
