package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_binary")

pex_binary(
    name = "local-test-runner",
    srcs = glob(["*.py"]),
    main = "main.py",
    resources = [
        "resources/test.conf",
    ],
    reqs = ["argparse==1.4.0"],
    deps = [
        "//heron/common/src/python:common-py",
        "//integration-test/src/python/common",
    ]
)
