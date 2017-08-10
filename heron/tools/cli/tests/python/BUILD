load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_test(
    name = "opts_unittest",
    srcs = ["opts_unittest.py"],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)

pex_test(
    name = "client_command_unittest",
    srcs = ["client_command_unittest.py"],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
    reqs = [
        "mock==1.0.1",
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
