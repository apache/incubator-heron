package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "opts_unittest",
    srcs = ["opts_unittest.py"],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)

pex_pytest(
    name = "client_command_unittest",
    srcs = ["client_command_unittest.py"],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
        "mock==1.0.1",
    ],
    size = "small",
)
