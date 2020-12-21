package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "opts_unittest",
    size = "small",
    srcs = ["opts_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
)

pex_pytest(
    name = "client_command_unittest",
    size = "small",
    srcs = ["client_command_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/tools/cli/src/python:cli-py",
    ],
)
