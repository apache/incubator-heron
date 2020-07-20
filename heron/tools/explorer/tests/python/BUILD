package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "explorer_unittest",
    size = "small",
    srcs = ["explorer_unittest.py"],
    reqs = [
        "pytest==3.2.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/explorer/src/python:explorer-py",
    ],
)
