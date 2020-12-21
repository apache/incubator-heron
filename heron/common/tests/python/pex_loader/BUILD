package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "pex_loader_unittest",
    size = "small",
    srcs = [
        "constants.py",
        "pex_loader_unittest.py",
    ],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
    ],
)
