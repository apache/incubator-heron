package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "executor_unittest",
    size = "small",
    srcs = ["heron_executor_unittest.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    deps = [
        "//heron/executor/src/python:executor-py",
    ],
)
