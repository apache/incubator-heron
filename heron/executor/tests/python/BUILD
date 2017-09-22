package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "executor_unittest",
    srcs = ["heron_executor_unittest.py"],
    deps = [
        "//heron/executor/src/python:executor-py",
    ],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
    ],
    size = "small",
)
