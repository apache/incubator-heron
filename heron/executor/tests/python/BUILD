package(default_visibility = ["//visibility:public"])

pex_test(
    name = "executor_unittest",
    srcs = ["heron_executor_unittest.py"],
    deps = [
        "//heron/executor/src/python:executor-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==1.1.0",
    ],
    size = "small",
)
