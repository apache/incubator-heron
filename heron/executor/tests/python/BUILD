package(default_visibility = ["//visibility:public"])

pex_pytest(
    name = "executor_unittest",
    size = "small",
    srcs = ["heron_executor_unittest.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/executor/src/python:executor-py",
    ],
)
