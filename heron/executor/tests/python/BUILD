load("/tools/rules/pex_rules", "pex_test")

pex_test(
    name = "executor_unittest",
    srcs = ["heron_executor_unittest.py"],
    deps = [
        "//heron/executor/src/python:executor-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
