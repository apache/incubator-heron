package(default_visibility = ["//visibility:public"])

pex_test(
    name = "bolt_instance_unittest",
    srcs = ["bolt_instance_unittest.py"],
    deps = [
        "//heron/instance/tests/python:instance-tests-py",
    ],
    reqs = [
        "py==1.4.27",
        "pytest==2.6.4",
        "unittest2==0.5.1",
    ],
    size = "small",
)
