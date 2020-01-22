package(default_visibility = ["//visibility:public"])

pex_library(
    name = "instance-tests-py",
    srcs = ["mock_protobuf.py"],
    reqs = [
        "py==1.4.34",
        "pytest==3.2.2",
        "unittest2==1.1.0",
        "mock==1.0.1",
        "future==0.18.2",
    ],
    deps = [
        "//heron/proto:proto-py",
        "//heronpy/api:heron-python-py",
    ],
)
