package(default_visibility = ["//visibility:public"])

pex_library(
    name = "instance-tests-py",
    srcs = ["mock_protobuf.py"],
    reqs = [
        "pytest==3.2.2",
    ],
    deps = [
        "//heron/proto:proto-py",
        "//heronpy/api:heron-python-py",
    ],
)
