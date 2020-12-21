package(default_visibility = ["//visibility:public"])

pex_library(
    name = "instance-tests-py",
    srcs = ["mock_protobuf.py"],
    reqs = [
        "pytest==6.1.2",
    ],
    deps = [
        "//heron/proto:proto-py",
        "//heronpy/api:heron-python-py",
    ],
)
