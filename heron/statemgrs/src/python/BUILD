package(default_visibility = ["//visibility:public"])

pex_library(
    name = "statemgr-py",
    srcs = glob(["**/*.py"]),
    reqs = [
        "PyYAML==3.13",
        "kazoo==1.3.1",
        "zope.interface==4.0.5",
    ],
    deps = [
        "//heron/proto:proto-py",
    ],
)
