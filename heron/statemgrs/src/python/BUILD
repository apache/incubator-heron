package(default_visibility = ["//visibility:public"])

pex_library(
    name = "statemgr-py",
    srcs = glob(["**/*.py"]),
    reqs = [
        "PyYAML==5.4.1",
        "kazoo==2.8.0",
        "zope.interface==4.0.5",
    ],
    deps = [
        "//heron/proto:proto-py",
    ],
)
