package(default_visibility = ["//visibility:public"])

pex_library(
    name = "tracker-py",
    srcs = glob(["clients/*.py"]),
)

pex_library(
    name = "common-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["clients"],
        exclude_directories = 1,
    ),
    reqs = ["PyYAML==5.4.1"],
    deps = [
        "//heron/common/src/python:common-py",
    ],
)
