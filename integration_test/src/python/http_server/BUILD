package(default_visibility = ["//visibility:public"])

pex_binary(
    name = "http-server",
    srcs = [
        "main.py",
    ],
    main = "main.py",
    reqs = [
        "tornado==4.0.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
    ],
)
