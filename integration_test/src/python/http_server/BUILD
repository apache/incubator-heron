package(default_visibility = ["//visibility:public"])

pex_binary(
    name = "http-server",
    srcs = [
        "main.py",
    ],
    main = "main.py",
    reqs = [
        "tornado==6.1",
        "werkzeug==2.0.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
    ],
)
