package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-ui-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    reqs = [
        "pycrypto==2.6.1",
        "requests==2.3.0",
        "tornado==4.0.2",
    ],
    deps = [
        "//heron/common/src/python:common",
        "//heron/common/src/python:tracker-access",
    ]
)

pex_binary(
    name = "heron-ui",
    srcs = ["main.py"],
    deps = [
        ":heron-ui-lib",
    ],
    resources = [
        "//heron/ui/resources:templates",
        "//heron/ui/resources:static",
    ],
    zip_safe = False,
)
