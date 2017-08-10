package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_library(
    name = "heron-ui-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    reqs = [
        "pycrypto==2.6.1",
        "requests==2.11.1",
        "tornado==4.0.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/common/src/python:tracker-py",
    ]
)

pex_binary(
    name = "heron-ui",
    srcs = ["main.py"],
    deps = [
        ":heron-ui-lib",
    ],
    resources = [
        "//heron/tools/ui/resources:templates",
        "//heron/tools/ui/resources:static",
        "//scripts/packages:release_files",
    ],
    zip_safe = False,
)
