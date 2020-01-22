package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-ui-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    reqs = [
        "pycrypto==2.6.1",
        "requests==2.12.3",
        "tornado==4.0.2",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/common/src/python:tracker-py",
    ],
)

pex_binary(
    name = "heron-ui",
    srcs = ["main.py"],
    resources = [
        "//heron/tools/ui/resources:static",
        "//heron/tools/ui/resources:templates",
        "//scripts/packages:release_files",
    ],
    zip_safe = False,
    deps = [
        ":heron-ui-lib",
    ],
)
