package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-pythonspouts-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/api/src/python:heron-python-py",
    ],
    reqs = ['pulsar-client==1.19.dev2']
)

# for egg production
pex_binary(
    name = "heron-pythonspouts-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":heron-pythonspouts-py",
        "//heron/api/src/python:heron-python-py",
    ]
)
