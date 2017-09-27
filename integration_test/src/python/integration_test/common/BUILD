package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-integration-common-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heronpy/api:heron-python-py"
    ],
)
