package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-streamlet-api-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heronpy/api:heron-python-py",
    ],
)

# for egg production
pex_binary(
    name = "heron-streamlet-api-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":heron-streamlet-api-py",
        "//heronpy/api:heron-python-py",
    ]
)
