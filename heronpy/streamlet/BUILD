package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-python-streamlet-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heronpy/api:heron-python-py",
    ],
)

# for egg production
pex_binary(
    name = "heron-python-streamlet-api-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":heron-python-streamlet-py",
        "//heronpy/api:heron-python-py",
    ]
)
