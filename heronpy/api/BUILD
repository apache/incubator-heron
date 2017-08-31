package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-python-py",
    srcs = glob(["**/*.py"], exclude = [ "**/tests/**"]),
    deps = [
        "//heronpy/proto:proto-py",
    ],
)

# for egg production
pex_binary(
    name = "heron-python-package",
    srcs = glob(["**/*.py"], exclude = [ "**/tests/**"]),
    deps = [
        ":heron-python-py",
    ]
)
