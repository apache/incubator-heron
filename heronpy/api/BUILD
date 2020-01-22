package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-python-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["**/tests/**"],
    ),
    deps = [
        "//heronpy/proto:proto-py",
    ],
)

# for egg production
pex_binary(
    name = "heron-python-package",
    srcs = glob(
        ["**/*.py"],
        exclude = ["**/tests/**"],
    ),
    deps = [
        ":heron-python-py",
    ],
)
