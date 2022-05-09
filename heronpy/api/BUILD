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
    reqs = [
        "cloudpickle~=1.5.0",
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
