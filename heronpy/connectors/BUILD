package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-pythonconnectors-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heronpy/api:heron-python-py",
    ],
    # TODO:- We should turn this on as soon as bazel issues are resolved
    # reqs = ['pulsar-client==1.19.dev2']
)

# for egg production
pex_binary(
    name = "heron-pythonconnectors-package",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heronpy/api:heron-python-py",
    ],
)
