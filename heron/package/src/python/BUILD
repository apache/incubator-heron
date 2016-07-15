package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-package-lib",
    srcs = [
      "store/metastore.py",
      "store/blobstore.py",
      "common/utils.py",
      "common/package.py",
      "common/constants.py",
      "common/parser.py",
    ],
    deps = [
      "//heron/common/src/python:common",
    ],
    reqs = ["pyyaml==3.10"],
)

pex_binary(
    name = "heron-package",
    srcs = [
        "main.py",
    ],
    deps = [
        ":heron-package-lib",
    ]
)
