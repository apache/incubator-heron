package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-packer-lib",
    srcs = [
      "store/metastore.py",
      "store/blobstore.py",
      "common/utils.py",
      "common/packer.py",
      "common/constants.py",
      "common/parser.py",
    ],
    deps = [
      "//heron/common/src/python:common",
    ],
    reqs = ["pyyaml==3.10"],
)

pex_binary(
    name = "heron-packer",
    srcs = [
        "main.py",
    ],
    deps = [
        ":heron-packer-lib",
    ]
)
