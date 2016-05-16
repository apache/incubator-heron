package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
    name = "heron-executor",
    srcs = ["heron-executor.py"],
    reqs = ["pyyaml==3.10"],
)
