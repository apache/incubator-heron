package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "pyheron_integ_core",
  srcs = glob(["**/*.py"]),
  deps = [
    "//heron/streamparse/src/python:pyheron_lib"
  ],
)
