package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
  name = "word_spout",
  srcs = glob(["*.py"],),
  deps = [
    "//heron/instance/src/python:pyinstance_lib"
  ],
)
