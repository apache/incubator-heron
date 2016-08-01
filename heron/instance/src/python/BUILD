package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
  name = "defn_decoder",
  srcs = ["defn_decoder.py"],
  deps = ["//heron/proto:proto-py"]
)
