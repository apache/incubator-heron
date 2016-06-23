package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "heron-nw-cli-lib",
  srcs = [
    "heron_client.py",
    "stmgr_client.py",
    "utils.py",
  ],
  deps = [
    "//heron/proto:proto-py",
  ],
)

pex_binary(
  name = "heron-nw-cli-py",
  srcs = ["main.py"],
  deps = [":heron-nw-cli-lib"],
)
