package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "heron-nw-cli-lib",
  srcs = glob(
    ["**/*.py"],
    exclude = ["main.py", "test_server.py"]
  ),
  deps = [
    "//heron/proto:proto-py",
  ],
)

pex_binary(
  name = "heron-nw-cli-py",
  srcs = ["main.py"],
  deps = [":heron-nw-cli-lib"],
)

pex_binary(
  name = "test-server-py",
  srcs = ["test_server.py"],
  deps = [":heron-nw-cli-lib"],
)
