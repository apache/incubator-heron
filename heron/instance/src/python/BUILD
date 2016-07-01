package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "pyinstance_lib",
  srcs = glob(
    ["**/*.py"],
    exclude = ["main.py", "test_server.py"]
  ),
  deps = [
    "//heron/common/src/python:common",
    "//heron/proto:proto-py",
  ],
)

pex_binary(
  name = "heron-nw-cli-py",
  srcs = ["main.py"],
  deps = [":pyinstance_lib"],
)

pex_binary(
  name = "test-server-py",
  srcs = ["test_server.py"],
  deps = [":pyinstance_lib"],
)
