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
  reqs = ['colorlog==2.6.1', 'PyYAML==3.11'],
)

pex_library(
  name = "topology_lib",
  srcs = ["instance/component.py", "instance/bolt.py", "instance/spout.py"],
)

pex_binary(
  name = "heron-nw-cli-py",
  srcs = ["main.py"],
  deps = [":pyinstance_lib"],
)

pex_binary(
  name = "test-server-py",
  srcs = ["test_server.py"],
  deps = ["//heron/instance/tests/python:pytest-lib"],
)

pex_binary(
  name = "single_thread_heron_instance",
  srcs = ["single_thread_instance/single_thread_heron_instance.py"],
  deps = [":pyinstance_lib"],
)

pex_binary(
  name = "defn_decoder",
  srcs = ["defn_decoder.py"],
  deps = ["//heron/proto:proto-py"]
)
