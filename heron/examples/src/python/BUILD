package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
  name = "word_count",
  srcs = ["word_count_topology.py",
          "count_bolt.py",
          "word_spout.py",
          "test_task_hook.py"],
  deps = [
    "//heron/instance/src/python:pyinstance_lib"
  ],
)
