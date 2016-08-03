package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
  name = "word_count",
  srcs = ["word_count_topology.py",
          "count_bolt.py",
          "word_spout.py",
          "test_task_hook.py"],
  deps = [
    "//heron/instance/src/python/basics:pyheron_lib"
  ],
)

pex_binary(
  name = "custom_grouping",
  srcs = ["custom_grouping_topology.py",
          "consume_bolt.py",
          "word_spout.py"],
  deps = [
    "//heron/instance/src/python/basics:pyheron_lib"
  ],
)
