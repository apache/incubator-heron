package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = "example_lib",
  srcs = glob(['bolt/*.py', 'spout/*.py', 'misc/*.py']),
  deps = [
    '//heron/pyheron/src/python:pyheron_lib',
  ],
)

pex_library(
  name = "pyheron_examples",
  srcs = glob(['**/*.py']),
  deps = ['//heron/pyheron/src/python:pyheron_lib']
)

# with main method
pex_binary(
  name = "word_count",
  srcs = ["word_count_topology.py"],
  deps = [":example_lib"],
)

# without main method
pex_binary(
  name = "custom_grouping",
  srcs = ["custom_grouping_topology.py"],
  deps = [":example_lib"],
)

pex_binary(
  name = "multi_stream",
  srcs = ["multi_stream_topology.py"],
  deps = [":example_lib"],
)
