package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "example-py",
    srcs = glob(['bolt/*.py', 'spout/*.py', 'misc/*.py']),
    deps = [
      '//heron/pyheron/src/python:pyheron-py',
    ],
)

pex_library(
    name = "pyheron-examples-py",
    srcs = glob(['**/*.py']),
    deps = ['//heron/pyheron/src/python:pyheron-py']
)

# with main method
pex_binary(
    name = "word_count",
    srcs = ["word_count_topology.py"],
    deps = [":example-py"],
)

pex_binary(
    name = "half_acking",
    srcs = ["half_acking_topology.py"],
    deps = [":example-py"],
)

# without main method
pex_binary(
    name = "custom_grouping",
    srcs = ["custom_grouping_topology.py"],
    deps = [":example-py"],
)

pex_binary(
    name = "multi_stream",
    srcs = ["multi_stream_topology.py"],
    deps = [":example-py"],
)


