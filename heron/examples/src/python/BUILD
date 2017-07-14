package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "example-py",
    srcs = glob(['bolt/*.py', 'spout/*.py', 'misc/*.py']),
    deps = [
      '//heron/api/src/python:heron-python-py',
    ],
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

pex_binary(
    name = "window_size",
    srcs = ["window_size_topology.py"],
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

# dsl examples
pex_binary(
    name = "word_count_dsl",
    srcs = ["word_count_dsl.py"],
    deps = [
      '//heron/api/src/python:heron-python-py',
      '//heron/dsl/src/python:heron-pythondsl-py',
      '//heron/spouts/src/python:heron-pythonspouts-py',
    ],
)

