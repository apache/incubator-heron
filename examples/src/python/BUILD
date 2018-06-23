package(default_visibility = ["//visibility:public"])

pex_library(
    name = "example-py",
    srcs = glob(['bolt/*.py', 'spout/*.py', 'misc/*.py']),
    deps = [
      '//heronpy/api:heron-python-py',
    ],
)

# with main method
pex_binary(
    name = "word_count",
    interpreter = "python3",
    srcs = ["word_count_topology.py"],
    deps = [":example-py"],
)

pex_binary(
    name = "half_acking",
    interpreter = "python3",
    srcs = ["half_acking_topology.py"],
    deps = [":example-py"],
)

pex_binary(
    name = "window_size",
    interpreter = "python3",
    srcs = ["window_size_topology.py"],
    deps = [":example-py"],
)

pex_binary(
    name = "stateful_word_count",
    interpreter = "python3",
    srcs = ["stateful_word_count_topology.py"],
    deps = [":example-py"],
)

# without main method
pex_binary(
    name = "custom_grouping",
    interpreter = "python3",
    srcs = ["custom_grouping_topology.py"],
    deps = [":example-py"],
)

pex_binary(
    name = "multi_stream",
    interpreter = "python3",
    srcs = ["multi_stream_topology.py"],
    deps = [":example-py"],
)

# streamlet API examples
pex_binary(
    name = "word_count_streamlet",
    interpreter = "python3",
    srcs = ["word_count_streamlet.py"],
    deps = [
      '//heronpy/api:heron-python-py',
      '//heronpy/streamlet:heron-python-streamlet-py',
      '//heronpy/connectors:heron-pythonconnectors-py',
    ],
)

pex_binary(
    name = "pulsar_word_count_streamlet",
    interpreter = "python3",
    srcs = ["pulsar_word_count_streamlet.py"],
    deps = [
      '//heronpy/api:heron-python-py',
      '//heronpy/streamlet:heron-python-streamlet-py',
      '//heronpy/connectors:heron-pythonconnectors-py',
    ]
)

pex_binary(
    name = "join_streamlet_topology",
    interpreter = "python3",
    srcs = ["join_streamlet_topology.py"],
    deps = [
      '//heronpy/api:heron-python-py',
      '//heronpy/streamlet:heron-python-streamlet-py',
      '//heronpy/connectors:heron-pythonconnectors-py',
    ]
)


