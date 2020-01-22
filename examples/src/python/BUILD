package(default_visibility = ["//visibility:public"])

pex_library(
    name = "example-py",
    srcs = glob([
        "bolt/*.py",
        "spout/*.py",
        "misc/*.py",
    ]),
    deps = [
        "//heronpy/api:heron-python-py",
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

pex_binary(
    name = "stateful_word_count",
    srcs = ["stateful_word_count_topology.py"],
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

# streamlet API examples
pex_binary(
    name = "word_count_streamlet",
    srcs = ["word_count_streamlet.py"],
    deps = [
        "//heronpy/api:heron-python-py",
        "//heronpy/connectors:heron-pythonconnectors-py",
        "//heronpy/streamlet:heron-python-streamlet-py",
    ],
)

pex_binary(
    name = "pulsar_word_count_streamlet",
    srcs = ["pulsar_word_count_streamlet.py"],
    deps = [
        "//heronpy/api:heron-python-py",
        "//heronpy/connectors:heron-pythonconnectors-py",
        "//heronpy/streamlet:heron-python-streamlet-py",
    ],
)

pex_binary(
    name = "join_streamlet_topology",
    srcs = ["join_streamlet_topology.py"],
    deps = [
        "//heronpy/api:heron-python-py",
        "//heronpy/connectors:heron-pythonconnectors-py",
        "//heronpy/streamlet:heron-python-streamlet-py",
    ],
)
