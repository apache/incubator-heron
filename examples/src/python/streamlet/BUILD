package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "streamlet-example-py",
    deps = [
      '//heronpy/streamlet:heron-python-streamlet-py',
    ],
)

pex_binary(
    name = "streamlet_windowed_word_count",
    srcs = ["windowed_word_count.py"],
    deps = [
        "//heronpy/streamlet:heron-python-streamlet-py"
    ],
)