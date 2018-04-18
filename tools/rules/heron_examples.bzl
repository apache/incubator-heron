################################################################################
# Convenience macro for grouping all Heron example files
################################################################################
def heron_examples_files():
    return heron_examples_bin_files() + \
        heron_examples_conf_files() + \
        heron_examples_yaml_files() + \
        heron_examples_lib_files()

def heron_examples_bin_files():
    return []

def heron_examples_conf_files():
    return []

def heron_examples_yaml_files():
    return [
        "//eco-storm-examples/src/java:storm-eco-examples-support",
        "//eco-heron-examples/src/java:heron-eco-examples-support",
    ]

def heron_examples_lib_files():
    return [
        "//examples/src/java:heron-api-examples",
        "//examples/src/java:heron-streamlet-examples",
        "//examples/src/scala:heron-streamlet-scala-examples",
        "//eco-storm-examples/src/java:storm-eco-examples",
        "//eco-heron-examples/src/java:heron-eco-examples",
    ]
