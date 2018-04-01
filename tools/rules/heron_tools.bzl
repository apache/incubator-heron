################################################################################
# Convenience macros for Heron tools files
################################################################################
def heron_tools_files():
    return heron_tools_bin_files() + heron_tools_conf_files() + heron_tools_lib_files()

def heron_tools_bin_all_files():
    return [
        "//heron/tools/tracker/src/python:heron-tracker",
        "//heron/tools/ui/src/python:heron-ui",
        "//heron/tools/apiserver/src/shell:heron-apiserver",
        "//heron/tools/cli/src/python:heron",
        "//heron/tools/explorer/src/python:heron-explorer",
    ]

def heron_tools_bin_files():
    return [
        "//heron/tools/tracker/src/python:heron-tracker",
        "//heron/tools/ui/src/python:heron-ui",
        "//heron/tools/apiserver/src/shell:heron-apiserver"
    ]

def heron_tools_lib_files():
    return [
        "//heron/tools/apiserver/src/java:heron-apiserver",
    ]

def heron_tools_conf_files():
    return [
        "//heron/tools/config/src/yaml:tracker-yaml",
    ]
