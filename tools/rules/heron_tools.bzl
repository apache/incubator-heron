################################################################################
# Convenience macros for Heron tools files
################################################################################
def heron_tools_files():
    return heron_tools_bin_files() + heron_tools_conf_files() + heron_tools_lib_files()

def heron_tools_bin_files():
    return [
        "//heron/tracker/src/python:heron-tracker",
        "//heron/ui/src/python:heron-ui",
    ]

def heron_tools_conf_files():
    return [
        "//heron/config/src/yaml:conf-tracker",
    ]

def heron_tools_lib_files():
    return []
