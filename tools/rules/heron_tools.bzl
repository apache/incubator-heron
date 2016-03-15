################################################################################
# Convenience macros for Heron tools files
################################################################################
def heron_tools_files():
    return heron_tools_bin_files() + heron_tools_conf_files() + heron_tools_lib_files()

def heron_tools_bin_files():
    return [
        "//heron/tracker/src/python:heron-tracker",
    ]

def heron_tools_conf_files():
    return [
        "//heron/statemgrs/conf:stateconfs",
    ]

def heron_tools_lib_files():
    return []
