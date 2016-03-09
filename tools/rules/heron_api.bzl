################################################################################
# Convenience macro for grouping all Heron API files
################################################################################
def heron_api_files():
    return heron_api_bin_files() + heron_api_conf_files() + heron_api_lib_files()

def heron_api_bin_files():
    return []

def heron_api_conf_files():
    return []

def heron_api_lib_files():
    return [
        "//heron/api/src/java:api-java",
    ]

################################################################################
# Convenience macros for Heron Storm Compatibility API files
################################################################################
def heron_storm_compat_files():
    return heron_storm_compat_bin_files() + heron_storm_compat_conf_files() + heron_storm_compat_lib_files()

def heron_storm_compat_bin_files():
    return []

def heron_storm_compat_conf_files():
    return []

def heron_storm_compat_lib_files():
    return [
        "//heron/storm/src/java:storm-compatibility-java",
    ]
