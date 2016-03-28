################################################################################
# Convenience macro for grouping all Heron API files
################################################################################
def heron_api_files():
    return heron_api_lib_files()

def heron_api_lib_files():
    return [
        "//heron/api/src/java:heron-api",
        "//heron/storm/src/java:heron-storm",
    ] 
