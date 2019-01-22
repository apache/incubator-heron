licenses(["notice"])

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "tcmalloc",
    srcs = ["lib/libtcmalloc.a"],
    hdrs = [
        "include/gperftools/tcmalloc.h",
        "include/gperftools/malloc_extension.h",
        "include/google/malloc_extension.h",
    ],
    includes = [
        "include",
    ],
    linkstatic = 1,
)
