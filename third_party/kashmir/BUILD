licenses(["notice"])

package(default_visibility = ["//visibility:public"])

include_files = [
    "kashmir/abstractrandomstream.h",
    "kashmir/iofwd.h",
    "kashmir/iostate.h",
    "kashmir/polydevrandom.h",
    "kashmir/randomstream.h",
    "kashmir/devrandom.h",
    "kashmir/uuid.h",
]

cc_library(
    name = "kashmir-cxx",
    srcs = ["empty.cc"],
    hdrs = include_files,
    includes = [
        ".",
    ],
    linkstatic = 1,
)

filegroup(
    name = "kashmir-files",
    srcs = include_files,
)
