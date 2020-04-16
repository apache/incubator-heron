load("@rules_cc//cc:defs.bzl", "cc_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

include_files = [
    "abstractrandomstream.h",
    "iofwd.h",
    "iostate.h",
    "polydevrandom.h",
    "randomstream.h",
    "devrandom.h",
    "uuid.h",
]

cc_library(
    name = "kashmir-cxx",
    srcs = ["empty.cc"],
    hdrs = include_files,
    copts = [
        "-Ithird_party",
        "-I.",
    ],
    linkstatic = 1,
)

filegroup(
    name = "kashmir-files",
    srcs = include_files,
)
