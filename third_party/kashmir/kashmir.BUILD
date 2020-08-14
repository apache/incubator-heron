load("@rules_cc//cc:defs.bzl", "cc_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

include_files = glob(["kashmir/*.h"])

cc_library(
    name = "kashmir-cxx",
    srcs = [],
    hdrs = glob(["kashmir/*.h"]),
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
