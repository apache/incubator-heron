load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "threads-cxx",
    srcs = [
        "modinit.cpp",
        "pcqueue.h",
        "spcountdownlatch.cpp",
        "spcountdownlatch.h",
    ],
    hdrs = [
        "modinit.h",
        "threads.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/basics:basics-cxx",
        "//heron/common/src/cpp/errors:errors-cxx",
    ],
)
