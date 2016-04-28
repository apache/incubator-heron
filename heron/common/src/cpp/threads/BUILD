package(default_visibility = ["//visibility:public"])

cc_library(
    name = "threads-cxx",
    srcs = [
        "modinit.cpp",
        "pcqueue.cpp",
        "spcountdownlatch.cpp",

        "pcqueue.h",
        "spcountdownlatch.h",
    ],
    hdrs = [
        "threads.h",
        "modinit.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        "//heron/common/src/cpp/errors:errors-cxx",
        "//heron/common/src/cpp/basics:basics-cxx",
    ],
    linkstatic = 1,
)
